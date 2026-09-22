#include "moq/session.h"

#include "moq/codec.h"
#include "msquic_transport_adapter.h"

#include <algorithm>
#include <limits>
#include <spdlog/spdlog.h>

namespace moq {

// Buffers only until a stream is classified, then hands its bytes to the role.
class Session::PeerStreamGate final : public StreamSink {
public:
  PeerStreamGate(Session &session, std::weak_ptr<StreamContext> stream)
      : session_(session), stream_(std::move(stream)) {}

  void on_receive(const BytesView *chunks, size_t count, bool fin) override {
    const auto stream = stream_.lock();
    if (!stream || mode_ == Mode::Done)
      return;
    const auto lock = session_.lock_session();
    for (size_t index = 0; index < count; ++index) {
      if (mode_ == Mode::Padding) {
        if (!all_zero(chunks[index]))
          return session_.protocol_violation("padding stream contains non-zero bytes");
      } else {
        bytes_.insert(bytes_.end(), chunks[index].begin(), chunks[index].end());
      }
    }
    if (mode_ == Mode::Padding)
      return;
    if (stream->unidirectional()) {
      const auto type = read_varint(bytes_);
      if (type.status != DecodeStatus::Done) {
        if (fin)
          session_.protocol_violation("peer unidirectional stream ended before type");
        return;
      }
      if (type.value == kPaddingStreamType) {
        mode_ = Mode::Padding;
        if (!all_zero({bytes_.data() + type.bytes, bytes_.size() - type.bytes})) {
          return session_.protocol_violation("padding stream contains non-zero bytes");
        }
        bytes_.clear();
        return;
      }
      if (type.value != kSetupStreamType) {
        mode_ = Mode::Done;
        if (type.value == kFetchStreamType || is_subgroup_stream_type(type.value)) {
          return session_.handle_data_stream(type.value, stream, std::move(bytes_), fin);
        }
        return session_.protocol_violation("unknown peer unidirectional stream type " + std::to_string(type.value));
      }
    }
    const auto frame = read_control_message(bytes_);
    if (frame.status != DecodeStatus::Done) {
      if (fin)
        session_.protocol_violation("peer stream ended before first message");
      return;
    }
    mode_ = Mode::Done;
    if (stream->unidirectional()) {
      std::string error;
      if (!decode_setup(frame.message.payload, error))
        return session_.protocol_violation(std::move(error));
      bytes_.clear();
      session_.handle_peer_setup();
    } else {
      bytes_.erase(bytes_.begin(), bytes_.begin() + frame.bytes);
      session_.handle_peer_request(frame.message, stream, std::move(bytes_), fin);
    }
  }

private:
  static bool all_zero(BytesView bytes) {
    return std::all_of(bytes.begin(), bytes.end(), [](uint8_t byte) { return byte == 0; });
  }
  enum class Mode { Classify, Done, Padding };
  Session &session_;
  std::weak_ptr<StreamContext> stream_;
  Mode mode_ = Mode::Classify;
  ByteBuffer bytes_;
};

Session::Session(MsQuicClientConfig config) : config_(std::move(config)) {}
Session::~Session() = default;

bool Session::known_peer_request_type(uint64_t type) {
  switch (type) {
  case kMessageSubscribe:
  case kMessagePublish:
  case kMessagePublishNamespace:
  case kMessageTrackStatus:
  case kMessageFetch:
  case kMessageSubscribeNamespace:
  case kMessageSubscribeTracks:
    return true;
  default:
    return false;
  }
}

void Session::on_peer_stream_started(std::shared_ptr<StreamContext> stream) {
  spdlog::debug("Peer started a {} stream (id={})", stream->unidirectional() ? "unidirectional" : "bidirectional",
                stream->id());
  stream->set_sink(std::make_shared<PeerStreamGate>(*this, stream));
}

// Session management that are common to both publisher and subscriber.
void Session::start() {
  MsQuicTransportAdapter::Callbacks callbacks;
  callbacks.connected = [this] {
    const auto lock = lock_session();
    if (state_.phase != SessionPhase::Init)
      return;
    state_.phase = SessionPhase::SetupInProgress;
    try {
      const auto authority =
          config_.authority.empty() ? config_.host + ":" + std::to_string(config_.port) : config_.authority;
      auto stream = transport_->open_stream(true);
      if (!stream->send(encode_setup(authority, config_.path))) {
        throw std::runtime_error("StreamSend failed for SETUP");
      }
      state_.local_setup_sent = true;
      maybe_ready();
    } catch (const std::exception &error) {
      begin_close(SessionCloseErrorCode::InternalError, error.what());
    }
  };
  callbacks.peer_stream_started = [this](std::shared_ptr<StreamContext> stream) {
    on_peer_stream_started(std::move(stream));
  };
  callbacks.datagram_received = [this](BytesView bytes) {
    const auto lock = lock_session();
    on_datagram(bytes);
  };
  callbacks.transport_error = [this](std::string error) {
    const auto lock = lock_session();
    begin_close(SessionCloseErrorCode::InternalError, std::move(error));
  };
  callbacks.shutdown_complete = [this](bool handshake_completed) {
    const auto lock = lock_session();
    if (!handshake_completed && !state_.close_reason) {
      state_.close_reason = SessionCloseReason{SessionCloseErrorCode::InternalError, "QUIC handshake did not complete"};
    }
    state_.phase = SessionPhase::Closed;
    fail_ready_waiters("MOQT session closed");
  };
  transport_ = std::make_unique<MsQuicTransportAdapter>(config_, std::move(callbacks));
  transport_->start();
}

std::future<void> Session::ready() {
  const auto lock = lock_session();
  std::promise<void> promise;
  auto future = promise.get_future();
  if (state_.phase == SessionPhase::Ready) {
    promise.set_value();
  } else if (state_.phase >= SessionPhase::Closing) {
    fail(promise, "MOQT session closed before SETUP completed");
  } else {
    ready_waiters_.push_back(std::move(promise));
  }
  return future;
}

SessionStateSnapshot Session::state() const {
  const auto lock = lock_session();
  auto snapshot = state_;
  snapshot.active_subscriptions = active_subscriptions();
  return snapshot;
}

void Session::close(SessionCloseErrorCode error) {
  const auto lock = lock_session();
  begin_close(error, "local close");
}

void Session::close_and_wait() {
  std::unique_ptr<MsQuicTransportAdapter> transport;
  {
    const auto lock = lock_session();
    begin_close(SessionCloseErrorCode::NoError, "local close");
    transport = std::move(transport_);
  }
  // Drain callbacks outside the lock, while all derived members are still alive.
  transport.reset();
}

void Session::handle_peer_setup() {
  const auto lock = lock_session();
  state_.peer_setup_received = true;
  maybe_ready();
}

void Session::maybe_ready() {
  if (!state_.local_setup_sent || !state_.peer_setup_received || state_.phase >= SessionPhase::Ready)
    return;
  state_.phase = SessionPhase::Ready;
  state_.negotiated_version = kAlpn;
  for (auto &waiter : ready_waiters_)
    waiter.set_value();
  ready_waiters_.clear();
  on_ready();
}

void Session::protocol_violation(std::string error) {
  const auto lock = lock_session();
  begin_close(SessionCloseErrorCode::ProtocolViolation, std::move(error));
}

void Session::begin_close(SessionCloseErrorCode code, std::string reason) {
  if (state_.phase >= SessionPhase::Closing)
    return;
  state_.phase = SessionPhase::Closing;
  state_.close_reason = SessionCloseReason{code, reason};
  fail_ready_waiters(reason.empty() ? "MOQT session closing" : reason);
  terminate_subscriptions(reason);
  if (transport_)
    transport_->shutdown(code);
}

void Session::fail_ready_waiters(const std::string &reason) {
  for (auto &waiter : ready_waiters_)
    fail(waiter, reason);
  ready_waiters_.clear();
}

RequestId Session::allocate_request_id() {
  if (next_request_id_ > std::numeric_limits<RequestId>::max() - 2) {
    throw std::overflow_error("MOQT Request ID space exhausted");
  }
  const auto id = next_request_id_;
  next_request_id_ += 2; // client Request IDs are even
  return id;
}

} // namespace moq

#include "moq/session.h"

#include "moq/codec.h"
#include "msquic_transport_adapter.h"

#include <algorithm>
#include <limits>
#include <spdlog/spdlog.h>

namespace moq {
class Session::PeerStreamGate final : public StreamSink, public std::enable_shared_from_this<PeerStreamGate> {
public:
  PeerStreamGate(Session &session, std::weak_ptr<StreamContext> stream)
      : session_(session), stream_(std::move(stream)) {}

  void on_receive(const BytesView *chunks, size_t count, bool fin) override {
    const std::unique_lock<std::recursive_mutex> lock = session_.lock_session();
    const std::shared_ptr<StreamContext> stream = stream_.lock();
    if (!stream || mode_ == Mode::Done || session_.state_.phase >= SessionPhase::Closing)
      return;
    received_fin = received_fin || fin;
    for (size_t index = 0; index < count; ++index) {
      if (mode_ == Mode::Padding) {
        if (!all_zero(chunks[index]))
          return session_.protocol_violation("padding stream contains non-zero bytes");
      } else {
        pending_bytes_.insert(pending_bytes_.end(), chunks[index].begin(), chunks[index].end());
      }
    }
    if (mode_ == Mode::Padding)
      return;
    if (!stream->unidirectional() && defer_until_ready())
      return;
    if (stream->unidirectional()) {
      const VarintResult type = read_varint(pending_bytes_);
      if (type.status != DecodeStatus::Done) {
        if (received_fin)
          session_.protocol_violation("peer unidirectional stream ended before type");
        return;
      }
      if (type.value == kPaddingStreamType) {
        mode_ = Mode::Padding;
        if (!all_zero({pending_bytes_.data() + type.bytes, pending_bytes_.size() - type.bytes})) {
          return session_.protocol_violation("padding stream contains non-zero bytes");
        }
        pending_bytes_.clear();
        return;
      }
      if (type.value != kSetupStreamType) {
        if (defer_until_ready())
          return;
        mode_ = Mode::Done;
        if (type.value == kFetchStreamType || is_subgroup_stream_type(type.value)) {
          return session_.handle_data_stream(type.value, stream, std::move(pending_bytes_), received_fin);
        }
        return session_.protocol_violation("unknown peer unidirectional stream type " + std::to_string(type.value));
      }
    }
    // control stream or peer request stream
    const ControlMessageResult frame = read_control_message(pending_bytes_);
    if (frame.status != DecodeStatus::Done) {
      if (received_fin)
        session_.protocol_violation("peer stream ended before first message");
      return;
    }
    mode_ = Mode::Done;
    if (stream->unidirectional()) {
      std::string error;
      if (!decode_setup(frame.message.payload, error))
        return session_.protocol_violation(std::move(error));
      pending_bytes_.clear();
      session_.handle_peer_setup();
    } else {
      pending_bytes_.erase(pending_bytes_.begin(), pending_bytes_.begin() + frame.bytes);
      session_.handle_peer_request(frame.message, stream, std::move(pending_bytes_), received_fin);
    }
  }

  void on_peer_send_aborted(uint64_t) override { discard(); }
  void on_peer_receive_aborted(uint64_t) override { discard(); }
  void on_stream_closed() override { discard(); }

private:
  void discard() {
    const std::unique_lock<std::recursive_mutex> lock = session_.lock_session();
    mode_ = Mode::Done;
    pending_bytes_.clear();
  }

  bool defer_until_ready() {
    if (session_.state_.phase == SessionPhase::Ready)
      return false;
    if (!queued_) {
      queued_ = true;
      session_.pending_setup_.push_back([self = shared_from_this()](bool replay) {
        if (replay)
          self->on_receive(nullptr, 0, false);
        else
          self->discard();
      });
    }
    return true;
  }

  static bool all_zero(BytesView bytes) {
    return std::all_of(bytes.begin(), bytes.end(), [](uint8_t byte) { return byte == 0; });
  }
  enum class Mode { Classify, Done, Padding };
  Session &session_;
  std::weak_ptr<StreamContext> stream_;
  Mode mode_ = Mode::Classify;
  ByteBuffer pending_bytes_;
  bool received_fin = false;
  bool queued_ = false;
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
    const std::unique_lock<std::recursive_mutex> lock = lock_session();
    if (state_.phase != SessionPhase::Init)
      return;
    state_.phase = SessionPhase::SetupInProgress;
    try {
      const std::string authority =
          config_.authority.empty() ? config_.host + ":" + std::to_string(config_.port) : config_.authority;
      std::shared_ptr<StreamContext> stream = transport_->open_stream(true);
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
    const std::unique_lock<std::recursive_mutex> lock = lock_session();
    if (state_.phase >= SessionPhase::Closing)
      return;
    if (state_.phase != SessionPhase::Ready) {
      pending_setup_.push_back([this, bytes = bytes.to_owned()](bool replay) {
        if (replay)
          on_datagram(bytes);
      });
      return;
    }
    on_datagram(bytes);
  };
  callbacks.transport_error = [this](std::string error) {
    const std::unique_lock<std::recursive_mutex> lock = lock_session();
    begin_close(SessionCloseErrorCode::InternalError, std::move(error));
  };
  callbacks.shutdown_complete = [this](bool handshake_completed) {
    const std::unique_lock<std::recursive_mutex> lock = lock_session();
    if (!handshake_completed && !state_.close_reason) {
      state_.close_reason = SessionCloseReason{SessionCloseErrorCode::InternalError, "QUIC handshake did not complete"};
    }
    state_.phase = SessionPhase::Closed;
    discard_pending_setup();
    fail_ready_waiters("MOQT session closed");
  };
  transport_ = std::make_unique<MsQuicTransportAdapter>(config_, std::move(callbacks));
  transport_->start();
}

std::future<void> Session::ready() {
  const std::unique_lock<std::recursive_mutex> lock = lock_session();
  std::promise<void> promise;
  std::future<void> future = promise.get_future();
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
  const std::unique_lock<std::recursive_mutex> lock = lock_session();
  SessionStateSnapshot snapshot = state_;
  snapshot.active_subscriptions = active_subscriptions();
  return snapshot;
}

void Session::close(SessionCloseErrorCode error) {
  const std::unique_lock<std::recursive_mutex> lock = lock_session();
  begin_close(error, "local close");
}

void Session::close_and_wait() {
  std::unique_ptr<MsQuicTransportAdapter> transport;
  {
    const std::unique_lock<std::recursive_mutex> lock = lock_session();
    begin_close(SessionCloseErrorCode::NoError, "local close");
    transport = std::move(transport_);
  }
  // Drain callbacks outside the lock, while all derived members are still alive.
  transport.reset();
}

void Session::handle_peer_setup() {
  const std::unique_lock<std::recursive_mutex> lock = lock_session();
  state_.peer_setup_received = true;
  maybe_ready();
}

void Session::maybe_ready() {
  if (!state_.local_setup_sent || !state_.peer_setup_received || state_.phase >= SessionPhase::Ready)
    return;
  state_.phase = SessionPhase::Ready;
  state_.negotiated_version = kAlpn;
  for (std::promise<void> &waiter : ready_waiters_)
    waiter.set_value();
  ready_waiters_.clear();
  on_ready();
  // Move the queue out because a replayed handler may close the session.
  std::vector<std::function<void(bool)>> pending = std::move(pending_setup_);
  pending_setup_.clear();
  for (std::function<void(bool)> &deliver : pending)
    deliver(state_.phase == SessionPhase::Ready);
}

void Session::discard_pending_setup() {
  std::vector<std::function<void(bool)>> pending = std::move(pending_setup_);
  pending_setup_.clear();
  for (std::function<void(bool)> &deliver : pending)
    deliver(false);
}

void Session::protocol_violation(std::string error) {
  const std::unique_lock<std::recursive_mutex> lock = lock_session();
  begin_close(SessionCloseErrorCode::ProtocolViolation, std::move(error));
}

void Session::begin_close(SessionCloseErrorCode code, std::string reason) {
  if (state_.phase >= SessionPhase::Closing)
    return;
  state_.phase = SessionPhase::Closing;
  discard_pending_setup();
  state_.close_reason = SessionCloseReason{code, reason};
  fail_ready_waiters(reason.empty() ? "MOQT session closing" : reason);
  terminate_subscriptions(reason);
  if (transport_)
    transport_->shutdown(code);
}

void Session::fail_ready_waiters(const std::string &reason) {
  for (std::promise<void> &waiter : ready_waiters_)
    fail(waiter, reason);
  ready_waiters_.clear();
}

RequestId Session::allocate_request_id() {
  if (next_request_id_ > std::numeric_limits<RequestId>::max() - 2) {
    throw std::overflow_error("MOQT Request ID space exhausted");
  }
  const RequestId id = next_request_id_;
  next_request_id_ += 2; // client Request IDs are even
  return id;
}

} // namespace moq

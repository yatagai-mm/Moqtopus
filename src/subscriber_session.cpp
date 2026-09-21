#include "moq/subscriber_session.h"

#include "data_plane.h"
#include "moq/codec.h"
#include "msquic_transport_adapter.h"
#include "request/subscriber_subscription.h"

#include <algorithm>
#include <exception>
#include <future>
#include <limits>
#include <mutex>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace moq::detail {
namespace {

// Settle a promise with an error; ignores promises that were already satisfied.
template <typename Promise> void fail(Promise &promise, std::exception_ptr error) {
  try {
    promise.set_exception(std::move(error));
  } catch (const std::future_error &) {
  }
}

template <typename Promise> void fail(Promise &promise, const std::string &message) {
  fail(promise, std::make_exception_ptr(std::runtime_error(message)));
}

std::string default_authority(const MsQuicClientConfig &config) {
  return config.authority.empty() ? config.host + ":" + std::to_string(config.port) : config.authority;
}

bool known_peer_request_type(uint64_t type) {
  static constexpr uint64_t kKnown[] = {
      codec::kMessageSubscribe,      codec::kMessagePublish, codec::kMessagePublishNamespace,
      codec::kMessageTrackStatus,    codec::kMessageFetch,   codec::kMessageSubscribeNamespace,
      codec::kMessageSubscribeTracks};
  return std::find(std::begin(kKnown), std::end(kKnown), type) != std::end(kKnown);
}

} // namespace

class SessionImpl : public std::enable_shared_from_this<SessionImpl>, public SubscriptionOwner {
public:
  SessionImpl(MsQuicClientConfig msquic_config, SubscriberConfig subscriber_config)
      : msquic_config_(std::move(msquic_config)), subscriber_config_(std::move(subscriber_config)),
        data_plane_(
            subscriber_config_, [this](std::string error) { protocol_violation(std::move(error)); },
            [this](RequestId request_id, std::string error) {
              stop_subscription_now(request_id, true, std::move(error),
                                    static_cast<uint64_t>(StreamResetCode::MALFORMED_TRACK));
            }) {
    refresh_session_snapshot();
  }

  ~SessionImpl() override { close(SessionCloseErrorCode::NoError); }

  void start() {
    MsQuicTransportAdapter::Callbacks callbacks;
    callbacks.connected = [this] { on_connected(); };
    callbacks.peer_stream_started = [this](std::shared_ptr<StreamContext> stream) {
      on_peer_stream_started(std::move(stream));
    };
    callbacks.datagram_received = [this](BytesView datagram) { data_plane_.on_datagram(datagram); };
    callbacks.transport_error = [this](std::string error) {
      begin_close(SessionCloseErrorCode::InternalError, std::move(error));
    };
    callbacks.shutdown_complete = [this](bool handshake) { shutdown_complete(handshake); };
    transport_ = std::make_unique<MsQuicTransportAdapter>(msquic_config_, std::move(callbacks));
    transport_->start();
  }

  // waits until session is either ready or closed
  std::future<void> ready() {
    std::promise<void> promise;
    std::future<void> future = promise.get_future();
    if (phase_ == SessionPhase::Ready) {
      promise.set_value();
    } else if (phase_ == SessionPhase::Closing || phase_ == SessionPhase::Closed) {
      fail(promise, "MOQT session closed before SETUP completed");
    } else {
      ready_waiters_.push_back(std::move(promise));
    }
    return future;
  }

  SessionStateSnapshot state() const {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    return session_snapshot_;
  }

  // returns a state of the subscription with the given request ID
  SubscriptionStateSnapshot subscription_state(RequestId request_id) const {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    const auto snapshot = subscription_snapshots_.find(request_id);
    if (snapshot != subscription_snapshots_.end()) {
      return snapshot->second;
    }
    SubscriptionStateSnapshot terminated;
    terminated.request_id = request_id;
    return terminated;
  }

  std::future<SubscriptionHandle> subscribe(SubscribeRequest request, std::shared_ptr<ObjectHandler> handler) {
    auto promise = std::make_shared<std::promise<SubscriptionHandle>>();
    std::future<SubscriptionHandle> future = promise->get_future();
    if (!handler) {
      fail(*promise, "SUBSCRIBE requires an ObjectHandler");
      return future;
    }
    if (phase_ != SessionPhase::Ready) {
      fail(*promise, "MOQT session is not ready for SUBSCRIBE");
      return future;
    }
    try {
      if (request.request_id) {
        throw std::invalid_argument("explicit SUBSCRIBE Request ID is not allowed");
      }
      const RequestId request_id = allocate_request_id();
      auto stream = transport_->open_stream(false);
      const std::weak_ptr<SessionImpl> weak = weak_from_this();

      // settles the subscribe promise and refreshes snapshots
      auto subscribe_result_cb = [weak, promise, request_id](std::optional<RequestError> rejected,
                                                             std::optional<TrackAlias>) {
        const auto active = weak.lock();
        if (!active) {
          return;
        }
        if (rejected) {
          fail(*promise, rejected_exception(*rejected));
        } else {
          try {
            promise->set_value(SubscriptionHandle(request_id, weak));
          } catch (const std::future_error &) {
          }
        }
        active->refresh_session_snapshot();
        active->update_subscription_snapshot_from_fsm(request_id);
      };

      auto fsm = std::make_shared<SubscriptionFSM>(request_id, std::move(handler), stream, weak_from_this(),
                                                   std::move(subscribe_result_cb));
      // Install request state before sending so an immediate response cannot race sink setup.
      stream->set_sink(fsm);
      subscriptions_.emplace(request_id, fsm);
      update_subscription_snapshot_from_fsm(request_id);
      refresh_session_snapshot();

      if (!stream->send(codec::encode_subscribe(request_id, request))) {
        stop_subscription_now(request_id, true);
        throw std::runtime_error("StreamSend failed for SUBSCRIBE");
      }
    } catch (...) {
      fail(*promise, std::current_exception());
    }
    return future;
  }

  std::future<RequestOk> request_update(RequestId existing_request_id, RequestUpdate update) {
    std::promise<RequestOk> promise;
    std::future<RequestOk> future = promise.get_future();
    const auto found = subscriptions_.find(existing_request_id);
    if (found == subscriptions_.end() || found->second->phase() != moq::SubscriptionPhase::Established) {
      fail(promise, "REQUEST_UPDATE requires an established subscription");
      return future;
    }
    auto fsm = found->second;
    try {
      const RequestId request_id = allocate_request_id();
      auto stream = fsm->stream();
      fsm->send_request_update(request_id, std::move(update), std::move(promise),
                               [stream](ByteBuffer bytes) { return stream->send(std::move(bytes)); });
      update_subscription_snapshot_from_fsm(existing_request_id);
    } catch (...) {
      fail(promise, std::current_exception());
    }
    return future;
  }

  std::future<void> stop_subscription(RequestId request_id) {
    std::promise<void> promise;
    stop_subscription_now(request_id, false, std::string{},
                          static_cast<uint64_t>(StreamResetCode::Cancelled));
    promise.set_value();
    return promise.get_future();
  }

  void close(SessionCloseErrorCode error) { begin_close(error, "local close"); }

  void close_and_wait(SessionCloseErrorCode error) {
    begin_close(error, "local close");
    // Drain transport callbacks while the public session still owns this impl.
    // Otherwise a callback can release the last shared_ptr and destroy its own
    // StreamContext re-entrantly.
    auto transport = std::move(transport_);
    transport.reset();
  }

  // ---- called by stream gates (cold path) ----

  DataPlane &data_plane() { return data_plane_; }

  void handle_peer_setup() {
    spdlog::debug("Peer SETUP received");
    peer_setup_received_ = true;
    maybe_ready();
  }

  void protocol_violation(std::string error) {
    begin_close(SessionCloseErrorCode::ProtocolViolation, std::move(error));
  }

  // subscriber-only implementation: refuse every peer-initiated request
  void reject_peer_request(uint64_t request_type, StreamContext &stream) {
    if (!known_peer_request_type(request_type)) {
      protocol_violation("unknown peer request type " + std::to_string(request_type));
      return;
    }
    const bool publish = request_type == codec::kMessagePublish;
    stream.send(
        codec::encode_request_error(publish ? RequestErrorCode::Uninterested : RequestErrorCode::NotSupported,
                                    publish ? "subscriber is not accepting PUBLISH" : "subscriber-only implementation"),
        true);
    stream.abort_receive(0);
  }

  // ---- SubscriptionOwner ----

  bool install_route(TrackAlias alias, std::shared_ptr<ReceiveRoute> route) override {
    if (data_plane_.install_route(alias, std::move(route))) {
      return true;
    }
    begin_close(SessionCloseErrorCode::DuplicateTrackAlias, "SUBSCRIBE_OK reused an established Track Alias");
    return false;
  }

  void retire_route(TrackAlias alias) override { data_plane_.retire_route(alias); }

private:
  // Send SETUP on connection establishment
  void on_connected() {
    if (phase_ != SessionPhase::Init) {
      return;
    }
    phase_ = SessionPhase::SetupInProgress;
    try {
      spdlog::debug("MOQT connected; sending SETUP authority={} path={}", default_authority(msquic_config_),
                    msquic_config_.path);
      local_setup_stream_ = transport_->open_stream(true);
      if (!local_setup_stream_->send(codec::encode_setup(default_authority(msquic_config_), msquic_config_.path))) {
        throw std::runtime_error("StreamSend failed for SETUP");
      }
      local_setup_sent_ = true;
      refresh_session_snapshot();
    } catch (const std::exception &error) {
      begin_close(SessionCloseErrorCode::InternalError, error.what());
    }
  }

  void on_peer_stream_started(std::shared_ptr<StreamContext> stream); // defined after the gates

  void maybe_ready() {
    if (!local_setup_sent_ || !peer_setup_received_ || phase_ == SessionPhase::Closing ||
        phase_ == SessionPhase::Closed) {
      refresh_session_snapshot();
      return;
    }
    spdlog::debug("MOQT SETUP completed; session ready");
    phase_ = SessionPhase::Ready;
    for (auto &waiter : ready_waiters_) {
      waiter.set_value();
    }
    ready_waiters_.clear();
    refresh_session_snapshot();
  }

  RequestId allocate_request_id() {
    const RequestId request_id = next_request_id_;
    if (request_id > std::numeric_limits<RequestId>::max() - 2) {
      throw std::overflow_error("MOQT Request ID space exhausted");
    }
    next_request_id_ += 2; // client uses even Request IDs, server uses odd Request IDs
    return request_id;
  }

  // Stops the subscription corresponding to request_id immediately.
  // Unless specified, it resets both directions with error code 0 (Internal Error).
  void stop_subscription_now(RequestId request_id, bool report_error, std::string reason = std::string{},
                             uint64_t stream_error_code = 0) {
    const auto found = subscriptions_.find(request_id);
    if (found == subscriptions_.end()) {
      spdlog::debug("stop_subscription_now: no subscription found for request_id={}", request_id);
      return;
    }
    auto fsm = found->second;
    fsm->terminate(report_error, report_error ? reason : std::string{});
    if (auto stream = fsm->stream()) {
      stream->abort_send(stream_error_code);
      stream->abort_receive(stream_error_code);
    }
    update_subscription_snapshot_from_fsm(request_id);
    refresh_session_snapshot();
    subscriptions_.erase(found);
  }

  void malformed_track(RequestId request_id, std::string error) {
    spdlog::debug("malformed track for request_id={}: {}", request_id, error);
    stop_subscription_now(request_id, true, "malformed track", static_cast<uint64_t>(StreamResetCode::MALFORMED_TRACK));
  }

  void begin_close(SessionCloseErrorCode code, std::string reason) {
    if (phase_ == SessionPhase::Closed || phase_ == SessionPhase::Closing) {
      return;
    }
    spdlog::debug("Session beginning close: code={} reason={}", static_cast<uint64_t>(code), reason);
    phase_ = SessionPhase::Closing;
    close_reason_ = SessionCloseReason{code, reason};
    fail_ready_waiters(reason.empty() ? "MOQT session closing" : reason);
    for (auto &entry : subscriptions_) {
      if (entry.second) {
        entry.second->terminate(true, reason);
      }
    }
    refresh_session_snapshot();
    if (transport_) {
      transport_->shutdown(code);
    }
  }

  void shutdown_complete(bool handshake_completed) {
    spdlog::debug("Session shutdown complete: handshake_completed={}", handshake_completed);
    if (!handshake_completed && !close_reason_) {
      close_reason_ = SessionCloseReason{SessionCloseErrorCode::InternalError, "QUIC handshake did not complete"};
    }
    phase_ = SessionPhase::Closed;
    fail_ready_waiters("MOQT session closed");
    refresh_session_snapshot();
  }

  void fail_ready_waiters(const std::string &reason) {
    for (auto &waiter : ready_waiters_) {
      fail(waiter, reason);
    }
    ready_waiters_.clear();
  }

  void update_subscription_snapshot_from_fsm(RequestId request_id) {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    const auto found = subscriptions_.find(request_id);
    if (found == subscriptions_.end()) {
      return;
    }
    const auto &fsm = *found->second;
    subscription_snapshots_[request_id] =
        SubscriptionStateSnapshot{fsm.phase(), request_id, fsm.track_alias(), fsm.inflight_updates()};
  }

  void refresh_session_snapshot() {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    session_snapshot_.phase = phase_;
    session_snapshot_.local_setup_sent = local_setup_sent_;
    session_snapshot_.peer_setup_received = peer_setup_received_;
    session_snapshot_.negotiated_version =
        phase_ >= SessionPhase::Ready ? std::optional<std::string>(msquic_config_.alpn) : std::nullopt;
    session_snapshot_.active_subscriptions =
        static_cast<size_t>(std::count_if(subscriptions_.begin(), subscriptions_.end(), [](const auto &entry) {
          return entry.second && entry.second->phase() != moq::SubscriptionPhase::Terminated;
        }));
    session_snapshot_.close_reason = close_reason_;
  }

  MsQuicClientConfig msquic_config_;
  SubscriberConfig subscriber_config_;
  DataPlane data_plane_;

  SessionPhase phase_ = SessionPhase::Init;
  RequestId next_request_id_ = 0;
  bool local_setup_sent_ = false;
  bool peer_setup_received_ = false;
  std::optional<SessionCloseReason> close_reason_;
  std::shared_ptr<StreamContext> local_setup_stream_;
  std::vector<std::promise<void>> ready_waiters_;
  std::unordered_map<RequestId, std::shared_ptr<SubscriptionFSM>> subscriptions_;

  mutable std::mutex snapshot_mutex_;
  SessionStateSnapshot session_snapshot_;
  std::unordered_map<RequestId, SubscriptionStateSnapshot> subscription_snapshots_;
  std::unique_ptr<MsQuicTransportAdapter> transport_;
};

namespace {

// Cold path: buffers the first bytes of a peer-initiated stream until it can be
// classified. Subgroup streams are handed to the DataPlane, which swaps the
// stream over to the hot-path SubgroupReceiver sink; this gate then dies with it.
class PeerStreamGate final : public StreamSink {
public:
  PeerStreamGate(std::weak_ptr<SessionImpl> session, std::weak_ptr<StreamContext> stream)
      : session_(std::move(session)), stream_(std::move(stream)) {}

  void on_receive(const BytesView *chunks, size_t count, bool fin) override {
    const auto session = session_.lock();
    const auto stream = stream_.lock();
    if (!session || !stream) {
      return;
    }
    if (mode_ == Mode::Padding) {
      for (size_t index = 0; index < count; ++index) {
        if (!all_zero(chunks[index])) {
          return session->protocol_violation("padding stream contains non-zero bytes");
        }
      }
      return;
    }
    if (mode_ == Mode::Done) { // post-SETUP control messages (e.g. GOAWAY) and rejected requests are ignored
      return;
    }
    for (size_t index = 0; index < count; ++index) {
      bytes_.insert(bytes_.end(), chunks[index].begin(), chunks[index].end());
    }
    if (stream->unidirectional()) {
      classify(*session, stream, fin);
    } else {
      reject_request(*session, stream, fin);
    }
  }

private:
  enum class Mode { Classify, Done, Padding };

  static bool all_zero(BytesView bytes) {
    return std::all_of(bytes.begin(), bytes.end(), [](uint8_t byte) { return byte == 0; });
  }

  // Unidirectional streams are classified by their first varint.
  void classify(SessionImpl &session, const std::shared_ptr<StreamContext> &stream, bool fin) {
    const codec::VarintResult type = codec::read_varint(bytes_);
    if (type.status != codec::DecodeStatus::Done) {
      if (fin) {
        session.protocol_violation("peer unidirectional stream ended before type");
      }
      return;
    }
    if (type.value == codec::kSetupStreamType) {
      const codec::ControlMessageResult frame = codec::read_control_message(bytes_);
      if (frame.status != codec::DecodeStatus::Done) {
        if (fin) {
          session.protocol_violation("peer setup stream ended mid-SETUP");
        }
        return;
      }
      std::string error;
      if (!codec::decode_setup(frame.message.payload, error)) {
        return session.protocol_violation(std::move(error));
      }
      mode_ = Mode::Done;
      bytes_.clear();
      return session.handle_peer_setup();
    }
    if (type.value == codec::kFetchStreamType) {
      return stream->abort_receive(0); // fetch is not supported
    }
    if (type.value == codec::kPaddingStreamType) {
      mode_ = Mode::Padding;
      if (!all_zero(BytesView{bytes_.data() + type.bytes, bytes_.size() - type.bytes})) {
        return session.protocol_violation("padding stream contains non-zero bytes");
      }
      bytes_.clear();
      return;
    }
    if (codec::is_subgroup_stream_type(type.value)) {
      return session.data_plane().start_subgroup_stream(stream, std::move(bytes_), fin);
    }
    session.protocol_violation("unknown peer unidirectional stream type " + std::to_string(type.value));
  }

  // A peer-initiated bidirectional stream carries one request, which this
  // subscriber-only implementation always rejects.
  void reject_request(SessionImpl &session, const std::shared_ptr<StreamContext> &stream, bool fin) {
    const codec::ControlMessageResult message = codec::read_control_message(bytes_);
    if (message.status != codec::DecodeStatus::Done) {
      if (fin) {
        session.protocol_violation("peer bidirectional stream ended before first request");
      }
      return;
    }
    mode_ = Mode::Done;
    bytes_.clear();
    session.reject_peer_request(message.message.type, *stream);
  }

  std::weak_ptr<SessionImpl> session_;
  std::weak_ptr<StreamContext> stream_;
  Mode mode_ = Mode::Classify;
  ByteBuffer bytes_;
};

} // namespace

void SessionImpl::on_peer_stream_started(std::shared_ptr<StreamContext> stream) {
  spdlog::debug("Peer started a {} stream (id={})", stream->unidirectional() ? "unidirectional" : "bidirectional",
                stream->id());
  stream->set_sink(std::make_shared<PeerStreamGate>(weak_from_this(), stream));
}

} // namespace moq::detail

namespace moq {

SubscriptionHandle::SubscriptionHandle(RequestId request_id, std::weak_ptr<detail::SessionImpl> session)
    : request_id_(request_id), session_(std::move(session)) {}

RequestId SubscriptionHandle::request_id() const { return request_id_; }

std::optional<TrackAlias> SubscriptionHandle::track_alias() const { return state().track_alias; }

SubscriptionStateSnapshot SubscriptionHandle::state() const {
  const std::shared_ptr<detail::SessionImpl> session = session_.lock();
  if (!session) {
    SubscriptionStateSnapshot terminated;
    terminated.request_id = request_id_;
    return terminated;
  }
  return session->subscription_state(request_id_);
}

MoqSubscriberSession::MoqSubscriberSession(std::shared_ptr<detail::SessionImpl> impl) : impl_(std::move(impl)) {}

std::future<std::unique_ptr<MoqSubscriberSession>> MoqSubscriberSession::connect(MsQuicClientConfig msquic_config,
                                                                                 SubscriberConfig subscriber_config) {
  std::promise<std::unique_ptr<MoqSubscriberSession>> promise;
  std::future<std::unique_ptr<MoqSubscriberSession>> future = promise.get_future();
  try {
    auto impl = std::make_shared<detail::SessionImpl>(std::move(msquic_config), std::move(subscriber_config));
    impl->start();
    promise.set_value(std::unique_ptr<MoqSubscriberSession>(new MoqSubscriberSession(std::move(impl))));
  } catch (...) {
    promise.set_exception(std::current_exception());
  }
  return future;
}

std::future<void> MoqSubscriberSession::ready() { return impl_->ready(); }

SessionStateSnapshot MoqSubscriberSession::state() const { return impl_->state(); }

std::future<SubscriptionHandle> MoqSubscriberSession::subscribe(SubscribeRequest request,
                                                                std::shared_ptr<ObjectHandler> handler) {
  return impl_->subscribe(std::move(request), std::move(handler));
}

std::future<RequestOk> MoqSubscriberSession::request_update(RequestId existing_request_id, RequestUpdate update) {
  return impl_->request_update(existing_request_id, std::move(update));
}

std::future<void> MoqSubscriberSession::stop_subscription(RequestId request_id) {
  return impl_->stop_subscription(request_id);
}

void MoqSubscriberSession::close(SessionCloseErrorCode error) { impl_->close(error); }

MoqSubscriberSession::~MoqSubscriberSession() {
  if (impl_) {
    impl_->close_and_wait(SessionCloseErrorCode::NoError);
  }
}

} // namespace moq

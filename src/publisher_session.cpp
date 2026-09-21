#include "moq/publisher_session.h"

#include "moq/codec.h"
#include "msquic_transport_adapter.h"
#include "request/publisher_subscription.h"
#include "send_data_plane.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <future>
#include <mutex>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace moq::detail {
namespace {

// Settle a promise with an error; ignores promises that were already satisfied.
template <typename Promise> void fail(Promise &promise, const std::string &message) {
  try {
    promise.set_exception(std::make_exception_ptr(std::runtime_error(message)));
  } catch (const std::future_error &) {
  }
}

std::string default_authority(const MsQuicClientConfig &config) {
  return config.authority.empty() ? config.host + ":" + std::to_string(config.port) : config.authority;
}

bool known_peer_request_type(uint64_t type) {
  static constexpr uint64_t kKnown[] = {codec::kMessageSubscribe,     codec::kMessagePublish,
                                        codec::kMessagePublishNamespace, codec::kMessageTrackStatus,
                                        codec::kMessageFetch,         codec::kMessageSubscribeNamespace,
                                        codec::kMessageSubscribeTracks};
  return std::find(std::begin(kKnown), std::end(kKnown), type) != std::end(kKnown);
}

bool reserved_namespace(const TrackNamespace &track_namespace) {
  return !track_namespace.empty() && track_namespace.front() == ".";
}

std::string namespace_text(const TrackNamespace &track_namespace) {
  std::string text;
  for (const std::string &field : track_namespace) {
    if (!text.empty()) {
      text += '/';
    }
    text += field;
  }
  return text;
}

std::string csv_field(const std::string &value) {
  std::string escaped;
  escaped.reserve(value.size() + 2);
  escaped.push_back('"');
  for (const char character : value) {
    if (character == '"') {
      escaped.push_back('"');
    }
    escaped.push_back(character);
  }
  escaped.push_back('"');
  return escaped;
}

// Sink for the bidi stream carrying a PUBLISH_NAMESPACE announcement. The
// relay answers with REQUEST_OK or REQUEST_ERROR; the stream then stays open
// for the lifetime of the announcement. Failures are logged, not fatal: the
// relay simply will not route SUBSCRIBEs for the namespace to this session.
class NamespaceAnnouncementFSM final : public StreamSink {
public:
  explicit NamespaceAnnouncementFSM(std::string name) : name_(std::move(name)) {}

  void on_receive(const BytesView *chunks, size_t count, bool /*fin*/) override {
    for (size_t index = 0; index < count; ++index) {
      buffer_.insert(buffer_.end(), chunks[index].begin(), chunks[index].end());
    }
    while (true) {
      const codec::ControlMessageResult parsed = codec::read_control_message(buffer_);
      if (parsed.status != codec::DecodeStatus::Done) {
        return;
      }
      buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(parsed.bytes));
      if (parsed.message.type == codec::kMessageRequestOk) {
        spdlog::info("namespace \"{}\" announced", name_);
      } else if (parsed.message.type == codec::kMessageRequestError) {
        std::string error;
        const std::optional<RequestError> rejected = codec::decode_request_error(parsed.message.payload, error);
        spdlog::warn("relay refused PUBLISH_NAMESPACE for \"{}\": {}", name_,
                     rejected ? rejected->reason : error);
      } else {
        spdlog::debug("ignoring message {} on PUBLISH_NAMESPACE stream", parsed.message.type);
      }
    }
  }

  void on_peer_send_aborted(uint64_t error_code) override {
    spdlog::warn("relay reset PUBLISH_NAMESPACE stream for \"{}\" (error {})", name_, error_code);
  }

private:
  std::string name_;
  ByteBuffer buffer_;
};

} // namespace

class PublisherSessionImpl : public std::enable_shared_from_this<PublisherSessionImpl>,
                             public PublisherRequestOwner {
public:
  PublisherSessionImpl(MsQuicClientConfig msquic_config, PublisherConfig publisher_config)
      : msquic_config_(std::move(msquic_config)), publisher_config_(publisher_config),
        send_plane_(SendDataPlane::Callbacks{
            [this] { return open_data_stream(); },
            [this](ByteBuffer bytes) { return transport_ && transport_->send_datagram(std::move(bytes)); },
            [this](RequestId request_id, PublishDoneCode code, std::string reason) {
              complete_subscription(request_id, code, std::move(reason));
            }}) {
    const char *event_csv_path = std::getenv("MOQTOPUS_PUBLISHER_EVENT_CSV");
    if (event_csv_path != nullptr && event_csv_path[0] != '\0') {
      event_csv_.open(event_csv_path, std::ios::out | std::ios::trunc);
      if (event_csv_) {
        event_csv_ << "timestamp_ns,sequence,event,request_id,track_namespace,track_name,error_code,detail\n";
        event_csv_.flush();
      } else {
        spdlog::warn("failed to open publisher event CSV at {}", event_csv_path);
      }
    }
  }

  ~PublisherSessionImpl() override { close(SessionCloseErrorCode::NoError); }

  void start() {
    MsQuicTransportAdapter::Callbacks callbacks;
    callbacks.connected = [this] { on_connected(); };
    callbacks.peer_stream_started = [this](std::shared_ptr<StreamContext> stream) {
      on_peer_stream_started(std::move(stream));
    };
    callbacks.datagram_received = [](BytesView) { /* a publisher-only session receives no objects */ };
    callbacks.transport_error = [this](std::string error) {
      const std::lock_guard<std::recursive_mutex> lock(mutex_);
      begin_close(SessionCloseErrorCode::InternalError, std::move(error));
    };
    callbacks.shutdown_complete = [this](bool handshake) { shutdown_complete(handshake); };
    transport_ = std::make_unique<MsQuicTransportAdapter>(msquic_config_, std::move(callbacks));
    transport_->start();
  }

  // waits until session is either ready or closed
  std::future<void> ready() {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::promise<void> promise;
    std::future<void> future = promise.get_future();
    if (phase_ == SessionPhase::Ready || phase_ == SessionPhase::Draining) {
      promise.set_value();
    } else if (phase_ == SessionPhase::Closing || phase_ == SessionPhase::Closed) {
      fail(promise, "MOQT session closed before SETUP completed");
    } else {
      ready_waiters_.push_back(std::move(promise));
    }
    return future;
  }

  SessionStateSnapshot state() const {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    SessionStateSnapshot snapshot;
    snapshot.phase = phase_;
    snapshot.local_setup_sent = local_setup_sent_;
    snapshot.peer_setup_received = peer_setup_received_;
    snapshot.negotiated_version =
        phase_ >= SessionPhase::Ready ? std::optional<std::string>(msquic_config_.alpn) : std::nullopt;
    snapshot.active_subscriptions = subscriptions_.size();
    snapshot.close_reason = close_reason_;
    return snapshot;
  }

  void register_track(PublishedTrack track) {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (reserved_namespace(track.track_namespace)) {
      spdlog::warn("track namespace \".\" is reserved; track \"{}\" not registered", track.track_name);
      return;
    }
    const TrackNamespace track_namespace = track.track_namespace;
    if (!send_plane_.register_track(std::move(track))) {
      spdlog::warn("register_track ignored: track is already registered");
      return;
    }
    if (phase_ == SessionPhase::Ready || phase_ == SessionPhase::Draining) {
      announce_namespace(track_namespace);
    } else {
      pending_announcements_.push_back(track_namespace);
    }
  }

  void unregister_track(const TrackNamespace &track_namespace, const TrackName &track_name) {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    end_track_locked(track_namespace, track_name, PublishDoneCode::TrackEnded, "track unregistered");
    if (send_plane_.unregister_track(track_namespace, track_name) &&
        !send_plane_.has_track_in_namespace(track_namespace)) {
      withdraw_namespace(track_namespace);
    }
  }

  void publish(PublishedObject object) {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (phase_ != SessionPhase::Ready && phase_ != SessionPhase::Draining) {
      spdlog::warn("publish dropped: session is not ready");
      return;
    }
    send_plane_.publish(object);
  }

  void end_track(const TrackNamespace &track_namespace, const TrackName &track_name, PublishDoneCode code,
                 std::string reason) {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    end_track_locked(track_namespace, track_name, code, std::move(reason));
  }

  void close(SessionCloseErrorCode error) {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    begin_close(error, "local close");
  }

  void close_and_wait(SessionCloseErrorCode error) {
    std::unique_ptr<MsQuicTransportAdapter> transport;
    {
      const std::lock_guard<std::recursive_mutex> lock(mutex_);
      begin_close(error, "local close");
      // Keep this impl alive while the adapter drains its callbacks. Destroying
      // it from the final callback would close that callback's stream re-entrantly.
      transport = std::move(transport_);
    }
    transport.reset();
  }

  // ---- called by the peer stream gate ----

  void handle_peer_setup() {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    spdlog::debug("Peer SETUP received");
    peer_setup_received_ = true;
    maybe_ready();
  }

  void handle_protocol_violation(std::string error) {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    begin_close(SessionCloseErrorCode::ProtocolViolation, std::move(error));
  }

  void handle_peer_request(codec::ControlMessage first_message, const std::shared_ptr<StreamContext> &stream,
                           ByteBuffer leftover, bool fin) {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (phase_ == SessionPhase::Closing || phase_ == SessionPhase::Closed) {
      return;
    }
    if (phase_ != SessionPhase::Ready && phase_ != SessionPhase::Draining) {
      begin_close(SessionCloseErrorCode::ProtocolViolation, "peer request before SETUP completed");
      return;
    }
    try {
      if (first_message.type == codec::kMessageSubscribe) {
        accept_subscribe(first_message.payload, stream, std::move(leftover), fin);
        return;
      }
      if (!known_peer_request_type(first_message.type)) {
        begin_close(SessionCloseErrorCode::ProtocolViolation,
                    "unknown peer request type " + std::to_string(first_message.type));
        return;
      }
      const bool publish = first_message.type == codec::kMessagePublish;
      stream->send(codec::encode_request_error(publish ? RequestErrorCode::Uninterested
                                                       : RequestErrorCode::NotSupported,
                                               publish ? "publisher is not accepting PUBLISH"
                                                       : "publisher-only implementation"),
                   true);
      stream->abort_receive(static_cast<uint64_t>(StreamResetCode::Cancelled));
    } catch (const std::exception &error) {
      begin_close(SessionCloseErrorCode::InternalError, error.what());
    }
  }

  // ---- PublisherRequestOwner ----

  std::unique_lock<std::recursive_mutex> lock_session() override {
    return std::unique_lock<std::recursive_mutex>(mutex_);
  }

  void protocol_violation(std::string error) override {
    begin_close(SessionCloseErrorCode::ProtocolViolation, std::move(error));
  }

  void invalid_request_id(std::string error) override {
    begin_close(SessionCloseErrorCode::InvalidRequestId, std::move(error));
  }

  bool consume_peer_request_id(RequestId request_id, std::string &error) override {
    if ((request_id & 1U) == 0) { // the peer is the server and uses odd Request IDs
      error = "peer Request ID " + std::to_string(request_id) + " has client parity";
      return false;
    }
    if (!peer_request_ids_.insert(request_id).second) {
      error = "duplicate peer Request ID " + std::to_string(request_id);
      return false;
    }
    return true;
  }

  SendDataPlane &send_plane() override { return send_plane_; }

  void subscription_transport_event(RequestId request_id, const TrackNamespace &track_namespace,
                                    const TrackName &track_name, const std::string &event,
                                    uint64_t error_code) override {
    record_subscription_event(event, request_id, track_namespace, track_name, std::to_string(error_code), {});
  }

  void subscription_closed(RequestId subscribe_request_id) override {
    subscriptions_.erase(subscribe_request_id);
  }

private:
  // Send SETUP on connection establishment
  void on_connected() {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
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
      maybe_ready();
    } catch (const std::exception &error) {
      begin_close(SessionCloseErrorCode::InternalError, error.what());
    }
  }

  void on_peer_stream_started(std::shared_ptr<StreamContext> stream); // defined after the gate

  void maybe_ready() {
    if (!local_setup_sent_ || !peer_setup_received_ || phase_ == SessionPhase::Closing ||
        phase_ == SessionPhase::Closed) {
      return;
    }
    spdlog::debug("MOQT SETUP completed; session ready");
    phase_ = SessionPhase::Ready;
    for (auto &waiter : ready_waiters_) {
      waiter.set_value();
    }
    ready_waiters_.clear();
    for (const TrackNamespace &track_namespace : pending_announcements_) {
      announce_namespace(track_namespace);
    }
    pending_announcements_.clear();
  }

  // Advertise a namespace to the relay so it routes SUBSCRIBEs here.
  void announce_namespace(const TrackNamespace &track_namespace) {
    const std::string name = namespace_text(track_namespace);
    if (announced_namespaces_.count(name) != 0) {
      return;
    }
    try {
      auto stream = transport_->open_stream(false);
      stream->set_sink(std::make_shared<NamespaceAnnouncementFSM>(name));
      const RequestId request_id = allocate_local_request_id();
      if (!stream->send(codec::encode_publish_namespace(request_id, track_namespace))) {
        throw std::runtime_error("StreamSend failed for PUBLISH_NAMESPACE");
      }
      spdlog::debug("PUBLISH_NAMESPACE sent for \"{}\" request={}", name, request_id);
      announced_namespaces_.emplace(name, std::move(stream));
    } catch (const std::exception &error) {
      spdlog::warn("PUBLISH_NAMESPACE for \"{}\" failed: {}", name, error.what());
    }
  }

  void withdraw_namespace(const TrackNamespace &track_namespace) {
    pending_announcements_.erase(
        std::remove(pending_announcements_.begin(), pending_announcements_.end(), track_namespace),
        pending_announcements_.end());

    const std::string name = namespace_text(track_namespace);
    const auto found = announced_namespaces_.find(name);
    if (found == announced_namespaces_.end()) {
      return;
    }
    found->second->abort_send(static_cast<uint64_t>(StreamResetCode::Cancelled));
    found->second->abort_receive(static_cast<uint64_t>(StreamResetCode::Cancelled));
    announced_namespaces_.erase(found);
    spdlog::debug("PUBLISH_NAMESPACE withdrawn for \"{}\"", name);
  }

  RequestId allocate_local_request_id() {
    const RequestId request_id = next_local_request_id_;
    next_local_request_id_ += 2; // client uses even Request IDs
    return request_id;
  }

  void accept_subscribe(const ByteBuffer &payload, const std::shared_ptr<StreamContext> &stream, ByteBuffer leftover,
                        bool fin) {
    std::string error;
    const std::optional<codec::Subscribe> subscribe = codec::decode_subscribe(payload, error);
    if (!subscribe) {
      begin_close(SessionCloseErrorCode::ProtocolViolation, std::move(error));
      return;
    }
    record_subscription_event("SUBSCRIBE_RECEIVED", subscribe->request_id, subscribe->track_namespace,
                              subscribe->track_name, {}, {});
    if (!consume_peer_request_id(subscribe->request_id, error)) {
      begin_close(SessionCloseErrorCode::InvalidRequestId, std::move(error));
      return;
    }
    codec::SubscriptionOptions options;
    if (!codec::decode_subscription_options(subscribe->parameters, options, error)) {
      begin_close(SessionCloseErrorCode::ProtocolViolation, std::move(error));
      return;
    }

    const auto reject = [this, &stream, &subscribe](RequestErrorCode code, const std::string &reason) {
      record_subscription_event("SUBSCRIBE_REJECTED", subscribe->request_id, subscribe->track_namespace,
                                subscribe->track_name, std::to_string(static_cast<uint64_t>(code)), reason);
      spdlog::debug("rejecting SUBSCRIBE: code={} reason={}", static_cast<uint64_t>(code), reason);
      stream->send(codec::encode_request_error(code, reason), true);
      stream->abort_receive(static_cast<uint64_t>(StreamResetCode::Cancelled));
    };
    if (reserved_namespace(subscribe->track_namespace)) {
      return reject(RequestErrorCode::DoesNotExist, "reserved track namespace");
    }
    if (subscriptions_.size() >= publisher_config_.max_subscriptions) {
      return reject(RequestErrorCode::ExcessiveLoad, "subscription limit reached");
    }
    const PublishedTrack *track = send_plane_.find_track(subscribe->track_namespace, subscribe->track_name);
    if (!track) {
      return reject(RequestErrorCode::DoesNotExist, "track is not published here");
    }

    const TrackAlias track_alias = next_track_alias_++;
    const SubscriptionDecision decision = send_plane_.attach_subscription(
        subscribe->request_id, track_alias, subscribe->track_namespace, subscribe->track_name, options);
    if (!decision.ok) {
      return reject(decision.code, decision.reason);
    }
    record_subscription_event("SUBSCRIBE_ACCEPTED", subscribe->request_id, subscribe->track_namespace,
                              subscribe->track_name, {}, {});

    auto fsm = std::make_shared<PublisherSubscriptionFSM>(subscribe->request_id, subscribe->track_namespace,
                                                          subscribe->track_name, track_alias, stream,
                                                          weak_from_this());
    stream->set_sink(fsm);
    subscriptions_.emplace(subscribe->request_id, fsm);

    std::vector<Parameter> parameters;
    if (const auto largest = send_plane_.largest_location(subscribe->track_namespace, subscribe->track_name)) {
      parameters.push_back(Parameter::location(codec::kParameterLargestObject, *largest));
    }
    if (!stream->send(codec::encode_subscribe_ok(track_alias, std::move(parameters), track->track_properties))) {
      begin_close(SessionCloseErrorCode::InternalError, "StreamSend failed for SUBSCRIBE_OK");
      return;
    }
    spdlog::debug("accepted SUBSCRIBE request={} track=\"{}\" alias={}", subscribe->request_id, subscribe->track_name,
                  track_alias);
    fsm->seed(std::move(leftover), fin);
  }

  void end_track_locked(const TrackNamespace &track_namespace, const TrackName &track_name, PublishDoneCode code,
                        std::string reason) {
    const std::optional<RequestId> request_id = send_plane_.subscription_for_track(track_namespace, track_name);
    if (!request_id) {
      return;
    }
    const auto found = subscriptions_.find(*request_id);
    if (found != subscriptions_.end()) {
      const auto fsm = found->second; // finish() erases the map entry
      fsm->finish(code, reason);
    }
  }

  void complete_subscription(RequestId request_id, PublishDoneCode code, std::string reason) {
    const auto found = subscriptions_.find(request_id);
    if (found != subscriptions_.end()) {
      const auto fsm = found->second; // finish() erases the map entry
      fsm->finish(code, reason);
    }
  }

  void record_subscription_event(const std::string &event, RequestId request_id,
                                 const TrackNamespace &track_namespace, const TrackName &track_name,
                                 const std::string &error_code, const std::string &detail) {
    if (!event_csv_) {
      return;
    }
    const auto timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count();
    event_csv_ << timestamp_ns << ',' << event_sequence_++ << ',' << csv_field(event) << ',' << request_id << ','
               << csv_field(namespace_text(track_namespace)) << ',' << csv_field(track_name) << ','
               << csv_field(error_code) << ',' << csv_field(detail) << '\n';
    event_csv_.flush();
  }

  std::shared_ptr<StreamContext> open_data_stream() {
    try {
      return transport_->open_stream(true);
    } catch (const std::exception &error) {
      begin_close(SessionCloseErrorCode::InternalError, error.what());
      return nullptr;
    }
  }

  void begin_close(SessionCloseErrorCode code, std::string reason) {
    if (phase_ == SessionPhase::Closed || phase_ == SessionPhase::Closing) {
      return;
    }
    spdlog::debug("Session beginning close: code={} reason={}", static_cast<uint64_t>(code), reason);
    phase_ = SessionPhase::Closing;
    close_reason_ = SessionCloseReason{code, reason};
    fail_ready_waiters(reason.empty() ? "MOQT session closing" : reason);
    // cancel() erases from subscriptions_ via subscription_closed()
    const auto active = subscriptions_;
    for (const auto &entry : active) {
      if (entry.second) {
        entry.second->cancel(static_cast<uint64_t>(StreamResetCode::SessionClosed));
      }
    }
    if (transport_) {
      transport_->shutdown(code);
    }
  }

  void shutdown_complete(bool handshake_completed) {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    spdlog::debug("Session shutdown complete: handshake_completed={}", handshake_completed);
    if (!handshake_completed && !close_reason_) {
      close_reason_ = SessionCloseReason{SessionCloseErrorCode::InternalError, "QUIC handshake did not complete"};
    }
    phase_ = SessionPhase::Closed;
    fail_ready_waiters("MOQT session closed");
  }

  void fail_ready_waiters(const std::string &reason) {
    for (auto &waiter : ready_waiters_) {
      fail(waiter, reason);
    }
    ready_waiters_.clear();
  }

  MsQuicClientConfig msquic_config_;
  PublisherConfig publisher_config_;
  SendDataPlane send_plane_;

  mutable std::recursive_mutex mutex_;
  SessionPhase phase_ = SessionPhase::Init;
  bool local_setup_sent_ = false;
  bool peer_setup_received_ = false;
  std::optional<SessionCloseReason> close_reason_;
  std::shared_ptr<StreamContext> local_setup_stream_;
  std::vector<std::promise<void>> ready_waiters_;
  TrackAlias next_track_alias_ = 0;
  RequestId next_local_request_id_ = 0;
  std::unordered_set<RequestId> peer_request_ids_;
  std::unordered_map<RequestId, std::shared_ptr<PublisherSubscriptionFSM>> subscriptions_;
  std::vector<TrackNamespace> pending_announcements_;
  std::unordered_map<std::string, std::shared_ptr<StreamContext>> announced_namespaces_;
  std::ofstream event_csv_;
  uint64_t event_sequence_ = 0;

  // Destroyed first: its destructor waits for in-flight transport callbacks,
  // which still reference the members above.
  std::unique_ptr<MsQuicTransportAdapter> transport_;
};

namespace {

// Cold path: buffers the first bytes of a peer-initiated stream until it can
// be classified. Bidirectional streams carry one request; SUBSCRIBE is handed
// to the session (which installs the subscription FSM as the new sink), and
// everything else is rejected. A publisher-only session never accepts
// incoming data streams.
class PublisherStreamGate final : public StreamSink {
public:
  PublisherStreamGate(std::weak_ptr<PublisherSessionImpl> session, std::weak_ptr<StreamContext> stream)
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
          return session->handle_protocol_violation("padding stream contains non-zero bytes");
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
      dispatch_request(*session, stream, fin);
    }
  }

private:
  enum class Mode { Classify, Done, Padding };

  static bool all_zero(BytesView bytes) {
    return std::all_of(bytes.begin(), bytes.end(), [](uint8_t byte) { return byte == 0; });
  }

  // Unidirectional streams are classified by their first varint.
  void classify(PublisherSessionImpl &session, const std::shared_ptr<StreamContext> &stream, bool fin) {
    const codec::VarintResult type = codec::read_varint(bytes_);
    if (type.status != codec::DecodeStatus::Done) {
      if (fin) {
        session.handle_protocol_violation("peer unidirectional stream ended before type");
      }
      return;
    }
    if (type.value == codec::kSetupStreamType) {
      const codec::ControlMessageResult frame = codec::read_control_message(bytes_);
      if (frame.status != codec::DecodeStatus::Done) {
        if (fin) {
          session.handle_protocol_violation("peer setup stream ended mid-SETUP");
        }
        return;
      }
      std::string error;
      if (!codec::decode_setup(frame.message.payload, error)) {
        return session.handle_protocol_violation(std::move(error));
      }
      mode_ = Mode::Done;
      bytes_.clear();
      return session.handle_peer_setup();
    }
    if (type.value == codec::kPaddingStreamType) {
      mode_ = Mode::Padding;
      if (!all_zero(BytesView{bytes_.data() + type.bytes, bytes_.size() - type.bytes})) {
        return session.handle_protocol_violation("padding stream contains non-zero bytes");
      }
      bytes_.clear();
      return;
    }
    if (type.value == codec::kFetchStreamType || codec::is_subgroup_stream_type(type.value)) {
      // a publisher-only session subscribes to nothing, so no data stream is expected
      mode_ = Mode::Done;
      bytes_.clear();
      return stream->abort_receive(static_cast<uint64_t>(StreamResetCode::Cancelled));
    }
    session.handle_protocol_violation("unknown peer unidirectional stream type " + std::to_string(type.value));
  }

  // A peer-initiated bidirectional stream carries one request.
  void dispatch_request(PublisherSessionImpl &session, const std::shared_ptr<StreamContext> &stream, bool fin) {
    const codec::ControlMessageResult message = codec::read_control_message(bytes_);
    if (message.status != codec::DecodeStatus::Done) {
      if (fin) {
        session.handle_protocol_violation("peer bidirectional stream ended before first request");
      }
      return;
    }
    mode_ = Mode::Done;
    ByteBuffer leftover(bytes_.begin() + static_cast<std::ptrdiff_t>(message.bytes), bytes_.end());
    bytes_.clear();
    // On SUBSCRIBE the session swaps the sink to the subscription FSM and
    // seeds it with the leftover bytes; this gate then dies with the stream.
    session.handle_peer_request(message.message, stream, std::move(leftover), fin);
  }

  std::weak_ptr<PublisherSessionImpl> session_;
  std::weak_ptr<StreamContext> stream_;
  Mode mode_ = Mode::Classify;
  ByteBuffer bytes_;
};

} // namespace

void PublisherSessionImpl::on_peer_stream_started(std::shared_ptr<StreamContext> stream) {
  spdlog::debug("Peer started a {} stream (id={})", stream->unidirectional() ? "unidirectional" : "bidirectional",
                stream->id());
  stream->set_sink(std::make_shared<PublisherStreamGate>(weak_from_this(), stream));
}

} // namespace moq::detail

namespace moq {

MoqPublisherSession::MoqPublisherSession(std::shared_ptr<detail::PublisherSessionImpl> impl)
    : impl_(std::move(impl)) {}

std::future<std::unique_ptr<MoqPublisherSession>> MoqPublisherSession::connect(MsQuicClientConfig msquic_config,
                                                                               PublisherConfig publisher_config) {
  std::promise<std::unique_ptr<MoqPublisherSession>> promise;
  std::future<std::unique_ptr<MoqPublisherSession>> future = promise.get_future();
  try {
    auto impl = std::make_shared<detail::PublisherSessionImpl>(std::move(msquic_config), publisher_config);
    impl->start();
    promise.set_value(std::unique_ptr<MoqPublisherSession>(new MoqPublisherSession(std::move(impl))));
  } catch (...) {
    promise.set_exception(std::current_exception());
  }
  return future;
}

std::future<void> MoqPublisherSession::ready() { return impl_->ready(); }

SessionStateSnapshot MoqPublisherSession::state() const { return impl_->state(); }

void MoqPublisherSession::register_track(PublishedTrack track) { impl_->register_track(std::move(track)); }

void MoqPublisherSession::unregister_track(const TrackNamespace &track_namespace, const TrackName &track_name) {
  impl_->unregister_track(track_namespace, track_name);
}

void MoqPublisherSession::publish(PublishedObject object) { impl_->publish(std::move(object)); }

void MoqPublisherSession::end_track(const TrackNamespace &track_namespace, const TrackName &track_name,
                                    PublishDoneCode code, std::string reason) {
  impl_->end_track(track_namespace, track_name, code, std::move(reason));
}

void MoqPublisherSession::close(SessionCloseErrorCode error) { impl_->close(error); }

MoqPublisherSession::~MoqPublisherSession() {
  if (impl_) {
    impl_->close_and_wait(SessionCloseErrorCode::NoError);
  }
}

} // namespace moq

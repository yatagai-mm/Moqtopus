#include "moq/publisher_session.h"

#include "moq/codec.h"
#include "send_data_plane.h"
#include "session.h"

#include <algorithm>
#include <exception>
#include <future>
#include <mutex>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "moq/errors.h"
#include "moq/types.h"
#include <memory>
#include <string>

namespace moq::detail {

static bool reserved_namespace(const TrackNamespace &track_namespace) {
  return !track_namespace.empty() && track_namespace.front() == ".";
}

static std::string namespace_text(const TrackNamespace &track_namespace) {
  std::string text;
  for (const std::string &field : track_namespace) {
    if (!text.empty()) {
      text += '/';
    }
    text += field;
  }
  return text;
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
        spdlog::warn("relay refused PUBLISH_NAMESPACE for \"{}\": {}", name_, rejected ? rejected->reason : error);
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

class Publisher : public std::enable_shared_from_this<Publisher>, public Session {
  class PublisherSubscriptionFSM final : public StreamSink {
  public:
    PublisherSubscriptionFSM(RequestId request_id, TrackNamespace track_namespace, TrackName track_name,
                             std::shared_ptr<StreamContext> stream, std::weak_ptr<Publisher> owner)
        : request_id_(request_id), track_namespace_(std::move(track_namespace)), track_name_(std::move(track_name)),
          stream_(std::move(stream)), owner_(std::move(owner)) {}

    // StreamSink: entered from the transport thread; takes the session lock.
    void on_receive(const BytesView *chunks, size_t count, bool fin) override {
      const auto owner = owner_.lock();
      if (!owner) {
        return;
      }
      const auto lock = owner->lock_session();
      if (terminated_) {
        return;
      }
      for (size_t index = 0; index < count; ++index) {
        buffer_.insert(buffer_.end(), chunks[index].begin(), chunks[index].end());
      }
      process_buffer(*owner, fin);
    }
    void on_peer_send_aborted(uint64_t error_code) override {
      const auto owner = owner_.lock();
      if (!owner) {
        return;
      }
      const auto lock = owner->lock_session();
      spdlog::debug("subscriber reset request stream for request {} (error {})", request_id_, error_code);
      cancel(static_cast<uint64_t>(StreamResetCode::Cancelled));
    }
    void on_peer_receive_aborted(uint64_t error_code) override {
      const auto owner = owner_.lock();
      if (!owner) {
        return;
      }
      const auto lock = owner->lock_session();
      spdlog::debug("subscriber sent STOP_SENDING for request {} (error {})", request_id_, error_code);
      cancel(static_cast<uint64_t>(StreamResetCode::Cancelled));
    } // subscriber STOP_SENDING
    void on_stream_closed() override {
      const auto owner = owner_.lock();
      if (!owner) {
        return;
      }
      const auto lock = owner->lock_session();
      if (!terminated_) {
        cancel(static_cast<uint64_t>(StreamResetCode::Cancelled));
      }
    }

    // Local termination: close all data streams, then PUBLISH_DONE + FIN.
    void finish(PublishDoneCode code, const std::string &reason) {
      const auto owner = owner_.lock();
      if (!owner || terminated_) {
        return;
      }
      // PUBLISH_DONE must follow the closure of every data stream (Section 10.11).
      const uint64_t stream_count = owner->send_plane_.detach_subscription(request_id_);
      if (!stream_->send(codec::encode_publish_done(static_cast<uint64_t>(code), stream_count, reason), true)) {
        spdlog::warn("StreamSend failed for PUBLISH_DONE on request {}", request_id_);
      }
      stream_->abort_receive(static_cast<uint64_t>(StreamResetCode::Cancelled));
      terminated_ = true;
      owner->subscriptions_.erase(request_id_);
    }
    // Peer cancellation or session close: reset all streams, destroy state.
    void cancel(uint64_t reset_error_code) {
      const auto owner = owner_.lock();
      if (!owner || terminated_) {
        return;
      }
      owner->send_plane_.detach_subscription(request_id_, reset_error_code);
      stream_->abort_send(reset_error_code);
      stream_->abort_receive(reset_error_code);
      terminated_ = true;
      owner->subscriptions_.erase(request_id_);
    }

  private:
    void process_buffer(Publisher &owner, bool fin) {
      while (!terminated_) {
        const codec::ControlMessageResult parsed = codec::read_control_message(buffer_);
        if (parsed.status != codec::DecodeStatus::Done) {
          if (fin && !buffer_.empty()) {
            owner.protocol_violation("request stream ended mid-message");
          }
          return;
        }
        buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(parsed.bytes));
        if (parsed.message.type == codec::kMessageRequestUpdate) {
          handle_request_update(owner, parsed.message);
        } else {
          owner.protocol_violation("unexpected message " + std::to_string(parsed.message.type) +
                                   " on SUBSCRIBE request stream");
          return;
        }
      }
    }
    void handle_request_update(Publisher &owner, const codec::ControlMessage &message) {
      std::string error;
      const std::optional<codec::RequestUpdateMessage> update = codec::decode_request_update(message.payload, error);
      if (!update) {
        owner.protocol_violation(std::move(error));
        return;
      }
      if (!owner.consume_peer_request_id(update->request_id, error)) {
        owner.invalid_request_id(std::move(error));
        return;
      }
      codec::SubscriptionOptions options;
      if (!codec::decode_subscription_options(update->parameters, options, error)) {
        owner.protocol_violation(std::move(error));
        return;
      }

      const auto rejection = owner.send_plane_.update_subscription(request_id_, options);
      if (rejection) {
        // A rejected update ends the subscription (draft-18 section 10.9.1).
        stream_->send(codec::encode_request_error(rejection->code, rejection->reason));
        finish(PublishDoneCode::UpdateFailed, rejection->reason);
        return;
      }

      std::vector<Parameter> parameters;
      if (const auto *track = owner.send_plane_.find_track(track_namespace_, track_name_); track && track->largest) {
        parameters.push_back(Parameter::location(codec::kParameterLargestObject, *track->largest));
      }
      if (!stream_->send(codec::encode_request_ok(std::move(parameters)))) {
        spdlog::warn("StreamSend failed for REQUEST_OK on request {}", request_id_);
      }
    }

    RequestId request_id_;
    TrackNamespace track_namespace_;
    TrackName track_name_;
    std::shared_ptr<StreamContext> stream_;
    std::weak_ptr<Publisher> owner_;
    ByteBuffer buffer_;
    bool terminated_ = false;
  };

public:
  Publisher(MsQuicClientConfig msquic_config, PublisherConfig publisher_config)
      : Session(std::move(msquic_config)), publisher_config_(publisher_config),
        send_plane_(SendDataPlane::Callbacks{
            [this] { return open_data_stream(); },
            [this](ByteBuffer bytes) { return transport_ && transport_->send_datagram(std::move(bytes)); },
            [this](RequestId request_id, PublishDoneCode code, std::string reason) {
              complete_subscription(request_id, code, std::move(reason));
            }}) {}

  ~Publisher() override { close_and_wait(); }

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
    if (state_.phase == SessionPhase::Ready) {
      announce_namespace(track_namespace);
    } else {
      pending_announcements_.push_back(track_namespace);
    }
  }

  void unregister_track(const TrackNamespace &track_namespace, const TrackName &track_name) {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    end_track(track_namespace, track_name, PublishDoneCode::TrackEnded, "track unregistered");
    if (send_plane_.unregister_track(track_namespace, track_name) &&
        !send_plane_.has_track_in_namespace(track_namespace)) {
      withdraw_namespace(track_namespace);
    }
  }

  void publish(PublishedObject object) {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (state_.phase != SessionPhase::Ready) {
      spdlog::warn("publish dropped: session is not ready");
      return;
    }
    send_plane_.publish(object);
  }

  void end_track(const TrackNamespace &track_namespace, const TrackName &track_name, PublishDoneCode code,
                 std::string reason) {
    const auto lock = lock_session();
    const auto *track = send_plane_.find_track(track_namespace, track_name);
    if (!track)
      return;
    const std::vector<RequestId> request_ids(track->subscriptions.begin(), track->subscriptions.end());
    for (const RequestId request_id : request_ids) {
      complete_subscription(request_id, code, reason);
    }
  }

  void handle_data_stream(uint64_t, const std::shared_ptr<StreamContext> &stream, ByteBuffer, bool) {
    stream->abort_receive(static_cast<uint64_t>(StreamResetCode::Cancelled));
  }

  void handle_peer_request(codec::ControlMessage first_message, const std::shared_ptr<StreamContext> &stream,
                           ByteBuffer leftover, bool fin) {
    const std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (state_.phase == SessionPhase::Closing || state_.phase == SessionPhase::Closed) {
      return;
    }
    if (state_.phase != SessionPhase::Ready) {
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
      stream->send(
          codec::encode_request_error(publish ? RequestErrorCode::Uninterested : RequestErrorCode::NotSupported,
                                      publish ? "publisher is not accepting PUBLISH" : "publisher-only implementation"),
          true);
      stream->abort_receive(static_cast<uint64_t>(StreamResetCode::Cancelled));
    } catch (const std::exception &error) {
      begin_close(SessionCloseErrorCode::InternalError, error.what());
    }
  }

  void invalid_request_id(std::string error) { begin_close(SessionCloseErrorCode::InvalidRequestId, std::move(error)); }

  bool consume_peer_request_id(RequestId request_id, std::string &error) {
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

private:
  void on_peer_stream_started(std::shared_ptr<StreamContext> stream) override;

  void on_ready() override {
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
      const RequestId request_id = allocate_request_id();
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

  void accept_subscribe(const ByteBuffer &payload, const std::shared_ptr<StreamContext> &stream, ByteBuffer leftover,
                        bool fin) {
    std::string error;
    const std::optional<codec::Subscribe> subscribe = codec::decode_subscribe(payload, error);
    if (!subscribe) {
      begin_close(SessionCloseErrorCode::ProtocolViolation, std::move(error));
      return;
    }

    if (!consume_peer_request_id(subscribe->request_id, error)) {
      begin_close(SessionCloseErrorCode::InvalidRequestId, std::move(error));
      return;
    }
    codec::SubscriptionOptions options;
    if (!codec::decode_subscription_options(subscribe->parameters, options, error)) {
      begin_close(SessionCloseErrorCode::ProtocolViolation, std::move(error));
      return;
    }

    const auto reject = [&stream](RequestErrorCode code, const std::string &reason) {
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
    const auto *track = send_plane_.find_track(subscribe->track_namespace, subscribe->track_name);
    if (!track) {
      return reject(RequestErrorCode::DoesNotExist, "track is not published here");
    }

    const TrackAlias track_alias = next_track_alias_++;
    const auto rejection = send_plane_.attach_subscription(subscribe->request_id, track_alias,
                                                           subscribe->track_namespace, subscribe->track_name, options);
    if (rejection) {
      return reject(rejection->code, rejection->reason);
    }

    auto fsm = std::make_shared<PublisherSubscriptionFSM>(subscribe->request_id, subscribe->track_namespace,
                                                          subscribe->track_name, stream, weak_from_this());
    stream->set_sink(fsm);
    subscriptions_.emplace(subscribe->request_id, fsm);

    std::vector<Parameter> parameters;
    if (const auto largest = track->largest) {
      parameters.push_back(Parameter::location(codec::kParameterLargestObject, *largest));
    }
    if (!stream->send(codec::encode_subscribe_ok(track_alias, std::move(parameters), track->track.track_properties))) {
      begin_close(SessionCloseErrorCode::InternalError, "StreamSend failed for SUBSCRIBE_OK");
      return;
    }
    spdlog::debug("accepted SUBSCRIBE request={} track=\"{}\" alias={}", subscribe->request_id, subscribe->track_name,
                  track_alias);
    const BytesView tail{leftover};
    fsm->on_receive(&tail, 1, fin);
  }

  void complete_subscription(RequestId request_id, PublishDoneCode code, std::string reason) {
    const auto found = subscriptions_.find(request_id);
    if (found != subscriptions_.end()) {
      const auto fsm = found->second; // finish() erases the map entry
      fsm->finish(code, reason);
    }
  }

  std::shared_ptr<StreamContext> open_data_stream() {
    try {
      return transport_->open_stream(true);
    } catch (const std::exception &error) {
      begin_close(SessionCloseErrorCode::InternalError, error.what());
      return nullptr;
    }
  }

  PublisherConfig publisher_config_;
  SendDataPlane send_plane_;

  TrackAlias next_track_alias_ = 0;
  std::unordered_set<RequestId> peer_request_ids_;
  std::unordered_map<RequestId, std::shared_ptr<PublisherSubscriptionFSM>> subscriptions_;
  std::vector<TrackNamespace> pending_announcements_;
  std::unordered_map<std::string, std::shared_ptr<StreamContext>> announced_namespaces_;

  void terminate_subscriptions(const std::string &) override {
    const auto active = subscriptions_; // cancel() erases the map entry
    for (const auto &entry : active)
      entry.second->cancel(static_cast<uint64_t>(StreamResetCode::SessionClosed));
  }
  size_t active_subscriptions() const override { return subscriptions_.size(); }
};

void Publisher::on_peer_stream_started(std::shared_ptr<StreamContext> stream) {
  spdlog::debug("Peer started a {} stream (id={})", stream->unidirectional() ? "unidirectional" : "bidirectional",
                stream->id());
  stream->set_sink(std::make_shared<PeerStreamGate<Publisher>>(weak_from_this(), stream));
}

} // namespace moq::detail

namespace moq {

std::unique_ptr<Publisher> Publisher::connect(MsQuicClientConfig msquic_config, PublisherConfig publisher_config) {
  auto impl = std::make_shared<detail::Publisher>(std::move(msquic_config), publisher_config);
  impl->start();
  return std::unique_ptr<Publisher>(new Publisher(std::move(impl)));
}

std::future<void> Publisher::ready() { return impl_->ready(); }

SessionStateSnapshot Publisher::state() const { return impl_->state(); }

void Publisher::register_track(PublishedTrack track) { impl_->register_track(std::move(track)); }

void Publisher::unregister_track(const TrackNamespace &track_namespace, const TrackName &track_name) {
  impl_->unregister_track(track_namespace, track_name);
}

void Publisher::publish(PublishedObject object) { impl_->publish(std::move(object)); }

void Publisher::end_track(const TrackNamespace &track_namespace, const TrackName &track_name,
                          PublishDoneCode code, std::string reason) {
  impl_->end_track(track_namespace, track_name, code, std::move(reason));
}

void Publisher::close(SessionCloseErrorCode error) { impl_->close(error); }

Publisher::~Publisher() { impl_->close_and_wait(); }

} // namespace moq

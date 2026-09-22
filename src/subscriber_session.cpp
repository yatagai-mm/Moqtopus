#include "moq/subscriber_session.h"

#include "data_plane.h"
#include "moq/codec.h"
#include "msquic_transport_adapter.h"

#include <deque>
#include <exception>
#include <spdlog/spdlog.h>
#include <unordered_map>
#include <utility>

namespace moq {
// converts REQUEST_ERROR to a C++ exception
inline std::exception_ptr rejected_exception(const RequestError &error) {
  return std::make_exception_ptr(RequestRejected(error.code, error.retry_interval, error.reason));
}

class Subscriber::SubscriptionFSM final : public StreamSink {
public:
  SubscriptionFSM(RequestId request_id, std::shared_ptr<ObjectHandler> handler, std::shared_ptr<StreamContext> stream,
                  Subscriber &owner, std::shared_ptr<std::promise<Subscription>> result)
      : request_id_(request_id), stream_(std::move(stream)), handler_(std::move(handler)), owner_(owner),
        result_(std::move(result)) {}

  // StreamSink: control responses arriving on the SUBSCRIBE bidi stream.
  void on_receive(const BytesView *chunks, size_t count, bool fin) override {
    const auto lock = owner_.lock_session();
    for (size_t index = 0; index < count; ++index) {
      response_buffer_.insert(response_buffer_.end(), chunks[index].begin(), chunks[index].end());
    }
    while (phase_ != SubscriptionPhase::Terminated) {
      const codec::ControlMessageResult parsed = codec::read_control_message(response_buffer_);
      if (parsed.status != codec::DecodeStatus::Done) {
        if (fin && !response_buffer_.empty()) {
          terminate("request stream ended mid-message");
        }
        return;
      }
      response_buffer_.erase(response_buffer_.begin(), response_buffer_.begin() + parsed.bytes);
      handle_control_message(parsed.message);
    }
  }
  void on_peer_send_aborted(uint64_t error_code) override {
    const auto lock = owner_.lock_session();
    terminate("publisher reset request stream with error " + std::to_string(error_code));
  }
  void on_stream_closed() override {
    const auto lock = owner_.lock_session();
    terminate("request stream shut down before subscription ended");
  }

  // Enqueue a request-update to be sent on this stream.
  void send_request_update(RequestId allocated_request_id, RequestUpdate update, std::promise<RequestOk> &promise) {
    if (!stream_->send(codec::encode_request_update(allocated_request_id, update))) {
      throw std::runtime_error("StreamSend failed for REQUEST_UPDATE");
    }
    updates_.push_back(std::move(promise));
  }

  // Stop the subscription and optionally report error to handler.
  void terminate(std::string reason = {}) {
    if (phase_ == SubscriptionPhase::Terminated)
      return;
    if (result_) {
      fail(*result_, rejected_exception(
                         {RequestErrorCode::InternalError, 0, reason.empty() ? "subscription terminated" : reason}));
      result_.reset();
    }
    const auto message = reason.empty() ? "subscription terminated before REQUEST_UPDATE completed" : reason;
    fail_updates(std::make_exception_ptr(std::runtime_error(message)));
    phase_ = SubscriptionPhase::Terminated;
    if (track_alias_)
      owner_.data_plane_->retire_route(*track_alias_);
    owner_.subscriptions_.erase(request_id_);
    if (!reason.empty()) {
      handler_->on_error(ReceiveError{0, std::move(reason)});
    }
  }

  SubscriptionPhase phase() const { return phase_; }
  std::shared_ptr<StreamContext> stream() const { return stream_; }
  std::optional<TrackAlias> track_alias() const { return track_alias_; }
  size_t inflight_updates() const { return updates_.size(); }

private:
  void fail_updates(std::exception_ptr error) {
    while (!updates_.empty()) {
      fail(updates_.front(), error);
      updates_.pop_front();
    }
  }
  void handle_control_message(const codec::ControlMessage &message) {
    const bool pending = phase_ == SubscriptionPhase::Pending;
    if (pending && message.type != codec::kMessageSubscribeOk && message.type != codec::kMessageRequestError) {
      return terminate("invalid first response on SUBSCRIBE stream");
    }
    std::string error;
    switch (message.type) {
    case codec::kMessageSubscribeOk: {
      if (!pending)
        return terminate("duplicate SUBSCRIBE_OK");
      const auto ok = codec::decode_subscribe_ok(message.payload, error);
      if (!ok)
        return terminate(error);
      auto route = std::make_shared<ReceiveRoute>();
      route->request_id = request_id_;
      route->handler = handler_;
      if (!owner_.data_plane_->install_route(ok->track_alias, route)) {
        return owner_.begin_close(SessionCloseErrorCode::DuplicateTrackAlias,
                                  "SUBSCRIBE_OK reused an established Track Alias");
      }
      if (phase_ == SubscriptionPhase::Terminated) {
        owner_.data_plane_->retire_route(ok->track_alias);
        return;
      }
      track_alias_ = ok->track_alias;
      phase_ = SubscriptionPhase::Established;
      result_->set_value(Subscription{request_id_, *track_alias_});
      result_.reset();
      return;
    }
    case codec::kMessageRequestError: {
      const auto rejected = codec::decode_request_error(message.payload, error);
      if (!rejected)
        return terminate(error);
      if (pending) {
        fail(*result_, rejected_exception(*rejected));
        result_.reset();
        return terminate();
      }
      if (updates_.empty())
        return terminate("REQUEST_ERROR arrived without an in-flight request");
      fail_updates(rejected_exception(*rejected));
      phase_ = SubscriptionPhase::UpdateFailed;
      return;
    }
    case codec::kMessageRequestOk: {
      if (updates_.empty())
        return terminate("REQUEST_OK arrived without an in-flight REQUEST_UPDATE");
      const auto ok = codec::decode_request_ok(message.payload, error);
      if (!ok)
        return terminate(error);
      if (!ok->track_properties.empty())
        return terminate("REQUEST_UPDATE_OK included Track Properties");
      updates_.front().set_value(*ok);
      updates_.pop_front();
      return;
    }
    case codec::kMessagePublishDone: {
      const auto done = codec::decode_publish_done(message.payload, error);
      if (!done)
        return terminate(error);
      handler_->on_publish_done(*done);
      return terminate();
    }
    case codec::kMessageGoAway:
      return;
    default:
      handler_->on_error(ReceiveError{0, "invalid response on SUBSCRIBE stream: " + std::to_string(message.type)});
    }
  }

  RequestId request_id_;
  SubscriptionPhase phase_ = SubscriptionPhase::Pending;
  std::optional<TrackAlias> track_alias_;
  std::shared_ptr<StreamContext> stream_;
  ByteBuffer response_buffer_;
  std::shared_ptr<ObjectHandler> handler_;
  std::deque<std::promise<RequestOk>> updates_; // settled in FIFO order by REQUEST_OK / REQUEST_ERROR
  Subscriber &owner_;
  std::shared_ptr<std::promise<Subscription>> result_;
};

Subscriber::~Subscriber() { close_and_wait(); }

std::unique_ptr<Subscriber> Subscriber::connect(MsQuicClientConfig msquic_config, SubscriberConfig subscriber_config) {
  auto session = std::unique_ptr<Subscriber>(new Subscriber(std::move(msquic_config), subscriber_config));
  session->start();
  return session;
}

Subscriber::Subscriber(MsQuicClientConfig msquic_config, SubscriberConfig subscriber_config)
    : Session(std::move(msquic_config)),
      data_plane_(std::make_unique<DataPlane>(
          subscriber_config, [this](std::string error) { protocol_violation(std::move(error)); },
          [this](RequestId request_id, std::string error) {
            const auto lock = lock_session();
            stop_subscription_now(request_id, std::move(error),
                                  static_cast<uint64_t>(StreamResetCode::MALFORMED_TRACK));
          })) {}

SubscriptionStateSnapshot Subscriber::subscription_state(RequestId request_id) const {
  const auto lock = lock_session();
  const auto found = subscriptions_.find(request_id);
  if (found == subscriptions_.end())
    return {SubscriptionPhase::Terminated, request_id, {}, 0};
  const auto &fsm = *found->second;
  return {fsm.phase(), request_id, fsm.track_alias(), fsm.inflight_updates()};
}

void Subscriber::stop_subscription(RequestId request_id) {
  const auto lock = lock_session();
  stop_subscription_now(request_id, {}, static_cast<uint64_t>(StreamResetCode::Cancelled));
}

void Subscriber::handle_data_stream(uint64_t type, const std::shared_ptr<StreamContext> &stream, ByteBuffer prefix,
                                    bool fin) {
  if (type == codec::kFetchStreamType)
    return stream->abort_receive(0);
  data_plane_->start_subgroup_stream(stream, std::move(prefix), fin);
}

std::future<Subscription> Subscriber::subscribe(SubscribeRequest request, std::shared_ptr<ObjectHandler> handler) {
  const auto lock = lock_session();
  auto promise = std::make_shared<std::promise<Subscription>>();
  std::future<Subscription> future = promise->get_future();
  if (!handler) {
    fail(*promise, "SUBSCRIBE requires an ObjectHandler");
    return future;
  }
  if (state_.phase != SessionPhase::Ready) {
    fail(*promise, "MOQT session is not ready for SUBSCRIBE");
    return future;
  }
  try {
    const RequestId request_id = allocate_request_id();
    auto stream = transport_->open_stream(false);
    auto fsm = std::make_shared<SubscriptionFSM>(request_id, std::move(handler), stream, *this, promise);
    // Install request state before sending so an immediate response cannot race sink setup.
    stream->set_sink(fsm);
    subscriptions_.emplace(request_id, fsm);

    if (!stream->send(codec::encode_subscribe(request_id, request))) {
      stop_subscription_now(request_id, "StreamSend failed for SUBSCRIBE");
      throw std::runtime_error("StreamSend failed for SUBSCRIBE");
    }
  } catch (...) {
    fail(*promise, std::current_exception());
  }
  return future;
}

std::future<RequestOk> Subscriber::request_update(RequestId existing_request_id, RequestUpdate update) {
  const auto lock = lock_session();
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
    fsm->send_request_update(request_id, std::move(update), promise);
  } catch (...) {
    fail(promise, std::current_exception());
  }
  return future;
}

void Subscriber::handle_peer_request(const codec::ControlMessage &message, const std::shared_ptr<StreamContext> &stream,
                                     ByteBuffer, bool) {
  const auto request_type = message.type;
  if (!known_peer_request_type(request_type)) {
    protocol_violation("unknown peer request type " + std::to_string(request_type));
    return;
  }
  const bool publish = request_type == codec::kMessagePublish;
  stream->send(
      codec::encode_request_error(publish ? RequestErrorCode::Uninterested : RequestErrorCode::NotSupported,
                                  publish ? "subscriber is not accepting PUBLISH" : "subscriber-only implementation"),
      true);
  stream->abort_receive(0);
}

void Subscriber::stop_subscription_now(RequestId request_id, std::string reason, uint64_t stream_error_code) {
  const auto found = subscriptions_.find(request_id);
  if (found == subscriptions_.end()) {
    spdlog::debug("stop_subscription_now: no subscription found for request_id={}", request_id);
    return;
  }
  auto fsm = found->second;
  fsm->terminate(std::move(reason));
  if (auto stream = fsm->stream()) {
    stream->abort_send(stream_error_code);
    stream->abort_receive(stream_error_code);
  }
}

void Subscriber::on_datagram(BytesView bytes) { data_plane_->on_datagram(bytes); }

void Subscriber::terminate_subscriptions(const std::string &reason) {
  const auto active = subscriptions_; // terminate() erases its entry
  for (const auto &entry : active)
    entry.second->terminate(reason);
}

size_t Subscriber::active_subscriptions() const { return subscriptions_.size(); }

} // namespace moq

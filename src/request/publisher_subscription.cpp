#include "request/publisher_subscription.h"

#include <spdlog/spdlog.h>
#include <utility>

namespace moq::detail {

PublisherSubscriptionFSM::PublisherSubscriptionFSM(RequestId request_id, TrackNamespace track_namespace,
                                                   TrackName track_name, TrackAlias track_alias,
                                                   std::shared_ptr<StreamContext> stream,
                                                   std::weak_ptr<PublisherRequestOwner> owner)
    : request_id_(request_id), track_namespace_(std::move(track_namespace)), track_name_(std::move(track_name)),
      track_alias_(track_alias), stream_(std::move(stream)), owner_(std::move(owner)) {}

void PublisherSubscriptionFSM::on_receive(const BytesView *chunks, size_t count, bool fin) {
  const auto owner = owner_.lock();
  if (!owner) {
    return;
  }
  const auto lock = owner->lock_session();
  if (phase_ == moq::SubscriptionPhase::Terminated) {
    return;
  }
  for (size_t index = 0; index < count; ++index) {
    buffer_.insert(buffer_.end(), chunks[index].begin(), chunks[index].end());
  }
  process_buffer(*owner, fin);
}

void PublisherSubscriptionFSM::on_peer_send_aborted(uint64_t error_code) {
  const auto owner = owner_.lock();
  if (!owner) {
    return;
  }
  const auto lock = owner->lock_session();
  owner->subscription_transport_event(request_id_, track_namespace_, track_name_, "RESET_STREAM_RECEIVED",
                                      error_code);
  spdlog::debug("subscriber reset request stream for request {} (error {})", request_id_, error_code);
  cancel(static_cast<uint64_t>(StreamResetCode::Cancelled));
}

void PublisherSubscriptionFSM::on_peer_receive_aborted(uint64_t error_code) {
  const auto owner = owner_.lock();
  if (!owner) {
    return;
  }
  const auto lock = owner->lock_session();
  owner->subscription_transport_event(request_id_, track_namespace_, track_name_, "STOP_SENDING_RECEIVED",
                                      error_code);
  spdlog::debug("subscriber sent STOP_SENDING for request {} (error {})", request_id_, error_code);
  cancel(static_cast<uint64_t>(StreamResetCode::Cancelled));
}

void PublisherSubscriptionFSM::on_stream_closed() {
  const auto owner = owner_.lock();
  if (!owner) {
    return;
  }
  const auto lock = owner->lock_session();
  if (phase_ != moq::SubscriptionPhase::Terminated) {
    cancel(static_cast<uint64_t>(StreamResetCode::Cancelled));
  }
}

void PublisherSubscriptionFSM::seed(ByteBuffer leftover, bool fin) {
  const auto owner = owner_.lock();
  if (!owner) {
    return;
  }
  buffer_ = std::move(leftover);
  if (!buffer_.empty() || fin) {
    process_buffer(*owner, fin);
  }
}

void PublisherSubscriptionFSM::process_buffer(PublisherRequestOwner &owner, bool fin) {
  while (phase_ == moq::SubscriptionPhase::Established) {
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

void PublisherSubscriptionFSM::handle_request_update(PublisherRequestOwner &owner,
                                                     const codec::ControlMessage &message) {
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

  const SubscriptionDecision decision = owner.send_plane().update_subscription(request_id_, options);
  if (!decision.ok) {
    reject_update_and_finish(owner, decision.code, decision.reason);
    return;
  }

  std::vector<Parameter> parameters;
  if (const auto largest = owner.send_plane().largest_location(track_namespace_, track_name_)) {
    parameters.push_back(Parameter::location(codec::kParameterLargestObject, *largest));
  }
  if (!stream_->send(codec::encode_request_ok(std::move(parameters)))) {
    spdlog::warn("StreamSend failed for REQUEST_OK on request {}", request_id_);
  }
}

// A rejected REQUEST_UPDATE terminates the subscription: REQUEST_ERROR
// followed by PUBLISH_DONE(UPDATE_FAILED) (Section 10.9.1).
void PublisherSubscriptionFSM::reject_update_and_finish(PublisherRequestOwner &owner, RequestErrorCode code,
                                                        const std::string &reason) {
  if (!stream_->send(codec::encode_request_error(code, reason))) {
    spdlog::warn("StreamSend failed for REQUEST_ERROR on request {}", request_id_);
  }
  phase_ = moq::SubscriptionPhase::UpdateFailed;
  finish(PublishDoneCode::UpdateFailed, reason);
}

void PublisherSubscriptionFSM::finish(PublishDoneCode code, const std::string &reason) {
  const auto owner = owner_.lock();
  if (!owner || phase_ == moq::SubscriptionPhase::Terminated) {
    return;
  }
  // PUBLISH_DONE must follow the closure of every data stream (Section 10.11).
  const uint64_t stream_count = owner->send_plane().finish_subscription(request_id_);
  if (!stream_->send(codec::encode_publish_done(static_cast<uint64_t>(code), stream_count, reason), true)) {
    spdlog::warn("StreamSend failed for PUBLISH_DONE on request {}", request_id_);
  }
  stream_->abort_receive(static_cast<uint64_t>(StreamResetCode::Cancelled));
  phase_ = moq::SubscriptionPhase::Terminated;
  owner->subscription_closed(request_id_);
}

void PublisherSubscriptionFSM::cancel(uint64_t reset_error_code) {
  const auto owner = owner_.lock();
  if (!owner || phase_ == moq::SubscriptionPhase::Terminated) {
    return;
  }
  owner->send_plane().reset_subscription(request_id_, reset_error_code);
  stream_->abort_send(reset_error_code);
  stream_->abort_receive(reset_error_code);
  phase_ = moq::SubscriptionPhase::Terminated;
  owner->subscription_closed(request_id_);
}

} // namespace moq::detail

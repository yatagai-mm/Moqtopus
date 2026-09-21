#include "send_data_plane.h"

#include <limits>
#include <spdlog/spdlog.h>
#include <utility>

namespace moq::detail {
namespace {

bool location_less(const Location &left, const Location &right) {
  return left.group < right.group || (left.group == right.group && left.object < right.object);
}

bool valid_publish_status(const PublishedObject &object) {
  if (!object.status) {
    return true;
  }
  if (*object.status != codec::kObjectStatusNormal && *object.status != codec::kObjectStatusEndOfGroup &&
      *object.status != codec::kObjectStatusEndOfTrack) {
    return false;
  }
  return *object.status == codec::kObjectStatusNormal || object.payload.empty();
}

bool nonzero(const std::optional<uint64_t> &value) { return value && *value != 0; }

} // namespace

SendDataPlane::SendDataPlane(Callbacks callbacks) : callbacks_(std::move(callbacks)) {}

std::string SendDataPlane::make_track_key(const TrackNamespace &track_namespace, const TrackName &track_name) {
  std::string key;
  for (const std::string &field : track_namespace) {
    key += std::to_string(field.size());
    key += ':';
    key += field;
  }
  key += '/';
  key += track_name;
  return key;
}

bool SendDataPlane::register_track(PublishedTrack track) {
  const std::string key = make_track_key(track.track_namespace, track.track_name);
  return tracks_.emplace(key, TrackEntry{std::move(track), std::nullopt, {}}).second;
}

bool SendDataPlane::unregister_track(const TrackNamespace &track_namespace, const TrackName &track_name) {
  const auto entry = tracks_.find(make_track_key(track_namespace, track_name));
  if (entry == tracks_.end()) {
    return false;
  }
  if (!entry->second.subscriptions.empty()) {
    // The session finishes subscriptions before unregistering; reset defensively.
    const std::vector<RequestId> request_ids(entry->second.subscriptions.begin(), entry->second.subscriptions.end());
    for (const RequestId request_id : request_ids) {
      detach_subscription(request_id, true, static_cast<uint64_t>(StreamResetCode::Cancelled));
    }
  }
  tracks_.erase(entry);
  return true;
}

const PublishedTrack *SendDataPlane::find_track(const TrackNamespace &track_namespace,
                                                const TrackName &track_name) const {
  const auto entry = tracks_.find(make_track_key(track_namespace, track_name));
  return entry == tracks_.end() ? nullptr : &entry->second.track;
}

bool SendDataPlane::has_track_in_namespace(const TrackNamespace &track_namespace) const {
  for (const auto &entry : tracks_) {
    if (entry.second.track.track_namespace == track_namespace) {
      return true;
    }
  }
  return false;
}

std::vector<RequestId> SendDataPlane::subscriptions_for_track(const TrackNamespace &track_namespace,
                                                              const TrackName &track_name) const {
  const auto entry = tracks_.find(make_track_key(track_namespace, track_name));
  if (entry == tracks_.end()) {
    return {};
  }
  return {entry->second.subscriptions.begin(), entry->second.subscriptions.end()};
}

std::optional<Location> SendDataPlane::largest_location(const TrackNamespace &track_namespace,
                                                        const TrackName &track_name) const {
  const auto entry = tracks_.find(make_track_key(track_namespace, track_name));
  return entry == tracks_.end() ? std::nullopt : entry->second.largest;
}

void SendDataPlane::resolve_filter(const codec::SubscriptionFilter &filter, const std::optional<Location> &largest,
                                   Location &start, std::optional<GroupId> &end_group) const {
  start = Location{0, 0};
  end_group.reset();
  switch (filter.filter_type) {
  case codec::kFilterLargestObject:
    if (largest) {
      start = Location{largest->group, largest->object + 1};
    }
    break;
  case codec::kFilterNextGroupStart:
    if (largest) {
      start = Location{largest->group + 1, 0};
    }
    break;
  case codec::kFilterAbsoluteStart:
    start = filter.start;
    break;
  case codec::kFilterAbsoluteRange:
    start = filter.start;
    end_group = filter.start.group + filter.end_group_delta; // overflow rejected during decode
    break;
  default:
    break; // unreachable: decode rejects unknown filter types
  }
}

bool SendDataPlane::passes_filter(const SubscriptionSend &subscription, GroupId group_id, ObjectId object_id) const {
  const Location location{group_id, object_id};
  if (location_less(location, subscription.start)) {
    return false;
  }
  return !subscription.end_group || group_id <= *subscription.end_group;
}

SubscriptionDecision SendDataPlane::attach_subscription(RequestId request_id, TrackAlias track_alias,
                                                        const TrackNamespace &track_namespace,
                                                        const TrackName &track_name,
                                                        const codec::SubscriptionOptions &options) {
  if (nonzero(options.subgroup_delivery_timeout) || nonzero(options.object_delivery_timeout)) {
    return SubscriptionDecision::reject(RequestErrorCode::NotSupported, "delivery timeouts are not supported");
  }
  const auto entry = tracks_.find(make_track_key(track_namespace, track_name));
  if (entry == tracks_.end()) {
    return SubscriptionDecision::reject(RequestErrorCode::DoesNotExist, "track is not registered");
  }
  if (subscriptions_.find(request_id) != subscriptions_.end()) {
    // TODO(moqt-draft-21): Draft-21 and later permit multiple subscriptions to a
    // single track when their Request IDs differ. DuplicateSubscription must not
    // be emitted solely because another Request ID already subscribes to this track.
    return SubscriptionDecision::reject(RequestErrorCode::DuplicateSubscription,
                                        "request ID already has an established subscription");
  }

  SubscriptionSend subscription;
  subscription.request_id = request_id;
  subscription.track_alias = track_alias;
  subscription.track_key = entry->first;
  subscription.forward = options.forward.value_or(1) != 0;
  subscription.subscriber_priority = options.subscriber_priority.value_or(128);
  subscription.group_order = options.group_order.value_or(entry->second.track.default_group_order);
  if (options.filter) {
    resolve_filter(*options.filter, entry->second.largest, subscription.start, subscription.end_group);
    if (subscription.end_group && entry->second.largest && entry->second.largest->group > *subscription.end_group) {
      return SubscriptionDecision::reject(RequestErrorCode::InvalidRange, "requested range was already published");
    }
  }
  if (subscription.forward) {
    subscription.joining_location = entry->second.largest;
  }

  entry->second.subscriptions.insert(request_id);
  subscriptions_.emplace(request_id, std::move(subscription));
  return SubscriptionDecision::accept();
}

SubscriptionDecision SendDataPlane::update_subscription(RequestId request_id,
                                                        const codec::SubscriptionOptions &options) {
  const auto found = subscriptions_.find(request_id);
  if (found == subscriptions_.end()) {
    return SubscriptionDecision::reject(RequestErrorCode::DoesNotExist, "subscription is not established");
  }
  if (nonzero(options.subgroup_delivery_timeout) || nonzero(options.object_delivery_timeout)) {
    return SubscriptionDecision::reject(RequestErrorCode::NotSupported, "delivery timeouts are not supported");
  }
  SubscriptionSend &subscription = found->second;
  const auto track = tracks_.find(subscription.track_key);
  const std::optional<Location> largest = track == tracks_.end() ? std::nullopt : track->second.largest;

  if (options.filter) {
    Location start;
    std::optional<GroupId> end_group;
    resolve_filter(*options.filter, largest, start, end_group);
    if (end_group && largest && largest->group > *end_group) {
      return SubscriptionDecision::reject(RequestErrorCode::InvalidRange, "requested range was already published");
    }
    subscription.start = start;
    subscription.end_group = end_group;
  }
  if (options.subscriber_priority) {
    subscription.subscriber_priority = *options.subscriber_priority;
  }
  if (options.forward) {
    const bool forward = *options.forward != 0;
    if (forward && !subscription.forward) {
      subscription.joining_location = largest; // Joining Location (Section 5.1)
    }
    subscription.forward = forward;
  }
  return SubscriptionDecision::accept();
}

uint64_t SendDataPlane::finish_subscription(RequestId request_id) { return detach_subscription(request_id, false, 0); }

uint64_t SendDataPlane::reset_subscription(RequestId request_id, uint64_t reset_error_code) {
  return detach_subscription(request_id, true, reset_error_code);
}

uint64_t SendDataPlane::detach_subscription(RequestId request_id, bool reset, uint64_t reset_error_code) {
  const auto found = subscriptions_.find(request_id);
  if (found == subscriptions_.end()) {
    return 0;
  }
  SubscriptionSend &subscription = found->second;
  for (auto &open : subscription.streams) {
    if (reset) {
      open.second.stream->abort_send(reset_error_code);
    } else {
      open.second.stream->finish_send();
    }
  }
  const uint64_t stream_count = subscription.stream_count;
  const auto track = tracks_.find(subscription.track_key);
  if (track != tracks_.end()) {
    track->second.subscriptions.erase(request_id);
  }
  subscriptions_.erase(found);
  return stream_count;
}

void SendDataPlane::publish(const PublishedObject &object) {
  const auto track_it = tracks_.find(make_track_key(object.track_namespace, object.track_name));
  if (track_it == tracks_.end()) {
    spdlog::warn("publish for unregistered track \"{}\" dropped", object.track_name);
    return;
  }
  if (!valid_publish_status(object) || object.delivery_kind == DeliveryKind::FetchStream) {
    spdlog::warn("published object {}/{} has an invalid status or delivery kind; dropped", object.group_id,
                 object.object_id);
    return;
  }
  TrackEntry &entry = track_it->second;
  const Location location{object.group_id, object.object_id};
  if (!entry.largest || location_less(*entry.largest, location)) {
    entry.largest = location;
  }

  if (entry.subscriptions.empty()) {
    return;
  }

  // Completion callbacks detach subscriptions, so iterate over a stable copy.
  const std::vector<RequestId> request_ids(entry.subscriptions.begin(), entry.subscriptions.end());
  for (const RequestId request_id : request_ids) {
    const auto subscription_it = subscriptions_.find(request_id);
    if (subscription_it == subscriptions_.end()) {
      entry.subscriptions.erase(request_id);
      continue;
    }
    SubscriptionSend &subscription = subscription_it->second;

    std::optional<PublishDoneCode> complete;
    std::string complete_reason;
    if (subscription.end_group && object.group_id > *subscription.end_group) {
      complete = PublishDoneCode::SubscriptionEnded;
      complete_reason = "end of subscription range";
    } else {
      if (subscription.forward && passes_filter(subscription, object.group_id, object.object_id)) {
        if (object.delivery_kind == DeliveryKind::Datagram) {
          send_datagram_object(subscription, object);
        } else {
          send_on_subgroup_stream(subscription, object);
        }
      }
      if (object.status && *object.status == codec::kObjectStatusEndOfTrack) {
        complete = PublishDoneCode::TrackEnded;
        complete_reason = "end of track";
      } else if (subscription.end_group && object.group_id == *subscription.end_group &&
                 (object.end_of_group || (object.status && *object.status == codec::kObjectStatusEndOfGroup))) {
        complete = PublishDoneCode::SubscriptionEnded;
        complete_reason = "end of subscription range";
      }
    }
    if (complete) {
      callbacks_.subscription_complete(subscription.request_id, *complete, std::move(complete_reason));
    }
  }
}

void SendDataPlane::send_on_subgroup_stream(SubscriptionSend &subscription, const PublishedObject &object) {
  const SubgroupId subgroup_id = object.subgroup_id.value_or(0);
  const std::pair<GroupId, SubgroupId> key{object.group_id, subgroup_id};

  auto open_it = subscription.streams.find(key);
  if (open_it != subscription.streams.end() && open_it->second.last_object_id &&
      object.object_id <= *open_it->second.last_object_id) {
    // Cannot append out-of-order Object IDs to a subgroup stream; replace it.
    spdlog::warn("object {} does not follow {} on subgroup stream; resetting", object.object_id,
                 *open_it->second.last_object_id);
    open_it->second.stream->abort_send(static_cast<uint64_t>(StreamResetCode::InternalError));
    subscription.streams.erase(open_it);
    open_it = subscription.streams.end();
  }

  ByteBuffer bytes;
  if (open_it == subscription.streams.end()) {
    auto stream = callbacks_.open_data_stream();
    if (!stream) {
      return; // transport failure; the session is closing
    }
    ++subscription.stream_count;
    open_it = subscription.streams.emplace(key, OpenSubgroupStream{std::move(stream), std::nullopt}).first;
    codec::encode_subgroup_header(bytes, subscription.track_alias, object.group_id, subgroup_id,
                                  object.publisher_priority);
  }
  OpenSubgroupStream &open = open_it->second;

  const uint64_t delta = open.last_object_id ? object.object_id - *open.last_object_id - 1 : object.object_id;
  BytesView properties{object.properties};
  if (object.status && *object.status != codec::kObjectStatusNormal && !properties.empty()) {
    spdlog::warn("dropping properties of non-normal status object {}/{}", object.group_id, object.object_id);
    properties = BytesView{};
  }
  codec::encode_subgroup_object(bytes, delta, properties, object.status, BytesView{object.payload});
  open.last_object_id = object.object_id;

  bool close_stream =
      object.end_of_subgroup || object.end_of_group || (object.status && *object.status != codec::kObjectStatusNormal);
  if (object.end_of_group && (!object.status || *object.status == codec::kObjectStatusNormal)) {
    // Explicit EndOfGroup marker (at Object ID + 1) so the subscriber learns
    // the final Object ID; the plain FIN would only close the subgroup.
    codec::encode_subgroup_object(bytes, 0, BytesView{}, codec::kObjectStatusEndOfGroup, BytesView{});
    close_stream = true;
  }

  if (!open.stream->send(std::move(bytes), close_stream)) {
    spdlog::warn("StreamSend failed on subgroup stream for request {}", subscription.request_id);
  }
  if (close_stream) {
    subscription.streams.erase(key);
  }
}

void SendDataPlane::send_datagram_object(const SubscriptionSend &subscription, const PublishedObject &object) {
  std::optional<ObjectStatusCode> status = object.status;
  if (!status && object.payload.empty()) {
    status = codec::kObjectStatusNormal; // zero-length objects encode Normal explicitly
  }
  BytesView properties{object.properties};
  if (status && *status != codec::kObjectStatusNormal && !properties.empty()) {
    properties = BytesView{};
  }
  ByteBuffer bytes = codec::encode_object_datagram(subscription.track_alias, object.group_id, object.object_id,
                                                   object.publisher_priority, properties, status,
                                                   BytesView{object.payload}, object.end_of_group);
  if (!callbacks_.send_datagram(std::move(bytes))) {
    // Oversized or unsupported datagrams are dropped without notification (Section 11.3).
    SPDLOG_TRACE("datagram for request {} dropped", subscription.request_id);
  }
}

} // namespace moq::detail

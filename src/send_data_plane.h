#pragma once

#include "moq/codec.h"
#include "moq/errors.h"
#include "moq/publisher_session.h"
#include "stream_context.h"

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace moq::detail {

// Outcome of attaching/updating a subscription. A failed decision carries the
// REQUEST_ERROR code to send back; protocol violations are rejected earlier by
// the codec.
struct SubscriptionDecision {
  bool ok = true;
  RequestErrorCode code = RequestErrorCode::InternalError;
  std::string reason;

  static SubscriptionDecision accept() { return {}; }
  static SubscriptionDecision reject(RequestErrorCode code, std::string reason) {
    return SubscriptionDecision{false, code, std::move(reason)};
  }
};

// Send-side data plane. Owns the registered tracks, the per-subscription send
// state (Forward State, filter, priorities), the open subgroup streams, and
// the wire serialization of outgoing Objects. Not internally synchronized: the
// owning session serializes every call.
class SendDataPlane {
public:
  struct Callbacks {
    std::function<std::shared_ptr<StreamContext>()> open_data_stream;
    std::function<bool(ByteBuffer)> send_datagram;
    // The published object reached the end of the subscription's filter range
    // (or the track ended); the owner must finish it with PUBLISH_DONE.
    std::function<void(RequestId, PublishDoneCode, std::string)> subscription_complete;
  };

  explicit SendDataPlane(Callbacks callbacks);

  // ---- track registry ----
  bool register_track(PublishedTrack track);
  bool unregister_track(const TrackNamespace &track_namespace, const TrackName &track_name);
  const PublishedTrack *find_track(const TrackNamespace &track_namespace, const TrackName &track_name) const;
  bool has_track_in_namespace(const TrackNamespace &track_namespace) const;
  std::vector<RequestId> subscriptions_for_track(const TrackNamespace &track_namespace,
                                                 const TrackName &track_name) const;
  std::optional<Location> largest_location(const TrackNamespace &track_namespace, const TrackName &track_name) const;

  // ---- subscription lifecycle ----
  SubscriptionDecision attach_subscription(RequestId request_id, TrackAlias track_alias,
                                           const TrackNamespace &track_namespace, const TrackName &track_name,
                                           const codec::SubscriptionOptions &options);
  SubscriptionDecision update_subscription(RequestId request_id, const codec::SubscriptionOptions &options);
  // Local termination: FIN every open data stream; detaches and returns the
  // total number of data streams opened for the subscription.
  uint64_t finish_subscription(RequestId request_id);
  // Peer cancellation or session close: reset every open data stream.
  uint64_t reset_subscription(RequestId request_id, uint64_t reset_error_code);

  // ---- publishing ----
  void publish(const PublishedObject &object);

private:
  struct OpenSubgroupStream {
    std::shared_ptr<StreamContext> stream;
    std::optional<ObjectId> last_object_id;
  };

  struct SubscriptionSend {
    RequestId request_id = 0;
    TrackAlias track_alias = 0;
    std::string track_key;
    bool forward = true;
    uint8_t subscriber_priority = 128;
    uint8_t group_order = 1;
    Location start;
    std::optional<GroupId> end_group;
    std::optional<Location> joining_location;
    uint64_t stream_count = 0;
    std::map<std::pair<GroupId, SubgroupId>, OpenSubgroupStream> streams;
  };

  struct TrackEntry {
    PublishedTrack track;
    std::optional<Location> largest;
    std::unordered_set<RequestId> subscriptions;
  };

  static std::string make_track_key(const TrackNamespace &track_namespace, const TrackName &track_name);
  void resolve_filter(const codec::SubscriptionFilter &filter, const std::optional<Location> &largest, Location &start,
                      std::optional<GroupId> &end_group) const;
  bool passes_filter(const SubscriptionSend &subscription, GroupId group_id, ObjectId object_id) const;
  void send_on_subgroup_stream(SubscriptionSend &subscription, const PublishedObject &object);
  void send_datagram_object(const SubscriptionSend &subscription, const PublishedObject &object);
  uint64_t detach_subscription(RequestId request_id, bool reset, uint64_t reset_error_code);

  Callbacks callbacks_;
  std::unordered_map<std::string, TrackEntry> tracks_;
  std::unordered_map<RequestId, SubscriptionSend> subscriptions_;
};

} // namespace moq::detail

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

namespace moq {

// Send-side data plane. Owns the registered tracks, the per-subscription send
// state (Forward State and filter), the open subgroup streams, and
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

  struct TrackEntry {
    PublishedTrack track;
    std::optional<Location> largest;
    std::unordered_set<RequestId> subscriptions;
  };

  // ---- track registry ----
  bool register_track(PublishedTrack track);
  bool unregister_track(const TrackNamespace &track_namespace, const TrackName &track_name);
  const TrackEntry *find_track(const TrackNamespace &track_namespace, const TrackName &track_name) const;
  bool has_track_in_namespace(const TrackNamespace &track_namespace) const;
  // ---- subscription lifecycle ----
  std::optional<RequestError> attach_subscription(RequestId request_id, TrackAlias track_alias,
                                                  const TrackNamespace &track_namespace, const TrackName &track_name,
                                                  const SubscriptionOptions &options);
  std::optional<RequestError> update_subscription(RequestId request_id, const SubscriptionOptions &options);
  // FIN on local completion, RESET on cancellation; returns opened stream count.
  uint64_t detach_subscription(RequestId request_id, std::optional<uint64_t> reset_error = {});

  // ---- publishing ----
  void publish(const PublishedObject &object);

private:
  struct OpenSubgroupStream {
    std::shared_ptr<StreamContext> stream;
    std::optional<ObjectId> last_object_id;
  };

  using TrackKey = std::pair<TrackNamespace, TrackName>;
  struct SubscriptionSend {
    RequestId request_id = 0;
    TrackAlias track_alias = 0;
    TrackKey track_key;
    bool forward = true;
    Location start;
    std::optional<GroupId> end_group;
    uint64_t stream_count = 0;
    std::map<std::pair<GroupId, SubgroupId>, OpenSubgroupStream> streams;
  };

  void resolve_filter(const SubscriptionFilter &filter, const std::optional<Location> &largest, Location &start,
                      std::optional<GroupId> &end_group) const;
  bool passes_filter(const SubscriptionSend &subscription, GroupId group_id, ObjectId object_id) const;
  void send_on_subgroup_stream(SubscriptionSend &subscription, const PublishedObject &object);
  void send_datagram_object(const SubscriptionSend &subscription, const PublishedObject &object);

  Callbacks callbacks_;
  std::map<TrackKey, TrackEntry> tracks_;
  std::unordered_map<RequestId, SubscriptionSend> subscriptions_;
};

} // namespace moq

#pragma once

#include "moq/session.h"
#include "moq/types.h"

#include <future>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace moq {

struct PublisherConfig {
  size_t max_subscriptions = 128;
};

struct PublishedTrack {
  TrackNamespace track_namespace;
  TrackName track_name;
  ObjectProperties track_properties;
};

struct PublishedObject {
  TrackNamespace track_namespace;
  TrackName track_name;
  GroupId group_id = 0;
  SubgroupId subgroup_id = 0;
  ObjectId object_id = 0;
  uint8_t publisher_priority = 128;
  ObjectProperties properties;
  ByteBuffer payload;
  std::optional<ObjectStatusCode> status; // Normal / EndOfGroup / EndOfTrack
  DeliveryKind delivery_kind = DeliveryKind::SubgroupStream;
  bool end_of_subgroup = false; // FIN the subgroup stream after this object
  bool end_of_group = false;    // also announce that the group is complete
};

class SendDataPlane;

// Client-side MOQT publisher: connects to a relay, accepts peer SUBSCRIBEs for
// registered tracks and fans published objects out to established
// subscriptions. All calls are thread-safe; publish() runs synchronously.
class Publisher final : private Session {
public:
  ~Publisher() override;

  static std::unique_ptr<Publisher> connect(MsQuicClientConfig msquic_config, PublisherConfig publisher_config = {});

  using Session::close;
  using Session::ready;
  using Session::state;

  void register_track(PublishedTrack track);
  void unregister_track(const TrackNamespace &track_namespace, const TrackName &track_name);
  void publish(PublishedObject object);
  void end_track(const TrackNamespace &track_namespace, const TrackName &track_name,
                 PublishDoneCode code = PublishDoneCode::TrackEnded, std::string reason = {});

private:
  class PublisherSubscriptionFSM;
  Publisher(MsQuicClientConfig msquic_config, PublisherConfig publisher_config);
  void handle_data_stream(uint64_t type, const std::shared_ptr<StreamContext> &stream, ByteBuffer prefix,
                          bool fin) override;
  void handle_peer_request(const ControlMessage &message, const std::shared_ptr<StreamContext> &stream,
                           ByteBuffer leftover, bool fin) override;
  bool consume_peer_request_id(RequestId request_id, std::string &error);
  void on_ready() override;
  void announce_namespace(const TrackNamespace &track_namespace);
  void withdraw_namespace(const TrackNamespace &track_namespace);
  void accept_subscribe(const ByteBuffer &payload, const std::shared_ptr<StreamContext> &stream, ByteBuffer leftover,
                        bool fin);
  void complete_subscription(RequestId request_id, PublishDoneCode code, std::string reason);
  std::shared_ptr<StreamContext> open_data_stream();
  void terminate_subscriptions(const std::string &reason) override;
  size_t active_subscriptions() const override;

  PublisherConfig publisher_config_;
  std::unique_ptr<SendDataPlane> send_plane_;
  TrackAlias next_track_alias_ = 0;
  std::unordered_set<RequestId> peer_request_ids_;
  std::unordered_map<RequestId, std::shared_ptr<PublisherSubscriptionFSM>> subscriptions_;
  std::vector<TrackNamespace> pending_announcements_;
  std::unordered_map<std::string, std::shared_ptr<StreamContext>> announced_namespaces_;
};

} // namespace moq

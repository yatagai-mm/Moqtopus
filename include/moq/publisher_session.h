#pragma once

#include "moq/client_config.h"
#include "moq/errors.h"
#include "moq/types.h"

#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>

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

namespace detail {
class Publisher;
}

// Client-side MOQT publisher: connects to a relay, accepts peer SUBSCRIBEs for
// registered tracks and fans published objects out to established
// subscriptions. All calls are thread-safe; publish() runs synchronously.
class Publisher {
public:
  ~Publisher();

  Publisher(const Publisher &) = delete;
  Publisher &operator=(const Publisher &) = delete;

  static std::unique_ptr<Publisher> connect(MsQuicClientConfig msquic_config, PublisherConfig publisher_config = {});

  std::future<void> ready();
  SessionStateSnapshot state() const;

  void register_track(PublishedTrack track);
  void unregister_track(const TrackNamespace &track_namespace, const TrackName &track_name);
  void publish(PublishedObject object);
  void end_track(const TrackNamespace &track_namespace, const TrackName &track_name,
                 PublishDoneCode code = PublishDoneCode::TrackEnded, std::string reason = {});
  void close(SessionCloseErrorCode error = SessionCloseErrorCode::NoError);

private:
  explicit Publisher(std::shared_ptr<detail::Publisher> impl) : impl_(std::move(impl)) {}

  std::shared_ptr<detail::Publisher> impl_;
};

} // namespace moq

#pragma once

#include "moq/errors.h"
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace moq {

using ByteBuffer = std::vector<uint8_t>;
using RequestId = uint64_t;
using TrackAlias = uint64_t;
using GroupId = uint64_t;
using SubgroupId = uint64_t;
using ObjectId = uint64_t;
using ObjectStatusCode = uint64_t;
using TrackNamespace = std::vector<std::string>;
using TrackName = std::string;
using ObjectProperties = ByteBuffer;

// Non-owning view into transport-owned bytes. Views handed to callbacks are
// valid only for the duration of the call; copy with to_owned() to retain.
struct BytesView {
  const uint8_t *data = nullptr;
  size_t size = 0;

  BytesView() = default;
  BytesView(const uint8_t *data_in, size_t size_in) : data(data_in), size(size_in) {}
  BytesView(const ByteBuffer &owned) : data(owned.data()), size(owned.size()) {}

  const uint8_t *begin() const { return data; }
  const uint8_t *end() const { return size ? data + size : data; }
  bool empty() const { return size == 0; }
  ByteBuffer to_owned() const { return empty() ? ByteBuffer{} : ByteBuffer(begin(), end()); }
};

struct Location {
  GroupId group = 0;
  ObjectId object = 0;
};

struct Parameter {
  uint64_t type = 0;
  ByteBuffer encoded_value;

  static Parameter uint8(uint64_t type, uint8_t value);
  static Parameter varint(uint64_t type, uint64_t value);
  static Parameter location(uint64_t type, Location value);
  static Parameter length_prefixed(uint64_t type, ByteBuffer value);
  static Parameter track_namespace(uint64_t type, TrackNamespace value);
};

struct SubscribeRequest {
  TrackNamespace track_namespace;
  TrackName track_name;
  std::vector<Parameter> parameters;
};

struct RequestUpdate {
  std::vector<Parameter> parameters;
};

struct RequestOk {
  std::vector<Parameter> parameters;
  ObjectProperties track_properties;
};

struct RequestError {
  RequestErrorCode code = RequestErrorCode::InternalError;
  uint64_t retry_interval = 0;
  std::string reason;
};

struct PublishDone {
  uint64_t status_code = 0;
  uint64_t stream_count = 0;
  std::string reason;
};

enum class DeliveryKind {
  SubgroupStream,
  Datagram,
  FetchStream,
};

// Objects are delivered synchronously from the transport thread; properties
// and payload are views into receive buffers, valid only during on_object.
struct Object {
  RequestId request_id = 0;
  TrackAlias track_alias = 0;
  GroupId group_id = 0;
  std::optional<SubgroupId> subgroup_id;
  ObjectId object_id = 0;
  uint8_t publisher_priority = 128;
  std::optional<ObjectStatusCode> status;
  BytesView properties;
  BytesView payload;
  DeliveryKind delivery_kind = DeliveryKind::Datagram;
  uint64_t stream_id = 0;
};

enum class SessionPhase {
  Init,
  SetupInProgress,
  Ready,
  Closing,
  Closed,
};

enum class SubscriptionPhase {
  Pending,
  Established,
  UpdateFailed,
  Terminated,
};

struct SessionCloseReason {
  SessionCloseErrorCode code = SessionCloseErrorCode::InternalError;
  std::string message;
};

struct SessionStateSnapshot {
  SessionPhase phase = SessionPhase::Init;
  bool local_setup_sent = false;
  bool peer_setup_received = false;
  std::optional<std::string> negotiated_version;
  size_t active_subscriptions = 0;
  std::optional<SessionCloseReason> close_reason;
};

struct SubscriptionStateSnapshot {
  SubscriptionPhase phase = SubscriptionPhase::Terminated;
  RequestId request_id = 0;
  std::optional<TrackAlias> track_alias;
  size_t inflight_updates = 0;
};

} // namespace moq

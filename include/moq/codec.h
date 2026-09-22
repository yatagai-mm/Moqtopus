#pragma once

#include "moq/types.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace moq {

constexpr uint64_t kSetupStreamType = 0x2f00;
constexpr uint64_t kFetchStreamType = 0x05;
constexpr uint64_t kPaddingStreamType = 0x132b3e28;
constexpr uint64_t kPaddingDatagramType = 0x132b3e29;

constexpr uint64_t kMessageRequestUpdate = 0x02;
constexpr uint64_t kMessageSubscribe = 0x03;
constexpr uint64_t kMessageSubscribeOk = 0x04;
constexpr uint64_t kMessageRequestError = 0x05;
constexpr uint64_t kMessageRequestOk = 0x07;
constexpr uint64_t kMessagePublishDone = 0x0b;
constexpr uint64_t kMessageGoAway = 0x10;
constexpr uint64_t kMessagePublish = 0x1d;
constexpr uint64_t kMessageSetup = 0x2f00;
constexpr uint64_t kMessagePublishNamespace = 0x06;
constexpr uint64_t kMessageTrackStatus = 0x0d;
constexpr uint64_t kMessageFetch = 0x16;
constexpr uint64_t kMessageSubscribeNamespace = 0x50;
constexpr uint64_t kMessageSubscribeTracks = 0x51;

// Message parameter identifiers used by the publisher
constexpr uint64_t kParameterObjectDeliveryTimeout = 0x02;
constexpr uint64_t kParameterSubgroupDeliveryTimeout = 0x06;
constexpr uint64_t kParameterLargestObject = 0x09;
constexpr uint64_t kParameterForward = 0x10;
constexpr uint64_t kParameterSubscriberPriority = 0x20;
constexpr uint64_t kParameterSubscriptionFilter = 0x21;
constexpr uint64_t kParameterGroupOrder = 0x22;
constexpr uint64_t kParameterNewGroupRequest = 0x32;

// Subscription filter types (Section 5.1.2)
constexpr uint64_t kFilterNextGroupStart = 0x1;
constexpr uint64_t kFilterLargestObject = 0x2;
constexpr uint64_t kFilterAbsoluteStart = 0x3;
constexpr uint64_t kFilterAbsoluteRange = 0x4;

// Object status codes (Section 11.2.1.1)
constexpr uint64_t kObjectStatusNormal = 0x0;
constexpr uint64_t kObjectStatusEndOfGroup = 0x3;
constexpr uint64_t kObjectStatusEndOfTrack = 0x4;

// Setup option identifiers
enum class SetupOption : uint64_t {
  Path = 0x01,
  Authority = 0x05,
  MoqtImplementation = 0x07,
};

enum class DecodeStatus {
  Done,
  NeedMoreData,
  Error,
};

struct VarintResult {
  DecodeStatus status = DecodeStatus::NeedMoreData;
  uint64_t value = 0;
  size_t bytes = 0;
};

struct ControlMessage {
  uint64_t type = 0;
  ByteBuffer payload;
};

struct ControlMessageResult {
  DecodeStatus status = DecodeStatus::NeedMoreData;
  ControlMessage message;
  size_t bytes = 0;
};

struct SubscribeOk {
  TrackAlias track_alias = 0;
  std::vector<Parameter> parameters;
  ObjectProperties track_properties;
};

struct Subscribe {
  RequestId request_id = 0;
  TrackNamespace track_namespace;
  TrackName track_name;
  std::vector<Parameter> parameters;
};

struct RequestUpdateMessage {
  RequestId request_id = 0;
  std::vector<Parameter> parameters;
};

struct SubscriptionFilter {
  uint64_t filter_type = kFilterLargestObject;
  Location start;               // AbsoluteStart / AbsoluteRange only
  uint64_t end_group_delta = 0; // AbsoluteRange only
};

// Typed view of the subscription-scoped parameters carried in SUBSCRIBE and
// REQUEST_UPDATE. An absent optional means the parameter was not present.
struct SubscriptionOptions {
  std::optional<uint8_t> forward;
  std::optional<SubscriptionFilter> filter;
  std::optional<uint64_t> subgroup_delivery_timeout;
  std::optional<uint64_t> object_delivery_timeout;
};

void write_varint(ByteBuffer &out, uint64_t value);
VarintResult read_varint(const uint8_t *data, size_t size, size_t offset = 0);
inline VarintResult read_varint(const ByteBuffer &bytes, size_t offset = 0) {
  return read_varint(bytes.data(), bytes.size(), offset);
}
ByteBuffer encode_control_message(uint64_t type, const ByteBuffer &payload);
ControlMessageResult read_control_message(const ByteBuffer &bytes, size_t offset = 0);

ByteBuffer encode_setup(std::string authority, std::string path);
// The subscriber ignores peer SETUP contents; decoding only validates the encoding.
bool decode_setup(const ByteBuffer &payload, std::string &error);
ByteBuffer encode_subscribe(RequestId request_id, const SubscribeRequest &request);
ByteBuffer encode_request_update(RequestId request_id, const RequestUpdate &update);
ByteBuffer encode_request_error(RequestErrorCode code, std::string reason, uint64_t retry_interval = 0);

std::optional<SubscribeOk> decode_subscribe_ok(const ByteBuffer &payload, std::string &error);
std::optional<RequestOk> decode_request_ok(const ByteBuffer &payload, std::string &error);
std::optional<RequestError> decode_request_error(const ByteBuffer &payload, std::string &error);
std::optional<PublishDone> decode_publish_done(const ByteBuffer &payload, std::string &error);

// Publisher-side control message codecs
std::optional<Subscribe> decode_subscribe(const ByteBuffer &payload, std::string &error);
std::optional<RequestUpdateMessage> decode_request_update(const ByteBuffer &payload, std::string &error);
// Validates parameter values; a false return is a protocol violation (session close).
bool decode_subscription_options(const std::vector<Parameter> &parameters, SubscriptionOptions &options,
                                 std::string &error);
ByteBuffer encode_subscribe_ok(TrackAlias track_alias, std::vector<Parameter> parameters,
                               const ObjectProperties &track_properties);
ByteBuffer encode_request_ok(std::vector<Parameter> parameters);
ByteBuffer encode_publish_done(uint64_t status_code, uint64_t stream_count, const std::string &reason);
ByteBuffer encode_publish_namespace(RequestId request_id, const TrackNamespace &track_namespace);

// Publisher-side data plane serialization
ByteBuffer encode_object_datagram(TrackAlias track_alias, GroupId group_id, ObjectId object_id, uint8_t priority,
                                  BytesView properties, const std::optional<ObjectStatusCode> &status,
                                  BytesView payload, bool end_of_group);
void encode_subgroup_header(ByteBuffer &out, TrackAlias track_alias, GroupId group_id, SubgroupId subgroup_id,
                            uint8_t priority);
void encode_subgroup_object(ByteBuffer &out, uint64_t object_id_delta, BytesView properties,
                            const std::optional<ObjectStatusCode> &status, BytesView payload);

void write_track_namespace(ByteBuffer &out, const TrackNamespace &name_space);
bool is_subgroup_stream_type(uint64_t type);

} // namespace moq

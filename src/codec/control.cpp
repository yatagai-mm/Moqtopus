#include "codec_internal.h"

namespace moq {

ByteBuffer encode_setup(std::string authority, std::string path) {
  ByteBuffer payload;
  uint64_t previous = 0;
  for (const auto &option : {std::pair{SetupOption::Path, path},
                             {SetupOption::Authority, authority},
                             {SetupOption::MoqtImplementation, std::string("kota-moqtopus")}}) {
    if (option.second.empty())
      continue;
    const auto type = static_cast<uint64_t>(option.first);
    write_varint(payload, type - previous);
    write_varint(payload, option.second.size());
    payload.insert(payload.end(), option.second.begin(), option.second.end());
    previous = type;
  }
  return encode_control_message(kMessageSetup, payload);
}

bool decode_setup(const ByteBuffer &payload, std::string &error) {
  return decode<bool>(payload, error, [](Reader &reader, bool &) {
    uint64_t type = 0;
    while (reader.remaining()) {
      const auto delta = reader.varint();
      Reader::require(delta <= std::numeric_limits<uint64_t>::max() - type, "SETUP option type overflow");
      type += delta;
      if (type & 1)
        reader.string(65535);
      else
        reader.varint();
    }
  })
      .has_value();
}

ByteBuffer encode_subscribe(RequestId request_id, const SubscribeRequest &request) {
  ByteBuffer payload;
  write_varint(payload, request_id);
  write_track_namespace(payload, request.track_namespace);
  write_varint(payload, request.track_name.size());
  payload.insert(payload.end(), request.track_name.begin(), request.track_name.end());
  encode_parameters(payload, request.parameters);

  return encode_control_message(kMessageSubscribe, payload);
}

std::optional<Subscribe> decode_subscribe(const ByteBuffer &payload, std::string &error) {
  return decode<Subscribe>(payload, error, [](Reader &reader, Subscribe &value) {
    value.request_id = reader.varint();
    value.track_namespace = reader.track_namespace();
    value.track_name = reader.string(4096);
    reader.parameters(value.parameters);
  });
}

bool decode_subscription_options(const std::vector<Parameter> &parameters, SubscriptionOptions &options,
                                 std::string &error) {
  for (const auto &parameter : parameters) {
    const auto parsed = decode<bool>(parameter.encoded_value, error, [&](Reader &reader, bool &) {
      switch (parameter.type) {
      case kParameterForward:
        options.forward = reader.byte();
        Reader::require(*options.forward <= 1, "invalid FORWARD parameter");
        break;
      case kParameterGroupOrder: {
        const auto order = reader.byte();
        Reader::require(order == 1 || order == 2, "invalid GROUP_ORDER parameter");
        break;
      }
      case kParameterSubscriptionFilter: {
        const auto size = reader.varint();
        Reader::require(size == reader.remaining(), "invalid SUBSCRIPTION_FILTER length");
        SubscriptionFilter filter;
        filter.filter_type = reader.varint();
        Reader::require(filter.filter_type >= kFilterNextGroupStart && filter.filter_type <= kFilterAbsoluteRange,
                        "unknown subscription filter type");
        if (filter.filter_type >= kFilterAbsoluteStart)
          filter.start = {reader.varint(), reader.varint()};
        if (filter.filter_type == kFilterAbsoluteRange)
          filter.end_group_delta = reader.varint();
        Reader::require(filter.end_group_delta <= std::numeric_limits<uint64_t>::max() - filter.start.group,
                        "SUBSCRIPTION_FILTER End Group overflows");
        options.filter = filter;
        break;
      }
      case kParameterSubgroupDeliveryTimeout:
        options.subgroup_delivery_timeout = reader.varint();
        break;
      case kParameterObjectDeliveryTimeout:
        options.object_delivery_timeout = reader.varint();
        break;
      default:
        reader.view(reader.remaining());
        break; // already validated by the message decoder
      }
    });
    if (!parsed)
      return false;
  }
  return true;
}

std::optional<SubscribeOk> decode_subscribe_ok(const ByteBuffer &payload, std::string &error) {
  return decode<SubscribeOk>(payload, error, [](Reader &reader, SubscribeOk &value) {
    value.track_alias = reader.varint();
    reader.parameters(value.parameters);
    value.track_properties = reader.view(reader.remaining()).to_owned();
  });
}

ByteBuffer encode_subscribe_ok(TrackAlias track_alias, std::vector<Parameter> parameters,
                               const ObjectProperties &track_properties) {
  ByteBuffer payload;
  write_varint(payload, track_alias);
  encode_parameters(payload, std::move(parameters));
  payload.insert(payload.end(), track_properties.begin(), track_properties.end());
  return encode_control_message(kMessageSubscribeOk, payload);
}

ByteBuffer encode_request_update(RequestId request_id, const RequestUpdate &update) {
  ByteBuffer payload;
  write_varint(payload, request_id);
  encode_parameters(payload, update.parameters);
  return encode_control_message(kMessageRequestUpdate, payload);
}

std::optional<RequestUpdateMessage> decode_request_update(const ByteBuffer &payload, std::string &error) {
  return decode<RequestUpdateMessage>(payload, error, [](Reader &reader, RequestUpdateMessage &value) {
    value.request_id = reader.varint();
    reader.parameters(value.parameters);
  });
}

std::optional<RequestOk> decode_request_ok(const ByteBuffer &payload, std::string &error) {
  return decode<RequestOk>(payload, error, [](Reader &reader, RequestOk &value) {
    reader.parameters(value.parameters);
    value.track_properties = reader.view(reader.remaining()).to_owned();
  });
}

ByteBuffer encode_request_ok(std::vector<Parameter> parameters) {
  ByteBuffer payload;
  encode_parameters(payload, std::move(parameters));
  return encode_control_message(kMessageRequestOk, payload);
}

ByteBuffer encode_request_error(RequestErrorCode code, std::string reason, uint64_t retry_interval) {
  ByteBuffer payload;
  write_varint(payload, static_cast<uint64_t>(code));
  write_varint(payload, retry_interval);
  write_varint(payload, reason.size());
  payload.insert(payload.end(), reason.begin(), reason.end());
  return encode_control_message(kMessageRequestError, payload);
}

std::optional<RequestError> decode_request_error(const ByteBuffer &payload, std::string &error) {
  return decode<RequestError>(payload, error, [](Reader &reader, RequestError &value) {
    value.code = static_cast<RequestErrorCode>(reader.varint());
    value.retry_interval = reader.varint();
    value.reason = reader.string(1024);
    // Redirect-specific fields remain opaque to this client.
    if (value.code == RequestErrorCode::Redirect)
      reader.view(reader.remaining());
  });
}

std::optional<PublishDone> decode_publish_done(const ByteBuffer &payload, std::string &error) {
  return decode<PublishDone>(payload, error, [](Reader &reader, PublishDone &value) {
    value.status_code = reader.varint();
    value.stream_count = reader.varint();
    value.reason = reader.string(1024);
  });
}

ByteBuffer encode_publish_done(uint64_t status_code, uint64_t stream_count, const std::string &reason) {
  ByteBuffer payload;
  write_varint(payload, status_code);
  write_varint(payload, stream_count);
  write_varint(payload, reason.size());
  payload.insert(payload.end(), reason.begin(), reason.end());
  return encode_control_message(kMessagePublishDone, payload);
}

ByteBuffer encode_publish_namespace(RequestId request_id, const TrackNamespace &track_namespace) {
  ByteBuffer payload;
  write_varint(payload, request_id);
  write_track_namespace(payload, track_namespace);
  write_varint(payload, 0); // no announcement parameters
  return encode_control_message(kMessagePublishNamespace, payload);
}

} // namespace moq

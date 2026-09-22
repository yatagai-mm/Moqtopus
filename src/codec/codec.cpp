#include "codec_internal.h"

#include "moq/errors.h"

#include <limits>
#include <stdexcept>
#include <utility>

namespace moq {

Parameter Parameter::uint8(uint64_t type, uint8_t value) { return Parameter{type, ByteBuffer{value}}; }

Parameter Parameter::varint(uint64_t type, uint64_t value) {
  Parameter parameter{type, {}};
  codec::write_varint(parameter.encoded_value, value);
  return parameter;
}

Parameter Parameter::location(uint64_t type, Location value) {
  Parameter parameter{type, {}};
  codec::write_varint(parameter.encoded_value, value.group);
  codec::write_varint(parameter.encoded_value, value.object);
  return parameter;
}

Parameter Parameter::length_prefixed(uint64_t type, ByteBuffer value) {
  Parameter parameter{type, {}};
  codec::write_varint(parameter.encoded_value, value.size());
  parameter.encoded_value.insert(parameter.encoded_value.end(), value.begin(), value.end());
  return parameter;
}

Parameter Parameter::track_namespace(uint64_t type, TrackNamespace value) {
  Parameter parameter{type, {}};
  codec::write_track_namespace(parameter.encoded_value, value);
  return parameter;
}

RequestRejected::RequestRejected(RequestErrorCode code, uint64_t retry_interval, std::string reason)
    : std::runtime_error("MOQT request rejected: code=" + std::to_string(static_cast<uint64_t>(code)) +
                         (reason.empty() ? "" : " reason=" + reason)),
      code_(code), retry_interval_(retry_interval), reason_(std::move(reason)) {}

} // namespace moq

namespace moq::codec {
void write_varint(ByteBuffer &out, uint64_t value) {
  size_t length = 1;
  for (uint64_t max = 0x7f; length < 9 && value > max; max = (max << 7) | 0x7f)
    ++length;
  // Lengths 1..8 have a unary prefix; length 9 uses 0xff and all 64 value bits.
  const auto prefix = static_cast<uint8_t>(0xff << (9 - length));
  out.push_back(length == 9 ? 0xff : static_cast<uint8_t>(prefix | (value >> ((length - 1) * 8))));
  for (int shift = (static_cast<int>(length) - 2) * 8; shift >= 0; shift -= 8) {
    out.push_back(static_cast<uint8_t>(value >> shift));
  }
}

VarintResult read_varint(const uint8_t *data, size_t size, size_t offset) {
  if (offset >= size)
    return {};
  const uint8_t first = data[offset];
  size_t length = 1;
  while (length < 9 && (first & (0x100 >> length)))
    ++length;
  if (length > size - offset)
    return {};
  uint64_t value = length == 9 ? 0 : first & (0xff >> length);
  for (size_t index = 1; index < length; ++index)
    value = (value << 8) | data[offset + index];
  return {DecodeStatus::Done, value, length};
}

ByteBuffer encode_control_message(uint64_t type, const ByteBuffer &payload) {
  ByteBuffer out;
  if (payload.size() > std::numeric_limits<uint16_t>::max()) {
    throw std::length_error("MOQT control message payload exceeds 65535 bytes");
  }
  write_varint(out, type);
  out.push_back(static_cast<uint8_t>(payload.size() >> 8));
  out.push_back(static_cast<uint8_t>(payload.size()));
  out.insert(out.end(), payload.begin(), payload.end());
  return out;
}

ControlMessageResult read_control_message(const ByteBuffer &bytes, size_t offset) {
  const VarintResult type = read_varint(bytes, offset);
  if (type.status != DecodeStatus::Done || bytes.size() - offset < type.bytes + 2) {
    return {};
  }
  const size_t length_offset = offset + type.bytes;
  const uint16_t length =
      static_cast<uint16_t>((static_cast<uint16_t>(bytes[length_offset]) << 8) | bytes[length_offset + 1]);
  const size_t frame_size = type.bytes + 2 + length;
  if (bytes.size() - offset < frame_size) {
    return {};
  }

  ControlMessage message;
  message.type = type.value;
  message.payload.assign(bytes.begin() + (length_offset + 2), bytes.begin() + (length_offset + 2 + length));
  return ControlMessageResult{DecodeStatus::Done, std::move(message), frame_size};
}

void write_track_namespace(ByteBuffer &out, const TrackNamespace &name_space) {
  if (name_space.size() > 32) {
    throw std::invalid_argument("MOQT track namespace has more than 32 fields");
  }
  write_varint(out, name_space.size());
  size_t total_size = 0;
  for (const std::string &field : name_space) {
    if (field.empty() || total_size + field.size() > 4096) {
      throw std::invalid_argument("invalid MOQT track namespace field");
    }
    write_varint(out, field.size());
    out.insert(out.end(), field.begin(), field.end());
    total_size += field.size();
  }
}

bool is_subgroup_stream_type(uint64_t type) {
  if ((type & 0x10) == 0 || type > 0x7f) {
    return false;
  }
  return ((type & 0x06) >> 1) != 0x03;
}

ByteBuffer encode_object_datagram(TrackAlias track_alias, GroupId group_id, ObjectId object_id, uint8_t priority,
                                  BytesView properties, const std::optional<ObjectStatusCode> &status,
                                  BytesView payload, bool end_of_group) {
  uint64_t type = 0;
  if (!properties.empty()) {
    type |= 0x01;
  }
  if (status) {
    type |= 0x20;
  } else if (end_of_group) {
    type |= 0x02;
  }
  if (object_id == 0) {
    type |= 0x04;
  }

  ByteBuffer out;
  write_varint(out, type);
  write_varint(out, track_alias);
  write_varint(out, group_id);
  if (object_id != 0) {
    write_varint(out, object_id);
  }
  out.push_back(priority);
  if (!properties.empty()) {
    write_varint(out, properties.size);
    out.insert(out.end(), properties.begin(), properties.end());
  }
  if (status) {
    write_varint(out, *status);
  } else {
    out.insert(out.end(), payload.begin(), payload.end());
  }
  return out;
}

void encode_subgroup_header(ByteBuffer &out, TrackAlias track_alias, GroupId group_id, SubgroupId subgroup_id,
                            uint8_t priority) {
  const uint64_t subgroup_id_mode = subgroup_id == 0 ? 0x0 : 0x2;
  const uint64_t type = 0x10 | 0x01 | (subgroup_id_mode << 1);
  write_varint(out, type);
  write_varint(out, track_alias);
  write_varint(out, group_id);
  if (subgroup_id != 0) {
    write_varint(out, subgroup_id);
  }
  out.push_back(priority);
}

void encode_subgroup_object(ByteBuffer &out, uint64_t object_id_delta, BytesView properties,
                            const std::optional<ObjectStatusCode> &status, BytesView payload) {
  write_varint(out, object_id_delta);
  write_varint(out, properties.size);
  out.insert(out.end(), properties.begin(), properties.end());
  if (status || payload.empty()) {
    write_varint(out, 0);
    write_varint(out, status.value_or(kObjectStatusNormal));
  } else {
    write_varint(out, payload.size);
    out.insert(out.end(), payload.begin(), payload.end());
  }
}

} // namespace moq::codec

#include "codec_internal.h"

namespace moq {

static ByteBuffer encode_values(const TrackPropertyValues &values, const ByteBuffer *immutable = nullptr) {
  ByteBuffer bytes;
  uint64_t previous = 0;
  const auto emit = [&](uint64_t type, const auto &value) {
    if (value) {
      write_varint(bytes, type - previous);
      write_varint(bytes, static_cast<uint64_t>(*value));
      previous = type;
    }
  };
  if (values.default_publisher_group_order &&
      *values.default_publisher_group_order != GroupOrder::Ascending &&
      *values.default_publisher_group_order != GroupOrder::Descending) {
    throw std::invalid_argument("invalid DEFAULT_PUBLISHER_GROUP_ORDER property");
  }
  emit(0x02, values.object_delivery_timeout);
  emit(0x04, values.max_cache_duration);
  emit(0x06, values.subgroup_delivery_timeout);
  if (immutable) {
    write_varint(bytes, 0x0b - previous);
    write_varint(bytes, immutable->size());
    bytes.insert(bytes.end(), immutable->begin(), immutable->end());
    previous = 0x0b;
  }
  emit(0x0e, values.default_publisher_priority);
  emit(0x22, values.default_publisher_group_order);
  emit(0x30, values.dynamic_groups);
  return bytes;
}

ByteBuffer encode_track_properties(const TrackProperties &properties) {
  if (!properties.immutable_properties) {
    return encode_values(properties);
  }
  const TrackPropertyValues &immutable = *properties.immutable_properties;
  if ((properties.object_delivery_timeout && immutable.object_delivery_timeout) ||
      (properties.max_cache_duration && immutable.max_cache_duration) ||
      (properties.subgroup_delivery_timeout && immutable.subgroup_delivery_timeout) ||
      (properties.default_publisher_priority && immutable.default_publisher_priority) ||
      (properties.default_publisher_group_order && immutable.default_publisher_group_order) ||
      (properties.dynamic_groups && immutable.dynamic_groups)) {
    throw std::invalid_argument("duplicate mutable and immutable Track Property");
  }
  const ByteBuffer encoded = encode_values(immutable);
  return encode_values(properties, &encoded);
}

} // namespace moq

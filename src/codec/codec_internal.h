#pragma once

#include "moq/codec.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace moq::codec {

struct Cursor {
  BytesView bytes;
  size_t offset = 0;

  size_t remaining() const { return bytes.size - offset; }
  bool read_byte(uint8_t &value) { return offset < bytes.size && (value = bytes.data[offset++], true); }
  bool read_varint(uint64_t &value) {
    const auto parsed = codec::read_varint(bytes.data, bytes.size, offset);
    if (parsed.status != DecodeStatus::Done)
      return false;
    offset += parsed.bytes;
    value = parsed.value;
    return true;
  }
  bool read_view(uint64_t size, BytesView &value) {
    if (size > remaining())
      return false;
    value = {bytes.data ? bytes.data + offset : nullptr, static_cast<size_t>(size)};
    offset += size;
    return true;
  }
  bool read_string(size_t size, std::string &value) {
    BytesView view;
    if (!read_view(size, view))
      return false;
    value.assign(view.empty() ? "" : reinterpret_cast<const char *>(view.data), view.size);
    return true;
  }
};

enum class ParameterEncoding {
  Uint8,
  Varint,
  Location,
  LengthPrefixed,
  TrackNamespace,
};

inline std::optional<ParameterEncoding> parameter_encoding(uint64_t type) {
  switch (type) {
  case 0x02:
  case 0x04:
  case 0x06:
  case 0x08:
  case 0x0a:
  case 0x32:
    return ParameterEncoding::Varint;
  case 0x03:
  case 0x21:
    return ParameterEncoding::LengthPrefixed;
  case 0x09:
    return ParameterEncoding::Location;
  case 0x10:
  case 0x20:
  case 0x22:
    return ParameterEncoding::Uint8;
  case 0x34:
    return ParameterEncoding::TrackNamespace;
  default:
    return std::nullopt;
  }
}

// Control messages are already framed: a missing field is malformed, never
// "need more data". Keep this distinction out of each individual decoder.
struct Reader : Cursor {
  static void require(bool valid, const char *message) {
    if (!valid)
      throw std::invalid_argument(message);
  }
  uint64_t varint() {
    uint64_t value = 0;
    require(read_varint(value), "truncated varint");
    return value;
  }
  uint8_t byte() {
    uint8_t value = 0;
    require(read_byte(value), "truncated byte");
    return value;
  }
  BytesView view(uint64_t length) {
    BytesView value;
    require(read_view(length, value), "truncated bytes");
    return value;
  }
  std::string string(size_t limit) {
    const auto size = varint();
    require(size <= limit, "string exceeds length limit");
    std::string value;
    require(read_string(size, value), "truncated string");
    return value;
  }
  TrackNamespace track_namespace() {
    const auto count = varint();
    require(count <= 32, "too many namespace fields");
    TrackNamespace fields;
    size_t total = 0;
    for (uint64_t i = 0; i < count; ++i) {
      auto field = string(4096);
      total += field.size();
      require(!field.empty() && total <= 4096, "invalid namespace field");
      fields.push_back(std::move(field));
    }
    return fields;
  }
  void parameters(std::vector<Parameter> &out) {
    const auto count = varint();
    uint64_t type = 0;
    for (uint64_t i = 0; i < count; ++i) {
      const auto delta = varint();
      require(delta <= std::numeric_limits<uint64_t>::max() - type && (i == 0 || delta != 0),
              "invalid message parameter type delta");
      type += delta;
      const auto encoding = parameter_encoding(type);
      require(encoding.has_value(), "unknown message parameter");
      const auto start = offset;
      switch (*encoding) {
      case ParameterEncoding::Uint8:
        byte();
        break;
      case ParameterEncoding::Varint:
        varint();
        break;
      case ParameterEncoding::Location:
        varint();
        varint();
        break;
      case ParameterEncoding::LengthPrefixed: {
        const auto size = varint();
        require(size <= 65535, "message parameter exceeds length limit");
        view(size);
        break;
      }
      case ParameterEncoding::TrackNamespace:
        track_namespace();
        break;
      }
      out.push_back({type, ByteBuffer(bytes.begin() + start, bytes.begin() + offset)});
    }
  }
};

template <typename T, typename Decode>
std::optional<T> decode(const ByteBuffer &payload, std::string &error, Decode parse) {
  try {
    Reader reader{{payload}};
    T value{};
    parse(reader, value);
    Reader::require(reader.remaining() == 0, "trailing control message bytes");
    return value;
  } catch (const std::invalid_argument &invalid) {
    error = invalid.what();
    return std::nullopt;
  }
}

inline void encode_parameters(ByteBuffer &payload, std::vector<Parameter> parameters) {
  std::stable_sort(parameters.begin(), parameters.end(),
                   [](const Parameter &left, const Parameter &right) { return left.type < right.type; });
  write_varint(payload, parameters.size());
  uint64_t previous = 0;
  for (const Parameter &parameter : parameters) {
    if ((previous != 0 && parameter.type <= previous) || !parameter_encoding(parameter.type)) {
      throw std::invalid_argument("unsupported or duplicate message parameter");
    }
    write_varint(payload, parameter.type - previous);
    payload.insert(payload.end(), parameter.encoded_value.begin(), parameter.encoded_value.end());
    previous = parameter.type;
  }
}

} // namespace moq::codec

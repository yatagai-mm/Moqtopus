#pragma once
#include "moq/types.h"
#include <stdexcept>

inline bool ParsePort(const char *value, uint16_t &port) {
  try {
    const unsigned long parsed = std::stoul(value);
    if (parsed == 0 || parsed > 65535) {
      return false;
    }
    port = static_cast<uint16_t>(parsed);
    return true;
  } catch (...) {
    return false;
  }
}

inline moq::TrackNamespace ParseNamespace(const std::string &value) {
  moq::TrackNamespace fields;
  size_t start = 0;
  while (start < value.size()) {
    const size_t slash = value.find('/', start);
    const size_t end = slash == std::string::npos ? value.size() : slash;
    if (end == start) {
      throw std::invalid_argument("namespace fields must not be empty");
    }
    fields.push_back(value.substr(start, end - start));
    if (slash == std::string::npos) {
      break;
    }
    start = slash + 1;
  }
  return fields;
}

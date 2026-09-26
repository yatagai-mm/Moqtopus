#include "data_plane.h"

#include "codec/codec_internal.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <mutex>
#include <optional>
#include <spdlog/spdlog.h>
#include <utility>

namespace moq {

constexpr uint64_t kNormalStatus = 0x0;
constexpr uint64_t kEndOfGroupStatus = 0x3;
constexpr uint64_t kEndOfTrackStatus = 0x4;

static bool is_valid_object_status(uint64_t status) {
  return status == kNormalStatus || status == kEndOfGroupStatus || status == kEndOfTrackStatus;
}

using Parse = DecodeStatus;

// object properties: varint length + opaque bytes
static Parse read_properties(Cursor &cursor, bool require_non_empty, BytesView &properties) {
  uint64_t length = 0;
  if (!cursor.read_varint(length)) {
    return Parse::NeedMoreData;
  }
  if ((require_non_empty && length == 0) || length > 65535) {
    return Parse::Error;
  }
  return cursor.read_view(length, properties) ? Parse::Done : Parse::NeedMoreData;
}

// Hot-path sink for one subgroup data stream. The header is parsed once and the
// route/flags are frozen; each subsequent receive is parsed in place over the
// QUIC buffers and objects are delivered as views. Only bytes of an object that
// straddles a receive boundary are copied (into stash_).
class SubgroupReceiver final : public StreamSink,
                               public PendingSubgroupStream,
                               public std::enable_shared_from_this<SubgroupReceiver> {
public:
  SubgroupReceiver(DataPlane &plane, std::shared_ptr<StreamContext> stream, ByteBuffer prefix)
      : plane_(plane), stream_(std::move(stream)), stash_(std::move(prefix)) {}

  void on_receive(const BytesView *chunks, size_t count, bool fin) override {
    const std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) {
      return;
    }
    if (stash_.empty() && count == 1) { // fast path: zero copy
      Cursor cursor{chunks[0]};
      run(cursor, fin);
      if (!closed_ && cursor.remaining() != 0) {
        stash_.assign(cursor.bytes.data + cursor.offset, cursor.bytes.data + cursor.bytes.size);
      }
      enforce_pending_buffer_limit();
      return;
    }
    for (size_t index = 0; index < count; ++index) {
      stash_.insert(stash_.end(), chunks[index].begin(), chunks[index].end());
    }
    Cursor cursor{stash_};
    run(cursor, fin);
    if (!closed_) {
      stash_.erase(stash_.begin(), stash_.begin() + static_cast<std::ptrdiff_t>(cursor.offset));
      enforce_pending_buffer_limit();
    } else {
      stash_.clear();
    }
  }

  void on_peer_send_aborted(uint64_t) override {
    const std::lock_guard<std::mutex> lock(mutex_);
    if (waiting_for_route_) {
      plane_.discard_pending_subgroup(alias_, this);
    }
    closed_ = true;
    waiting_for_route_ = false;
    stash_.clear();
  }

  void establish(std::shared_ptr<ReceiveRoute> route) override {
    const std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || !waiting_for_route_) {
      return;
    }
    spdlog::debug("resuming early subgroup stream {} for alias={} request={} with {} buffered bytes", stream_->id(),
                  alias_, route->request_id, stash_.size());
    route_ = std::move(route);
    waiting_for_route_ = false;
    Cursor cursor{stash_};
    run(cursor, fin_while_pending_);
    if (closed_) {
      stash_.clear();
    } else {
      stash_.erase(stash_.begin(), stash_.begin() + static_cast<std::ptrdiff_t>(cursor.offset));
    }
  }

private:
  void run(Cursor &cursor, bool fin) {
    if (!header_done_) {
      const size_t mark = cursor.offset;
      const Parse header = parse_header(cursor);
      if (header == Parse::Error || closed_) {
        return;
      }
      if (header == Parse::NeedMoreData) {
        cursor.offset = mark;
        if (fin) {
          fail("SUBGROUP_HEADER ended mid-header");
        }
        return;
      }
    }
    if (waiting_for_route_) {
      fin_while_pending_ = fin_while_pending_ || fin;
      return;
    }
    while (!closed_) {
      const size_t mark = cursor.offset;
      const Parse object = parse_object(cursor);
      if (object == Parse::Error) {
        return;
      }
      if (object == Parse::NeedMoreData) {
        cursor.offset = mark;
        if (fin && cursor.remaining() != 0) {
          fail("subgroup stream ended mid-object");
        }
        break;
      }
    }
    if (fin && !closed_) {
      closed_ = true;
      if (route_ && end_of_group_on_fin_ && last_object_id_) {
        route_->final_object_in_group[group_id_] = *last_object_id_;
      }
    }
  }

  Parse parse_header(Cursor &cursor) {
    uint64_t type = 0;
    uint64_t alias = 0;
    if (!cursor.read_varint(type)) {
      return Parse::NeedMoreData;
    }
    if (!is_subgroup_stream_type(type)) {
      return fail("invalid SUBGROUP_HEADER stream type");
    }
    if (!cursor.read_varint(alias) || !cursor.read_varint(group_id_)) {
      return Parse::NeedMoreData;
    }

    subgroup_id_mode_ = static_cast<uint8_t>((type & 0x06) >> 1);
    if (subgroup_id_mode_ == 0x02) {
      uint64_t subgroup = 0;
      if (!cursor.read_varint(subgroup)) {
        return Parse::NeedMoreData;
      }
      subgroup_id_ = subgroup;
    } else if (subgroup_id_mode_ == 0x00) {
      subgroup_id_ = 0;
    }

    const bool default_priority = (type & 0x20) != 0;
    if (!default_priority && !cursor.read_byte(publisher_priority_)) {
      return Parse::NeedMoreData;
    }

    alias_ = alias;
    properties_per_object_ = (type & 0x01) != 0;
    end_of_group_on_fin_ = (type & 0x08) != 0;
    if (default_priority) {
      publisher_priority_ = 128;
    }
    SPDLOG_TRACE("Subgroup stream {} header type={:#x} alias={} group={}", stream_->id(), type, alias_, group_id_);
    if (plane_.unknown_alias_policy() == UnknownAliasPolicy::Error) {
      route_ = plane_.find_route(alias_);
      if (!route_) {
        return fail("subgroup stream referenced unknown track alias " + std::to_string(alias_));
      }
    } else if (plane_.unknown_alias_policy() == UnknownAliasPolicy::Drop) {
      route_ = plane_.find_route(alias_);
      if (!route_) {
        abandon();
        return Parse::Done;
      }
    } else if (!plane_.find_or_park_subgroup(alias_, shared_from_this(), route_)) {
      abandon();
      return Parse::Done;
    }
    if (!route_) {
      // A SUBGROUP_HEADER only carries the publisher-selected Track Alias.
      // Keep the stream parked until SUBSCRIBE_OK establishes alias -> request.
      waiting_for_route_ = true;
      spdlog::debug("parking early subgroup stream {} for unknown track alias {}", stream_->id(), alias_);
    }
    header_done_ = true;
    return Parse::Done;
  }

  Parse parse_object(Cursor &cursor) {
    if (cursor.remaining() == 0) {
      return Parse::NeedMoreData;
    }
    uint64_t object_delta = 0;
    if (!cursor.read_varint(object_delta)) {
      return Parse::NeedMoreData;
    }
    BytesView properties;
    if (properties_per_object_) {
      const Parse props = read_properties(cursor, false, properties);
      if (props == Parse::Error) {
        return fail("invalid subgroup object properties");
      }
      if (props == Parse::NeedMoreData) {
        return Parse::NeedMoreData;
      }
    }
    uint64_t payload_length = 0;
    if (!cursor.read_varint(payload_length)) {
      return Parse::NeedMoreData;
    }

    std::optional<ObjectStatusCode> object_status;
    BytesView payload;
    if (payload_length == 0) {
      uint64_t status_code = 0;
      if (!cursor.read_varint(status_code)) {
        return Parse::NeedMoreData;
      }
      if (!is_valid_object_status(status_code) || (status_code != kNormalStatus && !properties.empty())) {
        return fail("invalid subgroup object status " + std::to_string(status_code) + " on stream " +
                    std::to_string(stream_->id()));
      }
      object_status = status_code;
    } else if (!cursor.read_view(payload_length, payload)) {
      return Parse::NeedMoreData;
    }

    ObjectId object_id = object_delta;
    if (last_object_id_) {
      if (*last_object_id_ == std::numeric_limits<uint64_t>::max() ||
          object_delta > std::numeric_limits<uint64_t>::max() - (*last_object_id_ + 1)) {
        return fail("subgroup Object ID delta overflow");
      }
      object_id = *last_object_id_ + object_delta + 1;
    }
    if (subgroup_id_mode_ == 0x01 && !subgroup_id_) {
      subgroup_id_ = object_id;
    }

    Object object;
    object.request_id = route_->request_id;
    object.track_alias = alias_;
    object.group_id = group_id_;
    object.subgroup_id = subgroup_id_.value_or(0);
    object.object_id = object_id;
    object.publisher_priority = publisher_priority_;
    object.status = object_status;
    object.properties = properties;
    object.payload = payload;
    object.delivery_kind = DeliveryKind::SubgroupStream;
    object.stream_id = stream_->id();
    last_object_id_ = object_id;
    plane_.deliver(*route_, object);
    return Parse::Done;
  }

  Parse fail(std::string error) {
    closed_ = true;
    plane_.protocol_error(std::move(error));
    return Parse::Error;
  }

  void enforce_pending_buffer_limit() {
    if (waiting_for_route_ && stash_.size() > plane_.max_buffered_subgroup_bytes_per_stream()) {
      spdlog::warn("abandoning early subgroup stream {} for alias={}: buffered bytes exceeded {}", stream_->id(),
                   alias_, plane_.max_buffered_subgroup_bytes_per_stream());
      abandon();
    }
  }

  void abandon() {
    if (waiting_for_route_) {
      plane_.discard_pending_subgroup(alias_, this);
    }
    stream_->abort_receive(0);
    closed_ = true;
    waiting_for_route_ = false;
    stash_.clear();
  }

  DataPlane &plane_;
  std::shared_ptr<StreamContext> stream_;
  std::mutex mutex_;
  ByteBuffer stash_;
  std::shared_ptr<ReceiveRoute> route_;
  bool header_done_ = false;
  bool closed_ = false;
  bool waiting_for_route_ = false;
  bool fin_while_pending_ = false;
  bool properties_per_object_ = false;
  bool end_of_group_on_fin_ = false;
  uint8_t subgroup_id_mode_ = 0;
  TrackAlias alias_ = 0;
  GroupId group_id_ = 0;
  std::optional<SubgroupId> subgroup_id_;
  uint8_t publisher_priority_ = 128;
  std::optional<ObjectId> last_object_id_;
};

DataPlane::DataPlane(SubscriberConfig config, ProtocolErrorCallback protocol_error, TrackErrorCallback track_error)
    : protocol_error(std::move(protocol_error)), track_error(std::move(track_error)), config_(std::move(config)) {}

bool DataPlane::install_route(TrackAlias alias, std::shared_ptr<ReceiveRoute> route) {
  std::vector<std::shared_ptr<PendingSubgroupStream>> pending_subgroups;
  {
    std::unique_lock<std::shared_mutex> lock(routes_mutex_);
    if (!routes_by_alias_.emplace(alias, route).second) {
      return false;
    }
    const auto pending = pending_subgroups_by_alias_.find(alias);
    if (pending != pending_subgroups_by_alias_.end()) {
      pending_subgroups = std::move(pending->second);
      pending_subgroup_stream_count_ -= pending_subgroups.size();
      pending_subgroups_by_alias_.erase(pending);
    }
  }
  const auto buffered = unknown_datagrams_.find(alias);
  if (buffered != unknown_datagrams_.end()) {
    const std::vector<ByteBuffer> datagrams = std::move(buffered->second);
    unknown_datagrams_.erase(buffered);
    for (const ByteBuffer &datagram : datagrams) {
      buffered_datagram_bytes_ -= datagram.size();
      deliver_datagram(BytesView{datagram}, false);
    }
  }
  for (const auto &subgroup : pending_subgroups) {
    subgroup->establish(route);
  }
  return true;
}

void DataPlane::retire_route(TrackAlias alias) {
  std::unique_lock<std::shared_mutex> lock(routes_mutex_);
  const auto route = routes_by_alias_.find(alias);
  if (route == routes_by_alias_.end()) {
    return;
  }
  route->second->active.store(false); // in-flight deliveries check this after route removal
  routes_by_alias_.erase(route);
}

std::shared_ptr<ReceiveRoute> DataPlane::find_route(TrackAlias alias) const {
  std::shared_lock<std::shared_mutex> lock(routes_mutex_);
  const auto route = routes_by_alias_.find(alias);
  return route == routes_by_alias_.end() ? nullptr : route->second;
}

bool DataPlane::find_or_park_subgroup(TrackAlias alias, std::shared_ptr<PendingSubgroupStream> stream,
                                      std::shared_ptr<ReceiveRoute> &route) {
  std::unique_lock<std::shared_mutex> lock(routes_mutex_);
  const auto established = routes_by_alias_.find(alias);
  if (established != routes_by_alias_.end()) {
    route = established->second;
    return true;
  }
  if (pending_subgroup_stream_count_ >= config_.max_pending_subgroup_streams) {
    return false;
  }
  pending_subgroups_by_alias_[alias].push_back(std::move(stream));
  ++pending_subgroup_stream_count_;
  return true;
}

void DataPlane::discard_pending_subgroup(TrackAlias alias, const PendingSubgroupStream *stream) {
  std::unique_lock<std::shared_mutex> lock(routes_mutex_);
  const auto found = pending_subgroups_by_alias_.find(alias);
  if (found == pending_subgroups_by_alias_.end()) {
    return;
  }
  auto &pending = found->second;
  const auto new_end = std::remove_if(pending.begin(), pending.end(),
                                      [stream](const auto &candidate) { return candidate.get() == stream; });
  pending_subgroup_stream_count_ -= static_cast<size_t>(std::distance(new_end, pending.end()));
  pending.erase(new_end, pending.end());
  if (pending.empty()) {
    pending_subgroups_by_alias_.erase(found);
  }
}

void DataPlane::on_datagram(BytesView datagram) { deliver_datagram(datagram, true); }

void DataPlane::start_subgroup_stream(const std::shared_ptr<StreamContext> &stream, ByteBuffer prefix, bool fin) {
  auto receiver = std::make_shared<SubgroupReceiver>(*this, stream, std::move(prefix));
  stream->set_sink(receiver);            // swap StreamContext's StreamSink from PeerStreamGate to SubgroupReceiver
  receiver->on_receive(nullptr, 0, fin); // parse the prefix the gate buffered
}

void DataPlane::deliver_datagram(BytesView bytes, bool allow_buffer) {
  Cursor cursor{bytes};
  uint64_t type = 0;
  if (!cursor.read_varint(type)) {
    return protocol_error("datagram has malformed type");
  }
  if (type == kPaddingDatagramType) {
    if (!std::all_of(cursor.bytes.data + cursor.offset, cursor.bytes.data + cursor.bytes.size,
                     [](uint8_t byte) { return byte == 0; })) {
      protocol_error("padding datagram contains non-zero bytes");
    }
    return;
  }
  if ((type > 0x0f && (type < 0x20 || type > 0x2f)) || ((type & 0x20) != 0 && (type & 0x02) != 0)) {
    return protocol_error("invalid object datagram type");
  }

  uint64_t alias = 0;
  uint64_t group = 0;
  if (!cursor.read_varint(alias) || !cursor.read_varint(group)) {
    return protocol_error("truncated object datagram header");
  }
  uint64_t object_id = 0;
  if ((type & 0x04) == 0 && !cursor.read_varint(object_id)) {
    return protocol_error("truncated object datagram Object ID");
  }

  const std::shared_ptr<ReceiveRoute> route = find_route(alias);
  if (!route) {
    if (allow_buffer) {
      buffer_unknown_datagram(alias, bytes);
    }
    return;
  }

  uint8_t publisher_priority = 128;
  if ((type & 0x08) == 0 && !cursor.read_byte(publisher_priority)) {
    return protocol_error("truncated object datagram priority");
  }
  BytesView properties;
  if ((type & 0x01) != 0 && read_properties(cursor, true, properties) != Parse::Done) {
    return protocol_error("invalid object datagram properties");
  }

  std::optional<ObjectStatusCode> object_status;
  BytesView payload;
  if ((type & 0x20) != 0) {
    uint64_t status_code = 0;
    if (!cursor.read_varint(status_code) || !is_valid_object_status(status_code) ||
        (status_code != kNormalStatus && !properties.empty()) || cursor.remaining() != 0) {
      return protocol_error("invalid object datagram status");
    }
    object_status = status_code;
  } else {
    payload = BytesView{cursor.bytes.data + cursor.offset, cursor.remaining()};
  }

  Object object;
  object.request_id = route->request_id;
  object.track_alias = alias;
  object.group_id = group;
  object.object_id = object_id;
  object.publisher_priority = publisher_priority;
  object.status = object_status;
  object.properties = properties;
  object.payload = payload;
  object.delivery_kind = DeliveryKind::Datagram;
  if ((type & 0x02) != 0) {
    route->final_object_in_group[group] = object_id;
  }
  deliver(*route, object);
}

void DataPlane::buffer_unknown_datagram(TrackAlias alias, BytesView bytes) {
  switch (config_.unknown_alias_policy) {
  case UnknownAliasPolicy::Drop:
    return;
  case UnknownAliasPolicy::Error:
    protocol_error("datagram referenced unknown track alias " + std::to_string(alias));
    return;
  case UnknownAliasPolicy::Buffer:
    break;
  }
  if (bytes.size > config_.max_buffered_datagram_bytes ||
      buffered_datagram_bytes_ + bytes.size > config_.max_buffered_datagram_bytes) {
    return;
  }
  std::vector<ByteBuffer> &datagrams = unknown_datagrams_[alias];
  if (datagrams.size() >= config_.max_buffered_datagrams_per_alias) {
    return;
  }
  buffered_datagram_bytes_ += bytes.size;
  datagrams.push_back(bytes.to_owned());
}

void DataPlane::deliver(ReceiveRoute &route, const Object &object) {
  if (!route.active.load()) {
    return;
  }
  const auto final = route.final_object_in_group.find(object.group_id);
  if (final != route.final_object_in_group.end() && object.object_id > final->second) {
    return track_error(route.request_id, "object followed final object in group");
  }
  if (route.final_object_in_track && (object.group_id > route.final_object_in_track->group ||
                                      (object.group_id == route.final_object_in_track->group &&
                                       object.object_id > route.final_object_in_track->object))) {
    return track_error(route.request_id, "object followed final object in track");
  }
  if (object.status && *object.status == kEndOfGroupStatus) {
    route.final_object_in_group[object.group_id] = object.object_id;
  }
  if (object.status && *object.status == kEndOfTrackStatus) {
    route.final_object_in_track = Location{object.group_id, object.object_id};
  }
  route.handler->on_object(object);
}

} // namespace moq

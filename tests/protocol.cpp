#include "fake_transport.h"
#include "send_data_plane.h"
#include "moq/codec.h"
#include "moq/publisher_session.h"
#include "moq/subscriber_session.h"
#include <cassert>
#include <chrono>
#include <limits>
#include <random>
using namespace moq;
using namespace std::chrono_literals;

struct Handler : ObjectHandler {
  std::vector<ObjectId> ids;
  std::vector<ByteBuffer> payloads;
  size_t done = 0, errors = 0;
  void on_object(const Object &o) override {
    ids.push_back(o.object_id);
    payloads.push_back(o.payload.to_owned());
  }
  void on_publish_done(PublishDone) override { ++done; }
  void on_error(ReceiveError) override { ++errors; }
};
void fragmented(fake::Stream &stream, const ByteBuffer &bytes, bool fin = false) {
  for (size_t i = 0; i < bytes.size(); ++i)
    stream.receive({bytes[i]}, fin && i + 1 == bytes.size());
}
void setup() { fragmented(fake::connection->peer_stream(true), encode_setup("relay", "/")); }

void codec_tests() {
  for (const uint64_t type : {0x10, 0x15, 0x18, 0x1d, 0x30, 0x35, 0x38, 0x3d,
                              0x50, 0x55, 0x58, 0x5d, 0x70, 0x75, 0x78, 0x7d}) {
    assert(is_subgroup_stream_type(type));
  }
  for (const uint64_t type : {0x0f, 0x16, 0x1f, 0x2f, 0x36, 0x3f, 0x56, 0x5f,
                              0x76, 0x7f, 0x80}) {
    assert(!is_subgroup_stream_type(type));
  }
  std::mt19937_64 random(18);
  for (unsigned i = 0; i < 10000; ++i) {
    uint64_t value = i < 64 ? (uint64_t{1} << i) - 1 : random();
    ByteBuffer bytes;
    write_varint(bytes, value);
    VarintResult result = read_varint(bytes);
    assert(result.status == DecodeStatus::Done && result.value == value && result.bytes == bytes.size());
    for (size_t n = 0; n < bytes.size(); ++n)
      assert(read_varint(bytes.data(), n).status == DecodeStatus::NeedMoreData);
  }
  ByteBuffer max;
  write_varint(max, UINT64_MAX);
  assert(max == ByteBuffer(9, 0xff));
  assert(encode_subscribe(0, {{"a"}, "b", {}}) == (ByteBuffer{3, 0, 7, 0, 1, 1, 'a', 1, 'b', 0}));
  assert(encode_request_update(2, {}) == (ByteBuffer{2, 0, 2, 2, 0}));
  assert(encode_request_ok({}) == (ByteBuffer{7, 0, 1, 0}));
  assert(encode_publish_done(1, 2, "x") == (ByteBuffer{11, 0, 4, 1, 2, 1, 'x'}));
  assert(encode_subscribe_ok(5, {}, {}) == (ByteBuffer{4, 0, 2, 5, 0}));
  std::string error;
  assert(!decode_subscribe({0, 33}, error));
  assert(!decode_setup({1, 2, 'x'}, error));
  assert(!decode_request_update({0, 2, 16, 1, 0, 1}, error)); // duplicate parameter
  assert(!decode_request_update({0, 1, 0x33, 0}, error));     // unknown parameter
  assert(!decode_request_update({0, 0, 0}, error));           // trailing data
  assert(!decode_publish_done({1, 2, 2, 'x'}, error));
  ByteBuffer framed = encode_subscribe(4, {{"a", "b"}, "c", {Parameter::uint8(kParameterForward, 0)}});
  for (size_t n = 0; n < framed.size(); ++n) {
    assert(read_control_message(ByteBuffer(framed.begin(), framed.begin() + n)).status == DecodeStatus::NeedMoreData);
  }
  std::optional<Subscribe> subscribe = decode_subscribe(read_control_message(framed).message.payload, error);
  assert(subscribe && subscribe->request_id == 4 && subscribe->track_namespace == (TrackNamespace{"a", "b"}));
  SubscriptionOptions options;
  assert(!decode_subscription_options({Parameter::uint8(kParameterForward, 2)}, options, error));
  assert(!decode_subscription_options({Parameter::uint8(kParameterGroupOrder, 0)}, options, error));
  ByteBuffer filter;
  write_varint(filter, kFilterAbsoluteRange);
  write_varint(filter, UINT64_MAX);
  write_varint(filter, 0);
  write_varint(filter, 1);
  assert(
      !decode_subscription_options({Parameter::length_prefixed(kParameterSubscriptionFilter, filter)}, options, error));
  bool threw = false;
  try {
    encode_control_message(3, ByteBuffer(65536));
  } catch (const std::length_error &) {
    threw = true;
  }
  assert(threw);
}

void subscriber_tests() {
  std::unique_ptr<Subscriber> session = Subscriber::connect({});
  std::future<void> ready = session->ready();
  assert(ready.wait_for(0ms) == std::future_status::timeout);
  setup();
  ready.get();
  assert(session->state().phase == SessionPhase::Ready);
  std::shared_ptr<Handler> handler = std::make_shared<Handler>();
  std::future<Subscription> pending = session->subscribe({{"test"}, "track", {}}, handler);
  fake::Stream &request = *fake::connection->streams.back();
  // Data can precede SUBSCRIBE_OK. Datagrams and complete Subgroup streams are
  // replayed when the response establishes the alias -> request mapping.
  fake::connection->callbacks.datagram_received(
      encode_object_datagram(7, 0, 0, kDefaultPublisherPriority, {}, {}, ByteBuffer{42}, false));
  ByteBuffer early_data;
  encode_subgroup_header(early_data, 7, 2, 0, kDefaultPublisherPriority);
  encode_subgroup_object(early_data, 0, {}, {}, ByteBuffer{9});
  fake::Stream &early_stream = fake::connection->peer_stream(true);
  fragmented(early_stream, early_data, true);
  assert(handler->ids.empty() && early_stream.shutdowns.empty());
  fragmented(request, encode_subscribe_ok(7, {}, {}));
  Subscription handle = pending.get();
  assert(handle.track_alias == 7 && handler->ids == (std::vector<ObjectId>{0, 0}));
  assert(handler->payloads == (std::vector<ByteBuffer>{{42}, {9}}));
  const SubscriptionStateSnapshot state = session->subscription_state(handle.request_id);
  assert(state.phase == SubscriptionPhase::Established && state.request_id == handle.request_id &&
         state.track_alias == handle.track_alias);
  assert(session->state().active_subscriptions == 1);
  ByteBuffer data;
  encode_subgroup_header(data, 7, 1, 0, kDefaultPublisherPriority);
  encode_subgroup_object(data, 0, {}, {}, ByteBuffer{1, 2});
  encode_subgroup_object(data, 2, {}, {}, ByteBuffer{3, 4});
  fragmented(fake::connection->peer_stream(true), data, true);
  assert(handler->ids == (std::vector<ObjectId>{0, 0, 0, 3}));
  assert(handler->payloads.back() == (ByteBuffer{3, 4}));
  std::future<RequestOk> update = session->request_update(handle.request_id, {});
  assert(session->subscription_state(handle.request_id).inflight_updates == 1);
  fragmented(request, encode_request_ok({}));
  (void)update.get();
  assert(session->subscription_state(handle.request_id).inflight_updates == 0);
  std::future<RequestOk> failed = session->request_update(handle.request_id, {});
  ByteBuffer rejected = encode_request_error(RequestErrorCode::NotSupported, "no");
  ByteBuffer done = encode_publish_done(static_cast<uint64_t>(PublishDoneCode::UpdateFailed), 1, "");
  rejected.insert(rejected.end(), done.begin(), done.end());
  request.receive(rejected, true);
  bool threw = false;
  try {
    (void)failed.get();
  } catch (const RequestRejected &) {
    threw = true;
  }
  assert(threw && handler->done == 1 &&
         session->subscription_state(handle.request_id).phase == SubscriptionPhase::Terminated);
  assert(session->state().active_subscriptions == 0);
  std::future<Subscription> pending2 = session->subscribe({{"test"}, "other", {}}, handler);
  fake::connection->streams.back()->receive({4, 0}, true);
  assert(pending2.wait_for(0ms) == std::future_status::ready);
  threw = false;
  try {
    (void)pending2.get();
  } catch (const RequestRejected &) {
    threw = true;
  }
  assert(threw);
  std::future<Subscription> pending3 = session->subscribe({{"test"}, "last", {}}, handler);
  fake::connection->streams.back()->receive(encode_subscribe_ok(0, {}, {}));
  Subscription last = pending3.get();
  assert(last.request_id != handle.request_id && last.track_alias == 0);
  assert(session->subscription_state(last.request_id).track_alias == 0);
  fake::connection->fail_send = true;
  std::future<RequestOk> unsent = session->request_update(last.request_id, {});
  threw = false;
  try {
    (void)unsent.get();
  } catch (const std::runtime_error &) {
    threw = true;
  }
  assert(threw && session->subscription_state(last.request_id).inflight_updates == 0);
  fake::connection->fail_send = false;
  session->stop_subscription(last.request_id);
  assert(session->subscription_state(last.request_id).phase == SubscriptionPhase::Terminated);
  session->close();
}

void track_properties_tests() {
  assert(encode_track_properties({}).empty());
  TrackProperties properties;
  properties.object_delivery_timeout = 10;
  properties.max_cache_duration = 20;
  properties.subgroup_delivery_timeout = 30;
  properties.default_publisher_priority = 7;
  properties.default_publisher_group_order = GroupOrder::Descending;
  properties.dynamic_groups = true;
  // Independent wire fixture: delta types, with even types carrying varints.
  const ByteBuffer expected{2, 10, 2, 20, 2, 30, 8, 7, 20, 2, 14, 1};
  assert(encode_track_properties(properties) == expected);

  TrackProperties zeros;
  zeros.object_delivery_timeout = 0;
  zeros.dynamic_groups = false;
  assert(encode_track_properties(zeros) == (ByteBuffer{2, 0, 46, 0}));
  TrackProperties nested;
  nested.object_delivery_timeout = 10;
  nested.default_publisher_priority = 7;
  nested.immutable_properties.emplace();
  nested.immutable_properties->max_cache_duration = 20;
  nested.immutable_properties->dynamic_groups = false;
  assert(encode_track_properties(nested) == (ByteBuffer{2, 10, 9, 4, 4, 20, 44, 0, 3, 7}));
  TrackProperties empty_container;
  empty_container.immutable_properties.emplace();
  assert(encode_track_properties(empty_container) == (ByteBuffer{11, 0}));

  TrackProperties maximum;
  maximum.object_delivery_timeout = UINT64_MAX;
  maximum.default_publisher_priority = 255;
  const ByteBuffer encoded_max = encode_track_properties(maximum);
  const VarintResult timeout = read_varint(encoded_max, 1);
  assert(timeout.status == DecodeStatus::Done && timeout.value == UINT64_MAX);
  const VarintResult priority = read_varint(encoded_max, 1 + timeout.bytes + 1);
  assert(priority.status == DecodeStatus::Done && priority.value == 255);

  const auto invalid = [](const TrackProperties &value) {
    bool threw = false;
    try {
      (void)encode_track_properties(value);
    } catch (const std::invalid_argument &) {
      threw = true;
    }
    assert(threw);
  };
  nested.dynamic_groups = false; // explicit false still counts as a duplicate
  invalid(nested);
  TrackProperties bad_order;
  bad_order.default_publisher_group_order = static_cast<GroupOrder>(0);
  invalid(bad_order);
  bad_order.default_publisher_group_order.reset();
  bad_order.immutable_properties.emplace();
  bad_order.immutable_properties->default_publisher_group_order = static_cast<GroupOrder>(3);
  invalid(bad_order);

  std::unique_ptr<Publisher> session = Publisher::connect({});
  setup();
  bool threw = false;
  try {
    session->register_track({{"test"}, "typed", bad_order});
  } catch (const std::invalid_argument &) {
    threw = true;
  }
  assert(threw);
  // Failed validation did not register the track. The later valid registration
  // snapshots the values, independent of subsequent app-side mutations.
  session->register_track({{"test"}, "typed", properties});
  properties.object_delivery_timeout = 99;
  fake::Stream &request = fake::connection->peer_stream(false);
  request.receive(encode_subscribe(1, {{"test"}, "typed", {}}));
  const ControlMessageResult message = read_control_message(request.sent);
  assert(message.message.type == kMessageSubscribeOk);
  std::string error;
  const std::optional<SubscribeOk> ok = decode_subscribe_ok(message.message.payload, error);
  assert(ok && ok->track_properties == expected);
}

void object_delivery_timeout_options_tests() {
  for (const bool immutable : {false, true}) {
    for (const uint64_t publisher : {uint64_t{0}, uint64_t{100}, UINT64_MAX}) {
      SendDataPlane plane({});
      PublishedTrack track{{"test"}, "timeouts", {}};
      if (immutable) {
        track.track_properties.immutable_properties.emplace();
        track.track_properties.immutable_properties->object_delivery_timeout = publisher;
      } else {
        track.track_properties.object_delivery_timeout = publisher;
      }
      assert(plane.register_track(track));
      assert(!plane.attach_subscription(1, 1, {"test"}, "timeouts", {}));
      assert(plane.object_delivery_timeout(1) == publisher);
      SubscriptionOptions options;
      options.object_delivery_timeout = 50;
      assert(!plane.attach_subscription(2, 2, {"test"}, "timeouts", options));
      assert(plane.object_delivery_timeout(2) == 50);
      assert(!plane.update_subscription(1, options));
      assert(plane.object_delivery_timeout(1) == 50);
      assert(!plane.update_subscription(1, {}));
      assert(plane.object_delivery_timeout(1) == 50);
      options.object_delivery_timeout = UINT64_MAX;
      assert(!plane.update_subscription(1, options));
      assert(plane.object_delivery_timeout(1) == (publisher == 0 ? UINT64_MAX : publisher));
      options.object_delivery_timeout = 0;
      assert(!plane.update_subscription(1, options));
      assert(plane.object_delivery_timeout(1) == publisher);
      assert(plane.object_delivery_timeout(2) == 50); // independent subscriptions
      options.object_delivery_timeout = 1;
      options.subgroup_delivery_timeout = 1;
      assert(plane.update_subscription(1, options)->code == RequestErrorCode::NotSupported);
      assert(plane.object_delivery_timeout(1) == publisher); // rejected update is atomic
      assert(plane.attach_subscription(3, 3, {"test"}, "timeouts", options));
      assert(!plane.object_delivery_timeout(3));
      plane.detach_subscription(1);
      assert(!plane.object_delivery_timeout(1));
    }
  }
  SendDataPlane plane({});
  assert(plane.register_track({{"test"}, "unset", {}}));
  assert(!plane.attach_subscription(1, 1, {"test"}, "unset", {}));
  assert(plane.object_delivery_timeout(1) == 0);
}

void subgroup_object_timeout_tests() {
  using Clock = std::chrono::steady_clock;
  Clock::time_point now = Clock::time_point{} + 10s;
  std::chrono::milliseconds open_delay{0};
  MsQuicTransportAdapter transport({}, {});
  SendDataPlane plane({
      [&] {
        now += open_delay;
        return transport.open_stream(true);
      },
      [&](ByteBuffer bytes) { return transport.send_datagram(std::move(bytes)); },
      [](RequestId, PublishDoneCode, std::string) { assert(false); },
      [&] { return now; }});
  assert(plane.register_track({{"test"}, "timeouts", {}}));
  SubscriptionOptions options;
  options.object_delivery_timeout = 10;
  assert(!plane.attach_subscription(1, 1, {"test"}, "timeouts", options));
  PublishedObject object;
  object.track_namespace = {"test"};
  object.track_name = "timeouts";
  object.payload = {1};
  object.received_at = now - 10ms + 1ns;
  plane.publish(object); // just before the deadline
  assert(fake::connection->streams.size() == 1);
  fake::Stream &first = *fake::connection->streams.front();
  assert(!first.sent.empty() && first.shutdowns.empty());
  object.object_id++;
  object.received_at = now - 10ms;
  const size_t previous_size = first.sent.size();
  plane.publish(object); // exact deadline is allowed
  assert(first.sent.size() > previous_size);
  const ByteBuffer sent = first.sent;
  object.object_id++;
  object.received_at = now - 10ms - 1ns;
  plane.publish(object); // even a sub-millisecond excess expires
  assert(first.sent == sent && !first.fin);
  assert(first.shutdowns.size() == 1);
  assert(first.shutdowns.front().first == QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND);
  assert(first.shutdowns.front().second == static_cast<uint64_t>(StreamResetCode::DeliveryTimeout));
  object.object_id++;
  object.received_at = now;
  plane.publish(object);
  assert(fake::connection->streams.size() == 1 && first.shutdowns.size() == 1);
  options.object_delivery_timeout = 0;
  assert(!plane.update_subscription(1, options));
  plane.publish(object); // disabling the timeout must not reopen the subgroup
  assert(fake::connection->streams.size() == 1);

  // A new subscription to the same track has independent subgroup state.
  assert(!plane.attach_subscription(2, 2, {"test"}, "timeouts", options));
  plane.publish(object);
  assert(fake::connection->streams.size() == 2);
  assert(plane.detach_subscription(2) == 1);
  options.object_delivery_timeout = 10;
  assert(!plane.update_subscription(1, options));
  object.subgroup_id = 1;
  plane.publish(object); // different subgroup in the same group still works
  assert(fake::connection->streams.size() == 3);
  object.group_id = 1;
  object.subgroup_id = 0;
  plane.publish(object); // same subgroup ID in another group still works
  assert(fake::connection->streams.size() == 4);

  object.group_id = 2;
  object.received_at = now - 11ms;
  plane.publish(object); // first object expired: no stream to reset or count
  assert(fake::connection->streams.size() == 4);
  object.received_at = now;
  plane.publish(object);
  assert(fake::connection->streams.size() == 4);

  object.group_id = 3;
  open_delay = 11ms;
  plane.publish(object); // expires between the initial check and StreamSend
  assert(fake::connection->streams.size() == 5);
  fake::Stream &late = *fake::connection->streams.back();
  assert(late.sent.empty() && late.shutdowns.size() == 1);
  assert(late.shutdowns.front().second == static_cast<uint64_t>(StreamResetCode::DeliveryTimeout));
  open_delay = 0ms;
  object.received_at = now;
  plane.publish(object);
  assert(fake::connection->streams.size() == 5);

  options.object_delivery_timeout = UINT64_MAX;
  assert(!plane.update_subscription(1, options));
  object.group_id = 4;
  object.received_at = now - 1s;
  object.end_of_subgroup = true;
  plane.publish(object); // huge wire timeout must not overflow
  assert(fake::connection->streams.size() == 6 && fake::connection->streams.back()->fin);
  options.object_delivery_timeout = 0;
  assert(!plane.update_subscription(1, options));
  object.group_id = 5;
  plane.publish(object); // zero means unlimited
  assert(fake::connection->streams.size() == 7 && fake::connection->streams.back()->fin);
  assert(plane.detach_subscription(1) == 6); // includes streams reset before sending
  assert(!plane.attach_subscription(3, 3, {"test"}, "timeouts", options));
  object.group_id = 0;
  plane.publish(object); // tombstones disappear with the old subscription
  assert(fake::connection->streams.size() == 8);
  assert(plane.detach_subscription(3) == 1);
}

void publisher_tests() {
  std::unique_ptr<Publisher> session = Publisher::connect({});
  session->register_track({{"test"}, "track", {}});
  setup();
  session->ready().get();
  assert(read_control_message(fake::connection->streams.back()->sent).message.type == kMessagePublishNamespace);
  fake::Stream &request = fake::connection->peer_stream(false);
  fragmented(request, encode_subscribe(1, {{"test"}, "track",
                                          {Parameter::varint(kParameterObjectDeliveryTimeout, 100)}}));
  assert(read_control_message(request.sent).message.type == kMessageSubscribeOk);
  assert(session->state().active_subscriptions == 1);
  PublishedObject object;
  object.track_namespace = {"test"};
  object.track_name = "track";
  object.payload = {1, 2};
  session->publish(object);
  fake::Stream &data = *fake::connection->streams.back();
  ByteBuffer expected;
  encode_subgroup_header(expected, 0, 0, 0, kDefaultPublisherPriority);
  encode_subgroup_object(expected, 0, {}, {}, object.payload);
  assert(data.sent == expected);
  request.sent.clear();
  fragmented(request, encode_request_update(3, {{Parameter::uint8(kParameterForward, 0)}}));
  assert(read_control_message(request.sent).message.type == kMessageRequestOk);
  object.object_id = 1;
  session->publish(object);
  assert(data.sent == expected);
  fragmented(request, encode_request_update(5, {{Parameter::uint8(kParameterForward, 1)}}));
  object.delivery_kind = DeliveryKind::Datagram;
  session->publish(object);
  assert(fake::connection->datagrams.size() == 1);
  request.sent.clear();
  fragmented(request, encode_request_update(7, {{Parameter::varint(kParameterObjectDeliveryTimeout, 1)}}));
  assert(read_control_message(request.sent).message.type == kMessageRequestOk);
  assert(!request.fin && data.shutdowns.empty() && session->state().active_subscriptions == 1);
  request.sent.clear();
  fragmented(request, encode_request_update(9, {{Parameter::varint(kParameterSubgroupDeliveryTimeout, 1)}}));
  ControlMessageResult error = read_control_message(request.sent);
  assert(error.message.type == kMessageRequestError);
  assert(read_control_message(request.sent, error.bytes).message.type == kMessagePublishDone);
  assert(request.fin && !data.shutdowns.empty() && session->state().active_subscriptions == 0);
  fake::Stream &duplicate = fake::connection->peer_stream(false);
  duplicate.receive(encode_subscribe(1, {{"test"}, "track", {}}));
  assert(fake::connection->close_code == SessionCloseErrorCode::InvalidRequestId);
}
void setup_buffering_tests() {
  {
    std::unique_ptr<Publisher> session = Publisher::connect({});
    session->register_track({{"test"}, "track", {}});
    fake::Stream &request = fake::connection->peer_stream(false);
    fragmented(request, encode_subscribe(1, {{"test"}, "track", {}}));
    fragmented(request, encode_request_update(3, {{Parameter::uint8(kParameterForward, 0)}}));
    assert(request.sent.empty());
    assert(session->state().phase == SessionPhase::SetupInProgress);
    setup();
    const ControlMessageResult first = read_control_message(request.sent);
    assert(first.message.type == kMessageSubscribeOk);
    assert(read_control_message(request.sent, first.bytes).message.type == kMessageRequestOk);
    assert(session->state().active_subscriptions == 1);
  }
  {
    std::unique_ptr<Publisher> session = Publisher::connect({});
    session->register_track({{"test"}, "track", {}});
    fake::Stream &request = fake::connection->peer_stream(false);
    const ByteBuffer bytes = encode_subscribe(1, {{"test"}, "track", {}});
    request.receive({bytes.front()});
    setup();
    assert(request.sent.empty());
    request.receive(ByteBuffer(bytes.begin() + 1, bytes.end()));
    assert(read_control_message(request.sent).message.type == kMessageSubscribeOk);
  }
  {
    std::unique_ptr<Publisher> session = Publisher::connect({});
    fake::Stream &invalid = fake::connection->peer_stream(false);
    invalid.receive(encode_control_message(0xdead, {}));
    fake::Stream &later = fake::connection->peer_stream(false);
    later.receive(encode_subscribe(1, {{"test"}, "track", {}}));
    setup();
    assert(session->state().phase == SessionPhase::Closed);
    assert(fake::connection->close_code == SessionCloseErrorCode::ProtocolViolation);
    assert(later.sent.empty()); // Stop replay if an earlier message closes the session.
  }
  // Observe dispatch directly: data must not reach a role handler before SETUP,
  // even if the whole stream (including FIN) or datagram has arrived already.
  struct Probe : Session {
    Probe() : Session({}) { start(); }
    ~Probe() { close_and_wait(); }
    std::vector<ByteBuffer> data, datagrams;
    std::vector<bool> fins;
    size_t requests = 0;
    void handle_data_stream(uint64_t, const std::shared_ptr<StreamContext> &, ByteBuffer bytes, bool fin) override {
      assert(state_.phase == SessionPhase::Ready);
      data.push_back(std::move(bytes));
      fins.push_back(fin);
    }
    void handle_peer_request(const ControlMessage &, const std::shared_ptr<StreamContext> &, ByteBuffer,
                             bool) override { ++requests; }
    void on_datagram(BytesView bytes) override { datagrams.push_back(bytes.to_owned()); }
    void terminate_subscriptions(const std::string &) override {}
    size_t active_subscriptions() const override { return 0; }
  };
  {
    Probe session;
    ByteBuffer bytes;
    encode_subgroup_header(bytes, 7, 0, 0, kDefaultPublisherPriority);
    encode_subgroup_object(bytes, 0, {}, {}, ByteBuffer{42});
    fragmented(fake::connection->peer_stream(true), bytes, true);
    ByteBuffer datagram{1, 2, 3};
    fake::connection->callbacks.datagram_received(datagram);
    datagram[0] = 9; // The transport's receive memory may be reused immediately.
    assert(session.data.empty() && session.datagrams.empty());
    setup();
    assert(session.data == (std::vector<ByteBuffer>{bytes}));
    assert(session.fins == (std::vector<bool>{true}));
    assert(session.datagrams == (std::vector<ByteBuffer>{{1, 2, 3}}));
  }
  {
    Probe session;
    fake::Stream &request = fake::connection->peer_stream(false);
    fragmented(request, encode_subscribe(1, {{"test"}, "track", {}}));
    QUIC_STREAM_EVENT aborted{};
    aborted.Type = QUIC_STREAM_EVENT_PEER_SEND_ABORTED;
    request.event(&aborted);
    setup();
    assert(session.requests == 0);
  }
  {
    Probe session;
    fragmented(fake::connection->peer_stream(false), encode_subscribe(1, {{"test"}, "track", {}}));
    fake::connection->callbacks.datagram_received(ByteBuffer{1});
    session.close();
    setup();
    assert(session.requests == 0 && session.datagrams.empty());
  }
  {
    std::unique_ptr<Subscriber> session = Subscriber::connect({});
    fake::Stream &request = fake::connection->peer_stream(false);
    fragmented(request, encode_subscribe(1, {{"test"}, "track", {}}), true);
    assert(request.sent.empty() && request.shutdowns.empty());
    setup();
    assert(read_control_message(request.sent).message.type == kMessageRequestError);
    assert(session->state().phase == SessionPhase::Ready);
  }
}

void edge_tests() {
  {
    // A Subgroup carries no Request ID. With concurrent pending SUBSCRIBEs,
    // each stream remains parked by alias until its own SUBSCRIBE_OK arrives.
    std::unique_ptr<Subscriber> session = Subscriber::connect({});
    setup();
    std::shared_ptr<Handler> first_handler = std::make_shared<Handler>();
    std::shared_ptr<Handler> second_handler = std::make_shared<Handler>();
    std::future<Subscription> first_pending = session->subscribe({{"test"}, "first", {}}, first_handler);
    fake::Stream &first_request = *fake::connection->streams.back();
    std::future<Subscription> second_pending = session->subscribe({{"test"}, "second", {}}, second_handler);
    fake::Stream &second_request = *fake::connection->streams.back();

    ByteBuffer second_data;
    encode_subgroup_header(second_data, 8, 0, 0, kDefaultPublisherPriority);
    encode_subgroup_object(second_data, 0, {}, {}, ByteBuffer{8});
    fragmented(fake::connection->peer_stream(true), second_data, true);
    ByteBuffer first_data;
    encode_subgroup_header(first_data, 7, 0, 0, kDefaultPublisherPriority);
    encode_subgroup_object(first_data, 0, {}, {}, ByteBuffer{7});
    fragmented(fake::connection->peer_stream(true), first_data, true);
    assert(first_handler->ids.empty() && second_handler->ids.empty());

    first_request.receive(encode_subscribe_ok(7, {}, {}));
    assert(first_pending.get().track_alias == 7);
    assert(first_handler->payloads == (std::vector<ByteBuffer>{{7}}));
    assert(second_handler->ids.empty());
    second_request.receive(encode_subscribe_ok(8, {}, {}));
    assert(second_pending.get().track_alias == 8);
    assert(second_handler->payloads == (std::vector<ByteBuffer>{{8}}));
    session->close();
  }
  {
    std::unique_ptr<Subscriber> session = Subscriber::connect({});
    std::future<void> ready = session->ready();
    fake::Stream &padding = fake::connection->peer_stream(true);
    ByteBuffer bytes;
    write_varint(bytes, kPaddingStreamType);
    bytes.push_back(0);
    fragmented(padding, bytes);
    assert(session->state().phase == SessionPhase::SetupInProgress);
    padding.receive({1});
    assert(fake::connection->close_code == SessionCloseErrorCode::ProtocolViolation);
    assert(ready.wait_for(0ms) == std::future_status::ready);
    assert(!session->state().negotiated_version);
  }
  {
    std::unique_ptr<Publisher> session = Publisher::connect({});
    setup();
    session->register_track({{"test"}, "track", {}});
    fake::Stream &request = fake::connection->peer_stream(false);
    ByteBuffer bytes = encode_subscribe(1, {{"test"}, "track", {}});
    const ByteBuffer update = encode_request_update(3, {{Parameter::uint8(kParameterForward, 0)}});
    bytes.insert(bytes.end(), update.begin(), update.end());
    request.receive(bytes);
    const ControlMessageResult first = read_control_message(request.sent);
    assert(first.message.type == kMessageSubscribeOk);
    assert(read_control_message(request.sent, first.bytes).message.type == kMessageRequestOk);
    session->unregister_track({"test"}, "track");
    assert(session->state().active_subscriptions == 0);
  }
  {
    std::unique_ptr<Subscriber> session = Subscriber::connect({});
    setup();
    struct CancellingHandler : Handler {
      Subscriber &session;
      explicit CancellingHandler(Subscriber &session) : session(session) {}
      void on_object(const Object &object) override {
        Handler::on_object(object);
        session.stop_subscription(object.request_id);
      }
    };
    std::shared_ptr<CancellingHandler> handler = std::make_shared<CancellingHandler>(*session);
    std::future<Subscription> pending = session->subscribe({{"test"}, "track", {}}, handler);
    fake::connection->callbacks.datagram_received(
        encode_object_datagram(7, 0, 0, kDefaultPublisherPriority, {}, {}, ByteBuffer{42}, false));
    fake::connection->streams.back()->receive(encode_subscribe_ok(7, {}, {}));
    assert(pending.wait_for(0ms) == std::future_status::ready);
    bool rejected = false;
    try {
      (void)pending.get();
    } catch (const RequestRejected &) {
      rejected = true;
    }
    assert(rejected && session->state().active_subscriptions == 0 && handler->ids.size() == 1);
  }
}

void lifetime_tests() {
  // Destruction settles SETUP and SUBSCRIBE futures before draining stream callbacks.
  {
    std::unique_ptr<Subscriber> session = Subscriber::connect({});
    std::future<void> ready = session->ready();
    session.reset();
    assert(fake::connection == nullptr && ready.wait_for(0ms) == std::future_status::ready);
    bool rejected = false;
    try {
      ready.get();
    } catch (const std::runtime_error &) {
      rejected = true;
    }
    assert(rejected);
  }
  {
    std::unique_ptr<Subscriber> session = Subscriber::connect({});
    setup();
    std::shared_ptr<Handler> handler = std::make_shared<Handler>();
    std::future<Subscription> pending = session->subscribe({{"test"}, "track", {}}, handler);
    fake::connection->peer_stream(true).receive({0xff}); // incomplete stream type
    session.reset();
    assert(fake::connection == nullptr && pending.wait_for(0ms) == std::future_status::ready);
    bool rejected = false;
    try {
      (void)pending.get();
    } catch (const RequestRejected &) {
      rejected = true;
    }
    assert(rejected && handler->errors == 1);
  }
  // An active data receiver and an unanswered update may outlive their request-map entry.
  {
    std::unique_ptr<Subscriber> session = Subscriber::connect({});
    setup();
    std::shared_ptr<Handler> handler = std::make_shared<Handler>();
    std::future<Subscription> pending = session->subscribe({{"test"}, "track", {}}, handler);
    fake::connection->streams.back()->receive(encode_subscribe_ok(7, {}, {}));
    std::future<RequestOk> update = session->request_update(pending.get().request_id, {});
    ByteBuffer data;
    encode_subgroup_header(data, 7, 1, 0, kDefaultPublisherPriority);
    encode_subgroup_object(data, 0, {}, {}, ByteBuffer{1, 2});
    data.pop_back();
    fake::connection->peer_stream(true).receive(data);
    session.reset();
    assert(fake::connection == nullptr && update.wait_for(0ms) == std::future_status::ready);
    bool rejected = false;
    try {
      (void)update.get();
    } catch (const std::runtime_error &) {
      rejected = true;
    }
    assert(rejected && handler->errors == 1 && handler->ids.empty());
  }
  {
    std::unique_ptr<Publisher> session = Publisher::connect({});
    setup();
    session->register_track({{"test"}, "track", {}});
    fake::connection->peer_stream(false).receive(encode_subscribe(1, {{"test"}, "track", {}}));
    PublishedObject object;
    object.track_namespace = {"test"};
    object.track_name = "track";
    object.payload = {1, 2};
    session->publish(object);
    fake::connection->peer_stream(false).receive({3}); // incomplete peer request
    session.reset();
    assert(fake::connection == nullptr);
  }
}

int main() {
  codec_tests();
  subscriber_tests();
  track_properties_tests();
  object_delivery_timeout_options_tests();
  subgroup_object_timeout_tests();
  publisher_tests();
  setup_buffering_tests();
  edge_tests();
  lifetime_tests();
}

#include "fake_transport.h"
#include "moq/codec.h"
#include "moq/publisher_session.h"
#include "moq/subscriber_session.h"
#include <cassert>
#include <chrono>
#include <limits>
#include <random>
using namespace moq;
using namespace moq::codec;
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
  std::mt19937_64 random(18);
  for (unsigned i = 0; i < 10000; ++i) {
    uint64_t value = i < 64 ? (uint64_t{1} << i) - 1 : random();
    ByteBuffer bytes;
    write_varint(bytes, value);
    auto result = read_varint(bytes);
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
  auto framed = encode_subscribe(4, {{"a", "b"}, "c", {Parameter::uint8(kParameterForward, 0)}});
  for (size_t n = 0; n < framed.size(); ++n) {
    assert(read_control_message(ByteBuffer(framed.begin(), framed.begin() + n)).status == DecodeStatus::NeedMoreData);
  }
  auto subscribe = decode_subscribe(read_control_message(framed).message.payload, error);
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
  auto session = Subscriber::connect({});
  auto ready = session->ready();
  assert(ready.wait_for(0ms) == std::future_status::timeout);
  setup();
  ready.get();
  assert(session->state().phase == SessionPhase::Ready);
  auto handler = std::make_shared<Handler>();
  auto pending = session->subscribe({{"test"}, "track", {}}, handler);
  auto &request = *fake::connection->streams.back();
  // A datagram can precede SUBSCRIBE_OK; it is replayed when the alias is installed.
  fake::connection->callbacks.datagram_received(encode_object_datagram(7, 0, 0, 128, {}, {}, ByteBuffer{42}, false));
  fragmented(request, encode_subscribe_ok(7, {}, {}));
  auto handle = pending.get();
  assert(handle.track_alias == 7 && handler->ids == std::vector<ObjectId>{0});
  const auto state = session->subscription_state(handle.request_id);
  assert(state.phase == SubscriptionPhase::Established && state.request_id == handle.request_id &&
         state.track_alias == handle.track_alias);
  assert(session->state().active_subscriptions == 1);
  ByteBuffer data;
  encode_subgroup_header(data, 7, 1, 0, 128);
  encode_subgroup_object(data, 0, {}, {}, ByteBuffer{1, 2});
  encode_subgroup_object(data, 2, {}, {}, ByteBuffer{3, 4});
  fragmented(fake::connection->peer_stream(true), data, true);
  assert(handler->ids == (std::vector<ObjectId>{0, 0, 3}));
  assert(handler->payloads.back() == (ByteBuffer{3, 4}));
  auto update = session->request_update(handle.request_id, {});
  assert(session->subscription_state(handle.request_id).inflight_updates == 1);
  fragmented(request, encode_request_ok({}));
  (void)update.get();
  assert(session->subscription_state(handle.request_id).inflight_updates == 0);
  auto failed = session->request_update(handle.request_id, {});
  auto rejected = encode_request_error(RequestErrorCode::NotSupported, "no");
  auto done = encode_publish_done(static_cast<uint64_t>(PublishDoneCode::UpdateFailed), 1, "");
  rejected.insert(rejected.end(), done.begin(), done.end());
  request.receive(rejected, true);
  bool threw = false;
  try {
    (void)failed.get();
  } catch (const RequestRejected &) {
    threw = true;
  }
  assert(threw && handler->done == 1 && session->subscription_state(handle.request_id).phase == SubscriptionPhase::Terminated);
  assert(session->state().active_subscriptions == 0);
  auto pending2 = session->subscribe({{"test"}, "other", {}}, handler);
  fake::connection->streams.back()->receive({4, 0}, true);
  assert(pending2.wait_for(0ms) == std::future_status::ready);
  threw = false;
  try {
    (void)pending2.get();
  } catch (const RequestRejected &) {
    threw = true;
  }
  assert(threw);
  auto pending3 = session->subscribe({{"test"}, "last", {}}, handler);
  fake::connection->streams.back()->receive(encode_subscribe_ok(0, {}, {}));
  auto last = pending3.get();
  assert(last.request_id != handle.request_id && last.track_alias == 0);
  assert(session->subscription_state(last.request_id).track_alias == 0);
  fake::connection->fail_send = true;
  auto unsent = session->request_update(last.request_id, {});
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

void publisher_tests() {
  auto session = Publisher::connect({});
  session->register_track({{"test"}, "track", {}});
  setup();
  session->ready().get();
  assert(read_control_message(fake::connection->streams.back()->sent).message.type == kMessagePublishNamespace);
  auto &request = fake::connection->peer_stream(false);
  fragmented(request, encode_subscribe(1, {{"test"}, "track", {}}));
  assert(read_control_message(request.sent).message.type == kMessageSubscribeOk);
  assert(session->state().active_subscriptions == 1);
  PublishedObject object;
  object.track_namespace = {"test"};
  object.track_name = "track";
  object.payload = {1, 2};
  session->publish(object);
  auto &data = *fake::connection->streams.back();
  ByteBuffer expected;
  encode_subgroup_header(expected, 0, 0, 0, 128);
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
  auto error = read_control_message(request.sent);
  assert(error.message.type == kMessageRequestError);
  assert(read_control_message(request.sent, error.bytes).message.type == kMessagePublishDone);
  assert(request.fin && !data.shutdowns.empty() && session->state().active_subscriptions == 0);
  auto &duplicate = fake::connection->peer_stream(false);
  duplicate.receive(encode_subscribe(1, {{"test"}, "track", {}}));
  assert(fake::connection->close_code == SessionCloseErrorCode::InvalidRequestId);
}
void edge_tests() {
  {
    auto session = Subscriber::connect({});
    auto ready = session->ready();
    auto &padding = fake::connection->peer_stream(true);
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
    auto session = Publisher::connect({});
    setup();
    session->register_track({{"test"}, "track", {}});
    auto &request = fake::connection->peer_stream(false);
    auto bytes = encode_subscribe(1, {{"test"}, "track", {}});
    const auto update = encode_request_update(3, {{Parameter::uint8(kParameterForward, 0)}});
    bytes.insert(bytes.end(), update.begin(), update.end());
    request.receive(bytes);
    const auto first = read_control_message(request.sent);
    assert(first.message.type == kMessageSubscribeOk);
    assert(read_control_message(request.sent, first.bytes).message.type == kMessageRequestOk);
    session->unregister_track({"test"}, "track");
    assert(session->state().active_subscriptions == 0);
  }
  {
    auto session = Subscriber::connect({});
    setup();
    struct CancellingHandler : Handler {
      Subscriber &session;
      explicit CancellingHandler(Subscriber &session) : session(session) {}
      void on_object(const Object &object) override {
        Handler::on_object(object);
        session.stop_subscription(object.request_id);
      }
    };
    auto handler = std::make_shared<CancellingHandler>(*session);
    auto pending = session->subscribe({{"test"}, "track", {}}, handler);
    fake::connection->callbacks.datagram_received(encode_object_datagram(7, 0, 0, 128, {}, {}, ByteBuffer{42}, false));
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
    auto session = Subscriber::connect({});
    auto ready = session->ready();
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
    auto session = Subscriber::connect({});
    setup();
    auto handler = std::make_shared<Handler>();
    auto pending = session->subscribe({{"test"}, "track", {}}, handler);
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
    auto session = Subscriber::connect({});
    setup();
    auto handler = std::make_shared<Handler>();
    auto pending = session->subscribe({{"test"}, "track", {}}, handler);
    fake::connection->streams.back()->receive(encode_subscribe_ok(7, {}, {}));
    auto update = session->request_update(pending.get().request_id, {});
    ByteBuffer data;
    encode_subgroup_header(data, 7, 1, 0, 128);
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
    auto session = Publisher::connect({});
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
  publisher_tests();
  edge_tests();
  lifetime_tests();
}

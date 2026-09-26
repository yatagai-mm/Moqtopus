#pragma once

#include "moq/object_handler.h"
#include "moq/session.h"
#include "moq/types.h"

#include <future>
#include <memory>
#include <unordered_map>

namespace moq {

enum class UnknownAliasPolicy {
  Drop,
  Buffer,
  // Backward-compatible name from when only datagrams were buffered.
  BufferDatagrams = Buffer,
  Error,
};

struct SubscriberConfig {
  UnknownAliasPolicy unknown_alias_policy = UnknownAliasPolicy::Buffer;
  size_t max_buffered_datagrams_per_alias = 16;
  size_t max_buffered_datagram_bytes = 256 * 1024;
  // Draft-18 Section 11.4.2 permits briefly buffering a Subgroup stream
  // until SUBSCRIBE_OK/PUBLISH establishes its Track Alias.
  size_t max_pending_subgroup_streams = 64;
  size_t max_buffered_subgroup_bytes_per_stream = 1024 * 1024;
};

class DataPlane;

struct Subscription {
  RequestId request_id = 0;
  TrackAlias track_alias = 0;
};

class Subscriber final : private Session {
public:
  ~Subscriber() override;

  static std::unique_ptr<Subscriber> connect(MsQuicClientConfig msquic_config, SubscriberConfig subscriber_config = {});

  using Session::close;
  using Session::ready;
  using Session::state;

  std::future<Subscription> subscribe(SubscribeRequest request, std::shared_ptr<ObjectHandler> handler);
  SubscriptionStateSnapshot subscription_state(RequestId request_id) const;
  std::future<RequestOk> request_update(RequestId existing_request_id, RequestUpdate update);
  void stop_subscription(RequestId request_id);

private:
  class SubscriptionRequest;
  Subscriber(MsQuicClientConfig msquic_config, SubscriberConfig subscriber_config);
  void handle_data_stream(uint64_t type, const std::shared_ptr<StreamContext> &stream, ByteBuffer prefix,
                          bool fin) override;
  void handle_peer_request(const ControlMessage &message, const std::shared_ptr<StreamContext> &stream,
                           ByteBuffer leftover, bool fin) override;
  void stop_subscription_now(RequestId request_id, std::string reason, uint64_t stream_error_code = 0);
  void on_datagram(BytesView bytes) override;
  void terminate_subscriptions(const std::string &reason) override;
  size_t active_subscriptions() const override;

  std::unique_ptr<DataPlane> data_plane_;
  std::unordered_map<RequestId, std::shared_ptr<SubscriptionRequest>> subscriptions_;
};

} // namespace moq

#pragma once

#include "moq/client_config.h"
#include "moq/errors.h"
#include "moq/object_handler.h"
#include "moq/types.h"

#include <future>
#include <memory>
#include <utility>

namespace moq {

enum class UnknownAliasPolicy {
  Drop,
  BufferDatagrams,
  Error,
};

struct SubscriberConfig {
  UnknownAliasPolicy unknown_alias_policy = UnknownAliasPolicy::BufferDatagrams;
  size_t max_buffered_datagrams_per_alias = 16;
  size_t max_buffered_datagram_bytes = 256 * 1024;
};

namespace detail {
class Subscriber;
}

struct Subscription {
  RequestId request_id = 0;
  TrackAlias track_alias = 0;
};

class Subscriber {
public:
  ~Subscriber();

  Subscriber(const Subscriber &) = delete;
  Subscriber &operator=(const Subscriber &) = delete;

  static std::unique_ptr<Subscriber> connect(MsQuicClientConfig msquic_config, SubscriberConfig subscriber_config = {});

  std::future<void> ready();
  SessionStateSnapshot state() const;

  std::future<Subscription> subscribe(SubscribeRequest request, std::shared_ptr<ObjectHandler> handler);
  SubscriptionStateSnapshot subscription_state(RequestId request_id) const;
  std::future<RequestOk> request_update(RequestId existing_request_id, RequestUpdate update);
  void stop_subscription(RequestId request_id);
  void close(SessionCloseErrorCode error = SessionCloseErrorCode::NoError);

private:
  explicit Subscriber(std::shared_ptr<detail::Subscriber> impl) : impl_(std::move(impl)) {}

  std::shared_ptr<detail::Subscriber> impl_;
};

} // namespace moq

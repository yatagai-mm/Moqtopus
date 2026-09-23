# Subscriber API

`moq::Subscriber` connects to a relay, subscribes to named tracks, and delivers received objects to an application-provided handler. Include `moq/subscriber_session.h`; the class declaration is in [subscriber_session.h][subscriber-source].

The public methods below include `ready()`, `state()`, and `close()`, which are exposed from [Session][session-source]. Create a subscriber with `connect()` and wait for `ready().get()` before subscribing.

## Public methods

### `connect`

```cpp
static std::unique_ptr<Subscriber> connect(
    MsQuicClientConfig msquic_config, SubscriberConfig subscriber_config = {});
```

Creates a subscriber and starts connecting using [MsQuicClientConfig][client-config] and [SubscriberConfig][subscriber-config].
Returns before MoQ setup completes; use `ready().get()` to wait, and catch exceptions from connection initialization or the future.

### `ready`

```cpp
std::future<void> ready();
```

Returns a future that completes once the local SETUP has been sent and the peer SETUP has been received.
Calling `.get()` throws if the session closes before it becomes ready.

### `state`

```cpp
SessionStateSnapshot state() const;
```

Returns a [SessionStateSnapshot][session-state] containing the phase, setup progress, negotiated version, subscription count, and close reason.
This is a copy of the current state; call again to observe subsequent changes.

### `subscribe`

```cpp
std::future<Subscription> subscribe(SubscribeRequest request,
                                    std::shared_ptr<ObjectHandler> handler);
```

Sends a [SubscribeRequest][subscribe-request] and installs a non-null [ObjectHandler][object-handler] to receive data and notifications.
The future returns a [Subscription][subscription] with `request_id` and `track_alias` after SUBSCRIBE_OK, or throws on rejection/failure; the session must already be ready.

### `subscription_state`

```cpp
SubscriptionStateSnapshot subscription_state(RequestId request_id) const;
```

Returns a [SubscriptionStateSnapshot][subscription-state] with the phase, assigned alias, and number of outstanding updates for the original subscription's [RequestId][request-id].
An unknown or removed subscription returns `Terminated`, an empty alias, and zero outstanding updates.

### `request_update`

```cpp
std::future<RequestOk> request_update(RequestId existing_request_id,
                                      RequestUpdate update);
```

Sends a [RequestUpdate][request-update] for an established subscription using its original [RequestId][request-id]; Moqtopus allocates the update's wire request ID internally.
The future returns [RequestOk][request-ok] on acceptance or throws on rejection/failure; a rejected update leaves the subscription in `UpdateFailed` while awaiting termination.

### `stop_subscription`

```cpp
void stop_subscription(RequestId request_id);
```

Stops the subscription identified by its original [RequestId][request-id], removes its receive route, and cancels its request stream.
Pending futures fail, an unknown ID is a no-op, and local cancellation does not generate an `on_publish_done()` callback.

### `close`

```cpp
void close(SessionCloseErrorCode error = SessionCloseErrorCode::NoError);
```

Initiates session shutdown and terminates its subscriptions without waiting for transport shutdown to finish; repeated calls are harmless.
[SessionCloseErrorCode][session-close-code] contains protocol error values; for details, see [MoQT draft-18, Section 3.5][draft-close].

### `~Subscriber`

```cpp
~Subscriber() override;
```

Closes the session if necessary and waits for transport callbacks to finish before releasing resources.
Destroy the returned `std::unique_ptr` from the application thread, after your callback work has completed.

## Configuration and request parameters

[MsQuicClientConfig][client-config] requires `host` and `port`. Defaults are `path = "/"`, an idle timeout of 30 seconds, and `disable_certificate_validation = true`; an empty `authority` uses `host:port`. The ALPN is fixed to `moqt-18`.

[SubscriberConfig][subscriber-config] controls data received before its Track Alias has been associated with a subscription. Its [UnknownAliasPolicy][alias-policy] is a Moqtopus option:

| Policy | Behavior |
| --- | --- |
| `Drop` | Discard unknown-alias Datagrams and abort unknown-alias Subgroup streams. |
| `BufferDatagrams` (default) | Buffer unknown-alias Datagrams and replay them when the alias is registered; unknown-alias Subgroup streams are still aborted. |
| `Error` | Treat an unknown alias as a session protocol error. |

The default buffer limits are 16 Datagrams per alias and 256 KiB in total. Datagrams exceeding either limit are dropped.

[SubscribeRequest][subscribe-request] identifies a track through `track_namespace` and `track_name`. Both it and [RequestUpdate][request-update] accept a vector of [Parameter][parameter] values; the factory helpers encode a byte, varint, location, length-prefixed value, or namespace. Parameter identifiers are declared in [codec.h][parameter-ids]; for their protocol meanings, see [MoQT draft-18, Section 10.2][draft-parameters], including [subscription filters in Section 10.2.9][draft-filter] and [Forward in Section 10.2.12][draft-forward]. An empty parameter vector sends no explicit overrides.

Peer rejection is reported through the future as [RequestRejected][request-rejected], which exposes `code()`, `retry_interval()`, and `reason()`. Its [RequestErrorCode][request-error-code] values follow the protocol; see [MoQT draft-18, Section 10.6][draft-request-error] for details. Local failures can also produce other standard exceptions.

## Receiving objects

Derive from [ObjectHandler][object-handler] and implement all three callbacks:

| Callback | Role |
| --- | --- |
| `void on_object(const Object &object)` | Consume the received [Object][object], including its IDs, delivery kind, status, properties, and payload. |
| `void on_publish_done(PublishDone done)` | Handle a peer's [PublishDone][publish-done] notification; status meanings are defined in [MoQT draft-18, Section 10.11][draft-publish-done]. |
| `void on_error(ReceiveError error)` | Handle a local receive/subscription error described by [ReceiveError][receive-error]. |

`on_object()` runs synchronously on a transport callback thread. Its [BytesView][bytes-view] payload and properties expire when the callback returns; use `object.payload.to_owned()` or `object.properties.to_owned()` to retain bytes. Keep callbacks short and do not wait for network-dependent futures inside them. Error notifications may also occur synchronously during local shutdown.

Callbacks can begin before the application retrieves the successful `subscribe()` future. Keep handler state ready before calling `subscribe()`; receiving an object must not depend on having already stored the returned `Subscription`.

[DeliveryKind][delivery-kind] distinguishes Subgroup streams and Datagrams. Although the shared enum includes `FetchStream`, this Subscriber currently aborts incoming FETCH streams. For wire-level object status meanings, see [MoQT draft-18, Section 11.2.1.1][draft-object-status].

[subscriber-source]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/subscriber_session.h#L32
[session-source]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/session.h#L22
[client-config]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/client_config.h#L12
[subscriber-config]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/subscriber_session.h#L19
[alias-policy]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/subscriber_session.h#L13
[subscription]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/subscriber_session.h#L27
[subscribe-request]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L54
[request-update]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L60
[request-ok]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L64
[request-id]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L12
[parameter]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L43
[parameter-ids]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/codec.h#L34
[session-state]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L123
[subscription-state]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L132
[session-close-code]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/errors.h#L9
[request-error-code]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/errors.h#L19
[request-rejected]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/errors.h#L73
[object-handler]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/object_handler.h#L8
[object]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L89
[publish-done]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L75
[receive-error]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/errors.h#L68
[bytes-view]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L24
[delivery-kind]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L81
[draft-close]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-3.5
[draft-parameters]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-10.2
[draft-filter]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-10.2.9
[draft-forward]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-10.2.12
[draft-request-error]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-10.6
[draft-publish-done]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-10.11
[draft-object-status]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-11.2.1.1

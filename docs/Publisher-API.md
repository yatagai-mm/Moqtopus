# Publisher API

`moq::Publisher` connects to a relay, registers tracks, and sends objects to peers that subscribe to those tracks. Include `moq/publisher_session.h`. The class declaration is in [publisher_session.h][publisher-source].

The public methods below include `ready()`, `state()`, and `close()`, which are exposed from [Session][session-source]. Create a publisher with `connect()` and wait for `ready().get()` before publishing.

## Public methods

### `connect`

```cpp
static std::unique_ptr<Publisher> connect(
    MsQuicClientConfig msquic_config, PublisherConfig publisher_config = {});
```

Creates a publisher and starts connecting using [MsQuicClientConfig][client-config] and [PublisherConfig][publisher-config].
Returns before MoQ setup completes. Use `ready().get()` to wait, and catch exceptions from connection initialization or the future.

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
This is a copy of the current state. Call again to observe subsequent changes.

### `register_track`

```cpp
void register_track(PublishedTrack track);
```

Registers a [PublishedTrack][published-track] and announces its namespace when the session is ready, allowing incoming subscriptions to the track.
Duplicate registrations and namespaces whose first field is `"."` are ignored with a warning. This call does not wait for the peer to acknowledge the announcement.

Track properties could be set:
```cpp
moq::PublishedTrack track;
track.track_namespace = {"example"};
track.track_name = "video";
track.track_properties.object_delivery_timeout = 100; // milliseconds
track.track_properties.max_cache_duration = 1000;
track.track_properties.default_publisher_group_order = moq::GroupOrder::Descending;
publisher->register_track(std::move(track));
```

Set properties through `PublishedTrack::track_properties`. For immutable values,
use `track_properties.immutable_properties.emplace()` and set fields on the
resulting `TrackPropertyValues`. Moqtopus retains the typed values and encodes
them internally. Property semantics are defined in [draft-18 Section 12](https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-12).

Invalid group-order values or duplicate mutable/immutable properties throw
`std::invalid_argument` before the track is registered. This API supplies
metadata. Local delivery-timeout enforcement, priority scheduling, and handling
of dynamic group requests are not yet implemented. Subscriber-side decoded
properties remain raw bytes.

### `unregister_track`

```cpp
void unregister_track(const TrackNamespace &track_namespace,
                      const TrackName &track_name);
```

Ends current subscriptions and removes the track identified by [TrackNamespace and TrackName][track-types].
Also withdraws its namespace announcement if no other registered track uses that namespace. An unknown track is a no-op.

### `publish`

```cpp
void publish(PublishedObject object);
```

Sends a [PublishedObject][published-object] to established subscriptions whose forwarding settings and requested ranges allow it.
Processes the object synchronously and queues transport sends. Returning does not confirm delivery, and objects are not retained for later subscribers.

### `end_track`

```cpp
void end_track(const TrackNamespace &track_namespace, const TrackName &track_name,
               PublishDoneCode code = PublishDoneCode::TrackEnded,
               std::string reason = {});
```

Ends current subscriptions for the specified [track][track-types], finishing their data streams and sending PUBLISH_DONE with `code` and `reason`. The track stays registered.
[PublishDoneCode][publish-done-code] contains protocol status values. For their meaning, see [MoQT draft-18, Section 10.11][draft-publish-done].

### `close`

```cpp
void close(SessionCloseErrorCode error = SessionCloseErrorCode::NoError);
```

Initiates session shutdown and terminates its subscriptions without waiting for transport shutdown to finish. Repeated calls are harmless.
[SessionCloseErrorCode][session-close-code] contains protocol error values. For details, see [MoQT draft-18, Section 3.5][draft-close].

### `~Publisher`

```cpp
~Publisher() override;
```

Closes the session if necessary and waits for transport callbacks to finish before releasing resources.
The `std::unique_ptr` returned by `connect()` calls this automatically when destroyed.

## Configuration and object fields

| Type | Usage |
| --- | --- |
| [MsQuicClientConfig][client-config] | Set `host` and `port`. Defaults are `path = "/"`, an idle timeout of 30 seconds, and `disable_certificate_validation = true`. An empty `authority` uses `host:port`. The ALPN is fixed to `moqt-18`. |
| [PublisherConfig][publisher-config] | `max_subscriptions` limits concurrent accepted subscriptions and defaults to 128. |
| [PublishedTrack][published-track] | Set `track_namespace` and `track_name`. The `track_properties` field is a typed `TrackProperties` value. Moqtopus encodes it internally. |
| [PublishedObject][published-object] | Set the track identity, group/object IDs, and payload. Defaults include subgroup ID 0, priority 128, and Subgroup stream delivery. |
| [DeliveryKind][delivery-kind] | Choose `SubgroupStream` or `Datagram`. `FetchStream` exists in the shared enum but publishing with it is unsupported and the object is dropped. |

For Subgroup streams, `end_of_subgroup` finishes the stream after the object. `end_of_group` also signals group completion. For Datagrams it marks the last object in the group. Object IDs must increase within an open Subgroup stream. An out-of-order ID causes that stream to be reset and replaced.

Leave `status` unset for ordinary payload data. The [object status constants][object-status] are protocol values. See [MoQT draft-18, Section 11.2.1.1][draft-object-status] for details. Object properties remain encoded bytes. Their wire format is described in [Section 2.5][draft-properties].

Calling `publish()` before readiness, for an unregistered track, or with an invalid status/delivery combination drops the object with a warning. The method has no delivery-result return value, and a Datagram rejected by the transport is also dropped.

[publisher-source]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/publisher_session.h#L46
[session-source]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/session.h#L22
[client-config]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/client_config.h#L12
[publisher-config]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/publisher_session.h#L16
[published-track]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/publisher_session.h#L20
[published-object]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/publisher_session.h#L26
[track-types]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L18
[session-state]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L123
[session-close-code]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/errors.h#L9
[publish-done-code]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/errors.h#L41
[delivery-kind]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/types.h#L81
[object-status]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/codec.h#L49
[draft-close]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-3.5
[draft-publish-done]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-10.11
[draft-object-status]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-11.2.1.1
[draft-properties]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-18.html#section-2.5

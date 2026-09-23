# Example

The programs in [example/publisher.cpp][publisher-source] and [example/subscriber.cpp][subscriber-source] demonstrate publishing a text track and receiving its objects. Both are clients: run them against a MoQT draft-18 relay that supports namespace announcements and subscriptions.

## Build and run

From the repository root, configure with vcpkg and build both example targets:

```sh
export VCPKG_ROOT=/path/to/vcpkg
cmake --preset=vcpkg -DMOQTOPUS_BUILD_EXAMPLES=ON
cmake --build build --target publisher subscriber
```

With a relay listening on `localhost:4433`, start the publisher in one terminal:

```sh
./build/publisher localhost 4433 camera/front video / stream
```

After the publisher has connected and registered its track, start the subscriber in another terminal:

```sh
./build/subscriber localhost 4433 camera/front video /
```

Replace the host, port, and path with your relay's settings. Both commands must use the same track namespace and name; `camera/front` becomes the two namespace fields `{"camera", "front"}`, while `video` is the track name. The publisher does not store objects for later subscribers, so reception starts with objects forwarded after the subscription is established, subject to the relay's behavior.

To use Datagrams instead, replace the publisher's last argument:

```sh
./build/publisher localhost 4433 camera/front video / datagram
```

The subscriber handles either delivery mode without an additional argument.

### Command-line arguments

```text
publisher  <host> <port> <namespace[/field...]> <track-name> [path] [stream|datagram]
subscriber <host> <port> <namespace[/field...]> <track-name> [path]
```

`path` defaults to `/`, and the publisher defaults to Subgroup streams. Because arguments are positional, supply a path before specifying `datagram`. The current publisher selects Datagram mode only for the exact string `datagram`; other mode strings follow the stream path.

Both examples use [arguments.h][arguments-source] to parse the port and split namespace fields. `LOG_LEVEL` controls spdlog verbosity and defaults to `debug`; for example:

```sh
LOG_LEVEL=info ./build/publisher localhost 4433 camera/front video / stream
```

Object output from the subscriber goes to `std::cout`, so reducing `LOG_LEVEL` does not suppress it. Both programs use the defaults in [MsQuicClientConfig][client-config], including `disable_certificate_validation = true`.

## Publisher example

1. **Read connection and track settings.** `main()` fills `MsQuicClientConfig`, parses the namespace and track name, and selects `DeliveryKind::SubgroupStream` or `DeliveryKind::Datagram`.
2. **Connect and wait.** `Publisher::connect(client_config)` starts the connection, and `session->ready().get()` waits for MoQ setup before proceeding.
3. **Register the track.** A `PublishedTrack` containing the namespace and name is passed to `register_track()`. Moqtopus announces the namespace and accepts matching incoming subscriptions.
4. **Generate objects.** The loop creates a `PublishedObject` with payload text such as `moqtopus object 0/0`, then calls `publish(std::move(object))`. It sleeps for 100 ms between iterations.
5. **Advance groups.** Each group contains 30 application-generated objects with IDs 0 through 29. The last one sets `end_of_group = true`; the next iteration increments `group_id` and resets `object_id` to 0. Subgroup ID remains at its default of 0.
6. **Shut down.** SIGINT or SIGTERM sets the `interrupted` flag. The loop exits, calls `end_track()` to end current subscriptions, then calls `close()`; destruction waits for transport cleanup.

The central publishing code is:

```cpp
moq::PublishedObject object;
object.track_namespace = track_namespace;
object.track_name = track_name;
object.group_id = group_id;
object.object_id = object_id;
object.delivery_kind = use_datagrams
    ? moq::DeliveryKind::Datagram : moq::DeliveryKind::SubgroupStream;
const std::string text = "moqtopus object " + std::to_string(group_id)
    + "/" + std::to_string(object_id);
object.payload.assign(text.begin(), text.end());
object.end_of_group = object_id + 1 == kObjectsPerGroup;
session->publish(std::move(object));
```

The publisher's final `published` count counts calls to `publish()`, including calls made before any peer subscribed. It is not a delivered-object count.

## Subscriber example

1. **Connect and wait.** `main()` constructs the connection configuration, calls `Subscriber::connect()`, and waits on `ready().get()`.
2. **Describe the track.** A `SubscribeRequest` receives the parsed namespace and track name. Its parameter vector is left empty, so the example supplies no explicit subscription overrides.
3. **Install the handler and subscribe.** A shared `PrintingHandler` is passed to `subscribe()`. Calling `.get()` waits for acceptance and returns a `Subscription`; the example prints its Request ID and Track Alias.
4. **Receive objects.** `PrintingHandler::on_object()` increments atomic counters and prints delivery mode, IDs, and either object status or a payload preview. `PayloadPreview()` shows up to 24 bytes, escaping non-printable bytes as `\xNN`.
5. **Handle completion or failure.** `on_publish_done()` prints the status, stream count, and reason; `on_error()` logs an error. Both set the handler's atomic stop flag, which the main loop checks every 100 ms.
6. **Clean up.** When the loop exits, the example calls `stop_subscription(subscription.request_id)` and `close()`, then prints the object and payload-byte counters.

The subscription setup is:

```cpp
moq::SubscribeRequest request;
request.track_namespace = ParseNamespace(argv[3]);
request.track_name = argv[4];

auto handler = std::make_shared<PrintingHandler>();
const moq::Subscription subscription =
    session->subscribe(std::move(request), handler).get();
```

`PrintingHandler` uses the payload only inside `on_object()`, while the receive buffer is valid. If you adapt it to queue objects for another thread, copy the payload and properties with `to_owned()` before returning from the callback.

The received-object count includes status objects. In stream mode, the publisher can emit an additional End-of-Group status object, so the subscriber's count need not match the publisher's application-generated object count.

The current subscriber source declares an `interrupted` flag but does not register signal handlers. Its normal cleanup path is reached through the handler's stop flag; Ctrl-C uses the process's default signal behavior rather than the publisher's flag-based shutdown path.

[publisher-source]: https://github.com/yatagai-mm/Moqtopus/blob/main/example/publisher.cpp
[subscriber-source]: https://github.com/yatagai-mm/Moqtopus/blob/main/example/subscriber.cpp
[arguments-source]: https://github.com/yatagai-mm/Moqtopus/blob/main/example/arguments.h
[client-config]: https://github.com/yatagai-mm/Moqtopus/blob/main/include/moq/client_config.h#L12

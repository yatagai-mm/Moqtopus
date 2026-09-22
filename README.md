# Moqtopus
![moqtopus oqtopus](/static/moqtopus.png)
MoQ Client API over MsQUIC. Moqtopus keeps the dependency graph small, which helps a lot when the rest of your application already take a gazillion year building (like UE).

## Draft Compatibility
Moqtopus was compliant with draft-18, and interopped with 4 relays at [IETF126](https://github.com/moq-wg/moq-transport/wiki/ad-hoc-interop-reports#2026-07-ietf-126-vienna). See [moq-interop-runner](https://englishm.github.io/moq-interop-runner/) for the latest interop result.

## Build
Moqtopus requires [vcpkg](https://github.com/microsoft/vcpkg). Set
`VCPKG_ROOT` to the vcpkg checkout before configuring the project.

```sh
export VCPKG_ROOT=/path/to/vcpkg
cmake --preset=vcpkg
cmake --build build
```

## Use from CMake

Once installed, Moqtopus provides a CMake config package:

```cmake
find_package(moqtopus CONFIG REQUIRED)
target_link_libraries(your_target PRIVATE moqtopus::moq_subscriber)
```

To install a local checkout:

```sh
cmake --preset=vcpkg -DMOQTOPUS_BUILD_EXAMPLES=OFF -DMOQTOPUS_BUILD_TOOLS=OFF
cmake --build build
cmake --install build --prefix /path/to/prefix
```

## Run the Example

The subscriber example accepts connection details on the command line and
prints received object metadata and a payload preview until the publisher
finishes or the process receives SIGINT:

```sh
cmake --build build --target subscriber
./build/subscriber <host> <port> <namespace[/field...]> <track-name> [path]
```

Example:

```sh
./build/subscriber localhost 4433 camera/front video /
```

## Subscriber flow
![sub](./static/data-flow-new.svg)

## API simplification

`connect()` now returns a `std::unique_ptr` directly and throws on startup failure;
remove `.get()` from factory calls. Continue to wait on `ready().get()` for SETUP.
`stop_subscription()` is synchronous and returns `void`.

Draft-18 fixes the ALPN to `moqt-18`. `SubscribeRequest::request_id`, the unused
`PublishedTrack` priority/order defaults, and unused `SubscriptionOptions` fields
have been removed. `PublishedObject::subgroup_id` is an integer defaulting to zero.
The codec now returns frames through `encode_control_message()`;
`encode_request_ok()` takes only parameters, and `encode_publish_namespace()`
takes only the request ID and namespace.

## Tests

```sh
cmake --build build
ctest --test-dir build --output-on-failure
```

The protocol tests inject deterministic transport events into the production
session and codec code. They do not replace interop testing against a real relay.

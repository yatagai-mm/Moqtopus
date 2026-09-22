#include "arguments.h"
#include "moq/subscriber_session.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

std::atomic_bool interrupted{false};

const char *DeliveryName(moq::DeliveryKind delivery) {
  switch (delivery) {
  case moq::DeliveryKind::SubgroupStream:
    return "subgroup";
  case moq::DeliveryKind::Datagram:
    return "datagram";
  case moq::DeliveryKind::FetchStream:
    return "fetch";
  }
  return "unknown";
}

std::string PayloadPreview(moq::BytesView payload) {
  std::ostringstream text;
  constexpr size_t kMaxBytes = 24;
  for (size_t index = 0; index < payload.size && index < kMaxBytes; ++index) {
    const uint8_t byte = payload.data[index];
    if (byte >= 0x20 && byte <= 0x7e) {
      text << static_cast<char>(byte);
    } else {
      text << "\\x" << std::hex << std::setw(2) << std::setfill('0') << static_cast<unsigned>(byte) << std::dec
           << std::setfill(' ');
    }
  }
  if (payload.size > kMaxBytes) {
    text << "...";
  }
  return text.str();
}

class PrintingHandler final : public moq::ObjectHandler {
public:
  void on_object(const moq::Object &object) override {
    const uint64_t object_count = object_count_.fetch_add(1) + 1;
    total_payload_bytes_.fetch_add(object.payload.size);

    std::cout << "object #" << object_count << " delivery=" << DeliveryName(object.delivery_kind)
              << " request=" << object.request_id << " alias=" << object.track_alias << " group=" << object.group_id
              << " object=" << object.object_id;
    if (object.subgroup_id) {
      std::cout << " subgroup=" << *object.subgroup_id;
    }
    if (object.status) {
      std::cout << " status=" << *object.status;
    } else {
      std::cout << " payload=" << object.payload.size << " bytes preview=\"" << PayloadPreview(object.payload) << '"';
    }
    std::cout << '\n';
  }

  void on_publish_done(moq::PublishDone done) override {
    std::cout << "publisher finished track status=" << done.status_code << " streams=" << done.stream_count;
    if (!done.reason.empty()) {
      std::cout << " reason=\"" << done.reason << '"';
    }
    std::cout << '\n';
    stopped_.store(true);
  }

  void on_error(moq::ReceiveError error) override {
    spdlog::error("receive error code={} message=\"{}\"", error.code, error.message);
    stopped_.store(true);
  }

  bool stopped() const { return stopped_.load(); }
  uint64_t object_count() const { return object_count_.load(); }
  uint64_t total_payload_bytes() const { return total_payload_bytes_.load(); }

private:
  std::atomic_bool stopped_{false};
  std::atomic_uint64_t object_count_{0};
  std::atomic_uint64_t total_payload_bytes_{0};
};

void Usage(const char *argv0) {
  spdlog::error("usage: {} <host> <port> <namespace[/field...]> <track-name> [path]", argv0);
  spdlog::error("example: {} localhost 4433 camera/front video /", argv0);
}

} // namespace

int main(int argc, char **argv) {
  if (const char *env_level = std::getenv("LOG_LEVEL")) {
    spdlog::set_level(spdlog::level::from_str(env_level));
  } else {
    spdlog::set_level(spdlog::level::debug);
  }

  if (argc < 5 || argc > 6) {
    Usage(argv[0]);
    return 2;
  }

  try {
    moq::MsQuicClientConfig client_config;
    client_config.host = argv[1];
    if (!ParsePort(argv[2], client_config.port)) {
      spdlog::error("invalid port: {}", argv[2]);
      return 2;
    }
    client_config.path = argc >= 6 ? argv[5] : "/";

    auto session = moq::Subscriber::connect(client_config);
    session->ready().get();

    moq::SubscribeRequest request;
    request.track_namespace = ParseNamespace(argv[3]);
    request.track_name = argv[4];

    auto handler = std::make_shared<PrintingHandler>();
    const moq::Subscription subscription = session->subscribe(std::move(request), handler).get();
    std::cout << "subscribed request=" << subscription.request_id << " alias=" << subscription.track_alias << '\n';

    while (!interrupted.load() && !handler->stopped()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    session->stop_subscription(subscription.request_id);
    session->close();

    std::cout << "received objects=" << handler->object_count() << " payload_bytes=" << handler->total_payload_bytes()
              << '\n';
    return 0;
  } catch (const std::exception &error) {
    spdlog::error("subscriber failed: {}", error.what());
    return 1;
  }
}

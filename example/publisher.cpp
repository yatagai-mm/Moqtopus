#include "arguments.h"
#include "moq/publisher_session.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <string>
#include <thread>

std::atomic_bool interrupted{false};

void HandleSignal(int) { interrupted.store(true); }

void Usage(const char *argv0) {
  spdlog::error("usage: {} <host> <port> <namespace[/field...]> <track-name> [path] [stream|datagram]", argv0);
  spdlog::error("example: {} localhost 4433 camera/front video / stream", argv0);
}

int main(int argc, char **argv) {
  if (const char *env_level = std::getenv("LOG_LEVEL")) {
    spdlog::set_level(spdlog::level::from_str(env_level));
  } else {
    spdlog::set_level(spdlog::level::debug);
  }

  if (argc < 5 || argc > 7) {
    Usage(argv[0]);
    return 2;
  }

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  try {
    moq::MsQuicClientConfig client_config;
    client_config.host = argv[1];
    if (!ParsePort(argv[2], client_config.port)) {
      spdlog::error("invalid port: {}", argv[2]);
      return 2;
    }
    client_config.path = argc >= 6 ? argv[5] : "/";
    const bool use_datagrams = argc >= 7 && std::string(argv[6]) == "datagram";

    const moq::TrackNamespace track_namespace = ParseNamespace(argv[3]);
    const moq::TrackName track_name = argv[4];

    auto session = moq::Publisher::connect(client_config);
    session->ready().get();
    spdlog::info("session ready; publishing track \"{}\" via {}", track_name,
                 use_datagrams ? "datagrams" : "subgroup streams");

    moq::PublishedTrack track;
    track.track_namespace = track_namespace;
    track.track_name = track_name;
    session->register_track(track);

    // One group per 30 objects, one object every 100 ms, until interrupted.
    constexpr uint64_t kObjectsPerGroup = 30;
    moq::GroupId group_id = 0;
    moq::ObjectId object_id = 0;
    uint64_t published = 0;
    while (!interrupted.load()) {
      moq::PublishedObject object;
      object.track_namespace = track_namespace;
      object.track_name = track_name;
      object.group_id = group_id;
      object.object_id = object_id;
      object.delivery_kind = use_datagrams ? moq::DeliveryKind::Datagram : moq::DeliveryKind::SubgroupStream;
      const std::string text = "moqtopus object " + std::to_string(group_id) + "/" + std::to_string(object_id);
      object.payload.assign(text.begin(), text.end());
      object.end_of_group = object_id + 1 == kObjectsPerGroup;

      session->publish(std::move(object));
      ++published;

      if (++object_id == kObjectsPerGroup) {
        object_id = 0;
        ++group_id;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    spdlog::info("interrupted; ending track after {} objects", published);
    session->end_track(track_namespace, track_name);
    session->close();
    return 0;
  } catch (const std::exception &error) {
    spdlog::error("publisher failed: {}", error.what());
    return 1;
  }
}

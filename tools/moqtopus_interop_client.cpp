// Interface for englishm/moq-interop-runner.

#include "moq/publisher_session.h"
#include "moq/subscriber_session.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using Clock = std::chrono::steady_clock;

constexpr std::chrono::seconds kSetupTimeout{2};
constexpr std::chrono::seconds kSessionIdleTimeout{5};
constexpr std::chrono::milliseconds kAnnouncementDelay{500};
constexpr std::chrono::milliseconds kSubscriberStartDelay{300};
constexpr std::chrono::milliseconds kPublisherStartDelay{500};
constexpr std::chrono::milliseconds kWithdrawalDelay{200};
constexpr std::chrono::milliseconds kSubscribeTimeout{1500};
constexpr std::chrono::seconds kSubscribeBeforeAnnounceTimeout{2};
constexpr const char *kDefaultRelayUrl = "moqt://localhost:4443";
constexpr const char *kTestTrack = "test-track";

const moq::TrackNamespace kTestNamespace = {"moq-test", "interop"};

// https://github.com/englishm/moq-interop-runner/blob/main/docs/TEST-CLIENT-INTERFACE.md#exit-codes
constexpr int kExitSuccess = 0;
constexpr int kExitFailure = 1;
constexpr int kExitUnsupported = 127;

const std::vector<std::string> kTestNames = {
    "setup-only",      "announce-only",      "publish-namespace-done",
    "subscribe-error", "announce-subscribe", "subscribe-before-announce",
};

struct Options {
  std::string relay_url = kDefaultRelayUrl;
  std::optional<std::string> testcase;
  bool list = false;
  bool verbose = false;
  bool tls_disable_verify = false;
};

struct RelayEndpoint {
  std::string host;
  uint16_t port = 4443;
  std::string path = "/";
};

struct TestResult {
  bool passed = false;
  std::string message;
  std::string expected;
  std::string received;
  long long duration_ms = 0;
};

class NullHandler final : public moq::ObjectHandler {
public:
  void on_object(const moq::Object &) override {}
  void on_publish_done(moq::PublishDone) override {}
  void on_error(moq::ReceiveError) override {}
};

bool EnvEnabled(const char *name) {
  const char *value = std::getenv(name);
  return value != nullptr && (std::string(value) == "1" || std::string(value) == "true");
}

uint16_t ParsePort(const std::string &value) {
  size_t consumed = 0;
  const unsigned long port = std::stoul(value, &consumed);
  if (consumed != value.size() || port == 0 || port > 65535) {
    throw std::invalid_argument("invalid relay port: " + value);
  }
  return static_cast<uint16_t>(port);
}

RelayEndpoint ParseRelayUrl(const std::string &url) {
  constexpr const char *scheme = "moqt://";
  if (url.compare(0, std::char_traits<char>::length(scheme), scheme) != 0) {
    throw std::invalid_argument("moqtopus only supports raw QUIC relay URLs using moqt://");
  }

  const std::string authority_and_path = url.substr(std::char_traits<char>::length(scheme));
  const size_t slash = authority_and_path.find('/');
  const std::string authority = authority_and_path.substr(0, slash);
  const size_t colon = authority.rfind(':');
  if (authority.empty() || colon == std::string::npos || colon == 0 || colon + 1 == authority.size()) {
    throw std::invalid_argument("relay URL must include host and port");
  }

  RelayEndpoint endpoint;
  endpoint.host = authority.substr(0, colon);
  endpoint.port = ParsePort(authority.substr(colon + 1));
  if (slash != std::string::npos) {
    endpoint.path = authority_and_path.substr(slash);
  }
  return endpoint;
}

Options ParseOptions(int argc, char **argv) {
  Options options;
  if (const char *relay_url = std::getenv("RELAY_URL")) {
    options.relay_url = relay_url;
  }
  if (const char *testcase = std::getenv("TESTCASE"); testcase != nullptr && *testcase != '\0') {
    options.testcase = testcase;
  }
  options.tls_disable_verify = EnvEnabled("TLS_DISABLE_VERIFY");
  options.verbose = EnvEnabled("VERBOSE");

  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "-l" || arg == "--list") {
      options.list = true;
    } else if (arg == "-v" || arg == "--verbose") {
      options.verbose = true;
    } else if (arg == "--tls-disable-verify") {
      options.tls_disable_verify = true;
    } else if (arg == "-r" || arg == "--relay" || arg == "-t" || arg == "--test") {
      if (index + 1 >= argc) {
        throw std::invalid_argument(arg + " requires a value");
      }
      const std::string value = argv[++index];
      if (arg == "-r" || arg == "--relay") {
        options.relay_url = value;
      } else {
        options.testcase = value;
      }
    } else {
      throw std::invalid_argument("unknown option: " + arg);
    }
  }
  return options;
}

moq::MsQuicClientConfig MakeClientConfig(const Options &options) {
  const RelayEndpoint endpoint = ParseRelayUrl(options.relay_url);
  moq::MsQuicClientConfig config;
  config.host = endpoint.host;
  config.port = endpoint.port;
  config.path = endpoint.path;
  config.disable_certificate_validation = options.tls_disable_verify;
  config.idle_timeout = kSessionIdleTimeout;
  return config;
}

template <typename Session> void WaitUntilReady(Session &session) {
  std::future<void> ready = session.ready();
  if (ready.wait_for(kSetupTimeout) != std::future_status::ready) {
    session.close();
    throw std::runtime_error("timeout waiting for peer SETUP");
  }
  ready.get();
}

std::unique_ptr<moq::Subscriber> ConnectSubscriber(const Options &options) {
  auto session = moq::Subscriber::connect(MakeClientConfig(options));
  WaitUntilReady(*session);
  return session;
}

std::unique_ptr<moq::Publisher> ConnectPublisher(const Options &options) {
  auto session = moq::Publisher::connect(MakeClientConfig(options));
  WaitUntilReady(*session);
  return session;
}

TestResult RunTest(const std::string &name, const Options &options) {
  const auto started = Clock::now();
  TestResult result;
  const moq::SubscribeRequest request{kTestNamespace, kTestTrack, {}};
  const moq::PublishedTrack track{kTestNamespace, kTestTrack, {}};
  try {
    if (name == "setup-only") {
      result.expected = "peer SETUP";
      auto session = ConnectSubscriber(options);
    } else if (name == "announce-only" || name == "publish-namespace-done") {
      result.expected = "REQUEST_OK for PUBLISH_NAMESPACE";
      auto publisher = ConnectPublisher(options);
      publisher->register_track(track);
      std::this_thread::sleep_for(kAnnouncementDelay);
      if (name == "publish-namespace-done") {
        result.expected = "PUBLISH_NAMESPACE followed by request stream cancellation";
        publisher->unregister_track(kTestNamespace, kTestTrack);
        std::this_thread::sleep_for(kWithdrawalDelay);
      }
    } else if (name == "subscribe-error") {
      result.expected = "REQUEST_ERROR";
      auto subscriber = ConnectSubscriber(options);
      auto subscribe =
          subscriber->subscribe({{"nonexistent", "namespace"}, kTestTrack, {}}, std::make_shared<NullHandler>());
      if (subscribe.wait_for(kSetupTimeout) != std::future_status::ready) {
        throw std::runtime_error("timeout waiting for REQUEST_ERROR");
      }
      try {
        const auto handle = subscribe.get();
        subscriber->stop_subscription(handle.request_id);
        result.received = "SUBSCRIBE_OK";
        throw std::runtime_error("relay accepted a subscription to a non-existent track");
      } catch (const moq::RequestRejected &) {
        // Rejection is the expected result for this test.
      }
    } else if (name == "announce-subscribe") {
      result.expected = "SUBSCRIBE_OK routed to publisher";
      auto publisher = ConnectPublisher(options);
      publisher->register_track(track);
      std::this_thread::sleep_for(kSubscriberStartDelay);
      auto subscriber = ConnectSubscriber(options);
      auto subscribe = subscriber->subscribe(request, std::make_shared<NullHandler>());
      if (subscribe.wait_for(kSubscribeTimeout) != std::future_status::ready) {
        throw std::runtime_error("timeout waiting for SUBSCRIBE_OK");
      }
      subscriber->stop_subscription(subscribe.get().request_id);
    } else {
      result.expected = "SUBSCRIBE_OK or REQUEST_ERROR";
      auto subscriber = ConnectSubscriber(options);
      auto subscribe = subscriber->subscribe(request, std::make_shared<NullHandler>());
      std::this_thread::sleep_for(kPublisherStartDelay);
      auto publisher = ConnectPublisher(options);
      publisher->register_track(track);
      std::this_thread::sleep_for(kSubscriberStartDelay);
      if (subscribe.wait_for(kSubscribeBeforeAnnounceTimeout) != std::future_status::ready) {
        throw std::runtime_error("timeout waiting for SUBSCRIBE_OK or REQUEST_ERROR");
      }
      try {
        subscriber->stop_subscription(subscribe.get().request_id);
      } catch (const moq::RequestRejected &) {
        // Relays may reject rather than buffer before an announcement.
      }
    }
    result.passed = true;
    result.expected.clear();
  } catch (const moq::RequestRejected &error) {
    result.message = error.what();
    result.received = "REQUEST_ERROR";
  } catch (const std::exception &error) {
    result.message = error.what();
    if (result.received.empty())
      result.received = "connection or protocol failure";
  }
  result.duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - started).count();
  return result;
}

std::string EscapeYaml(const std::string &value) {
  std::string escaped;
  escaped.reserve(value.size());
  for (const char ch : value) {
    if (ch == '\\' || ch == '"') {
      escaped.push_back('\\');
    }
    escaped.push_back(ch == '\n' ? ' ' : ch);
  }
  return escaped;
}

void PrintResult(size_t number, const std::string &name, const TestResult &result) {
  std::cout << (result.passed ? "ok " : "not ok ") << number << " - " << name << '\n';
  std::cout << "  ---\n";
  std::cout << "  duration_ms: " << result.duration_ms << '\n';
  if (!result.message.empty()) {
    std::cout << "  message: \"" << EscapeYaml(result.message) << "\"\n";
  }
  if (!result.expected.empty()) {
    std::cout << "  expected: \"" << EscapeYaml(result.expected) << "\"\n";
  }
  if (!result.received.empty()) {
    std::cout << "  received: \"" << EscapeYaml(result.received) << "\"\n";
  }
  std::cout << "  ...\n";
}

int main(int argc, char **argv) {
  try {
    const Options options = ParseOptions(argc, argv);
    spdlog::set_default_logger(spdlog::stderr_color_mt("moqtopus-interop"));
    spdlog::set_level(options.verbose ? spdlog::level::debug : spdlog::level::off);

    if (options.list) {
      for (const std::string &name : kTestNames) {
        std::cout << name << '\n';
      }
      return kExitSuccess;
    }

    if (options.testcase && std::find(kTestNames.begin(), kTestNames.end(), *options.testcase) == kTestNames.end()) {
      std::cerr << "unsupported test: " << *options.testcase << '\n';
      return kExitUnsupported;
    }

    const std::vector<std::string> tests = options.testcase ? std::vector<std::string>{*options.testcase} : kTestNames;
    std::cout << "TAP version 14\n";
    std::cout << "# moqtopus interop client\n";
    std::cout << "# Relay: " << options.relay_url << '\n';
    std::cout << "# Draft: draft-18\n";
    std::cout << "1.." << tests.size() << '\n';

    bool failed = false;
    for (size_t index = 0; index < tests.size(); ++index) {
      const TestResult result = RunTest(tests[index], options);
      PrintResult(index + 1, tests[index], result);
      failed = failed || !result.passed;
    }
    return failed ? kExitFailure : kExitSuccess;
  } catch (const std::exception &error) {
    std::cerr << "moqtopus interop client: " << error.what() << '\n';
    return kExitFailure;
  }
}

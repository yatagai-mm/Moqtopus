#pragma once

#include "moq/client_config.h"
#include "moq/types.h"

#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace moq {

class MsQuicTransportAdapter;
class StreamContext;
namespace codec {
struct ControlMessage;
}

// Connection lifetime shared by Subscriber and Publisher.
class Session {
public:
  std::future<void> ready();
  SessionStateSnapshot state() const;
  void close(SessionCloseErrorCode error = SessionCloseErrorCode::NoError);

protected:
  explicit Session(MsQuicClientConfig config);
  virtual ~Session();
  void start();
  void close_and_wait();
  void protocol_violation(std::string error);
  std::unique_lock<std::recursive_mutex> lock_session() const { return std::unique_lock<std::recursive_mutex>(mutex_); }
  void begin_close(SessionCloseErrorCode code, std::string reason);
  RequestId allocate_request_id();
  static bool known_peer_request_type(uint64_t type);

  template <typename Promise> static void fail(Promise &promise, std::exception_ptr error) {
    try {
      promise.set_exception(std::move(error));
    } catch (const std::future_error &) {
    }
  }

  template <typename Promise> static void fail(Promise &promise, const std::string &message) {
    fail(promise, std::make_exception_ptr(std::runtime_error(message)));
  }

  virtual void handle_data_stream(uint64_t type, const std::shared_ptr<StreamContext> &stream, ByteBuffer prefix,
                                  bool fin) = 0;
  virtual void handle_peer_request(const codec::ControlMessage &message, const std::shared_ptr<StreamContext> &stream,
                                   ByteBuffer leftover, bool fin) = 0;
  virtual void on_datagram(BytesView) {}
  virtual void on_ready() {}
  virtual void terminate_subscriptions(const std::string &reason) = 0;
  virtual size_t active_subscriptions() const = 0;

  mutable std::recursive_mutex mutex_;
  SessionStateSnapshot state_;
  // Drained by the derived destructor before its callback targets disappear.
  std::unique_ptr<MsQuicTransportAdapter> transport_;

private:
  class PeerStreamGate;
  void on_peer_stream_started(std::shared_ptr<StreamContext> stream);
  void handle_peer_setup();
  void maybe_ready();
  void fail_ready_waiters(const std::string &reason);
  MsQuicClientConfig config_;
  RequestId next_request_id_ = 0;
  std::vector<std::promise<void>> ready_waiters_;
};

} // namespace moq

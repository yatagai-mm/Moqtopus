#pragma once

#include "moq/codec.h"
#include "msquic_transport_adapter.h"

#include <algorithm>
#include <future>
#include <limits>
#include <mutex>

namespace moq::detail {

template <typename Promise> void fail(Promise &promise, std::exception_ptr error) {
  try {
    promise.set_exception(std::move(error));
  } catch (const std::future_error &) {
  }
}

template <typename Promise> void fail(Promise &promise, const std::string &message) {
  fail(promise, std::make_exception_ptr(std::runtime_error(message)));
}

inline bool known_peer_request_type(uint64_t type) {
  switch (type) {
  case codec::kMessageSubscribe:
  case codec::kMessagePublish:
  case codec::kMessagePublishNamespace:
  case codec::kMessageTrackStatus:
  case codec::kMessageFetch:
  case codec::kMessageSubscribeNamespace:
  case codec::kMessageSubscribeTracks:
    return true;
  default:
    return false;
  }
}

// Both client roles share SETUP, readiness, request IDs and transport lifetime.
class Session {
public:
  explicit Session(MsQuicClientConfig config) : config_(std::move(config)) {}
  virtual ~Session() = default;
  void start();
  std::future<void> ready();
  SessionStateSnapshot state() const;
  void close(SessionCloseErrorCode error);
  void close_and_wait();
  void handle_peer_setup();
  void protocol_violation(std::string error);
  std::unique_lock<std::recursive_mutex> lock_session() const { return std::unique_lock<std::recursive_mutex>(mutex_); }

protected:
  virtual void on_peer_stream_started(std::shared_ptr<StreamContext> stream) = 0;
  virtual void on_datagram(BytesView) {}
  virtual void on_ready() {}
  virtual void terminate_subscriptions(const std::string &reason) = 0;
  virtual size_t active_subscriptions() const = 0;
  void begin_close(SessionCloseErrorCode code, std::string reason);
  RequestId allocate_request_id();

  mutable std::recursive_mutex mutex_;
  SessionStateSnapshot state_;
  // Destroyed by the derived destructor before callback targets disappear.
  std::unique_ptr<MsQuicTransportAdapter> transport_;

private:
  void maybe_ready();
  void fail_ready_waiters(const std::string &reason);
  MsQuicClientConfig config_;
  RequestId next_request_id_ = 0;
  std::vector<std::promise<void>> ready_waiters_;
};

// Buffers only until a stream is classified, then hands its bytes to the role.
template <typename Owner> class PeerStreamGate final : public StreamSink {
public:
  PeerStreamGate(std::weak_ptr<Owner> session, std::weak_ptr<StreamContext> stream)
      : session_(std::move(session)), stream_(std::move(stream)) {}

  void on_receive(const BytesView *chunks, size_t count, bool fin) override {
    const auto session = session_.lock();
    const auto stream = stream_.lock();
    if (!session || !stream || mode_ == Mode::Done)
      return;
    const auto lock = session->lock_session();
    for (size_t index = 0; index < count; ++index) {
      if (mode_ == Mode::Padding) {
        if (!all_zero(chunks[index]))
          return session->protocol_violation("padding stream contains non-zero bytes");
      } else {
        bytes_.insert(bytes_.end(), chunks[index].begin(), chunks[index].end());
      }
    }
    if (mode_ == Mode::Padding)
      return;
    if (stream->unidirectional()) {
      const auto type = codec::read_varint(bytes_);
      if (type.status != codec::DecodeStatus::Done) {
        if (fin)
          session->protocol_violation("peer unidirectional stream ended before type");
        return;
      }
      if (type.value == codec::kPaddingStreamType) {
        mode_ = Mode::Padding;
        if (!all_zero({bytes_.data() + type.bytes, bytes_.size() - type.bytes})) {
          return session->protocol_violation("padding stream contains non-zero bytes");
        }
        bytes_.clear();
        return;
      }
      if (type.value != codec::kSetupStreamType) {
        mode_ = Mode::Done;
        if (type.value == codec::kFetchStreamType || codec::is_subgroup_stream_type(type.value)) {
          return session->handle_data_stream(type.value, stream, std::move(bytes_), fin);
        }
        return session->protocol_violation("unknown peer unidirectional stream type " + std::to_string(type.value));
      }
    }
    const auto frame = codec::read_control_message(bytes_);
    if (frame.status != codec::DecodeStatus::Done) {
      if (fin)
        session->protocol_violation("peer stream ended before first message");
      return;
    }
    mode_ = Mode::Done;
    if (stream->unidirectional()) {
      std::string error;
      if (!codec::decode_setup(frame.message.payload, error))
        return session->protocol_violation(std::move(error));
      bytes_.clear();
      session->handle_peer_setup();
    } else {
      bytes_.erase(bytes_.begin(), bytes_.begin() + frame.bytes);
      session->handle_peer_request(frame.message, stream, std::move(bytes_), fin);
    }
  }

private:
  static bool all_zero(BytesView bytes) {
    return std::all_of(bytes.begin(), bytes.end(), [](uint8_t byte) { return byte == 0; });
  }
  enum class Mode { Classify, Done, Padding };
  std::weak_ptr<Owner> session_;
  std::weak_ptr<StreamContext> stream_;
  Mode mode_ = Mode::Classify;
  ByteBuffer bytes_;
};

} // namespace moq::detail

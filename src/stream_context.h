#pragma once

#include "moq/types.h"

#include <msquic.h>

#include <cstdint>
#include <memory>

namespace moq {

// including msquic_transport_adapter.h will cause circular dependency, so forward declare here
class MsQuicTransportAdapter;

// MsQuic retains both bytes and descriptor until send completion.
struct PendingSend {
  explicit PendingSend(ByteBuffer input)
      : bytes(std::move(input)), buffer{static_cast<uint32_t>(bytes.size()), bytes.data()} {}
  ByteBuffer bytes;
  QUIC_BUFFER buffer;
};

// StreamSink is a common interface for handling incoming bytes. StreamContext owns a StreamSink and forwards bytes to
// it.
class StreamSink {
public:
  virtual ~StreamSink() = default;

  virtual void on_receive(const BytesView *chunks, size_t count, bool fin) = 0;
  virtual void on_peer_send_aborted(uint64_t /*error_code*/) {}
  // Peer sent STOP_SENDING for our send direction.
  virtual void on_peer_receive_aborted(uint64_t /*error_code*/) {}
  virtual void on_stream_closed() {}
};

// StreamContext manages a single QUIC stream. StreamContext does not actually parses nor do any operation for the bytes
// but translate it to BytesView and forward it to StreamSink, so later handlers can parse the bytes without copying.
class StreamContext {
public:
  StreamContext(const StreamContext &) = delete;
  StreamContext &operator=(const StreamContext &) = delete;

  bool unidirectional() const { return unidirectional_; }
  uint64_t id() const { return id_; }

  // A sink may swap itself out mid-call; later events go to the new sink.
  // For example, incoming bytes are first handled by PeerStreamGate, which classifies the stream and then swaps in the
  // role-specific handler such as SubgroupReceiver or Publisher::SubscriptionRequest.
  void set_sink(std::shared_ptr<StreamSink> sink) { sink_ = std::move(sink); }

  bool send(ByteBuffer bytes, bool fin = false);
  void abort_receive(uint64_t error_code);
  // Gracefully finish the send direction (FIN without payload).
  bool finish_send();
  // Reset the send direction only; the receive direction stays open.
  void abort_send(uint64_t error_code);

private:
  friend class MsQuicTransportAdapter;

  StreamContext(MsQuicTransportAdapter &adapter, HQUIC handle, bool unidirectional);
  static QUIC_STATUS QUIC_API stream_callback(HQUIC stream, void *context, QUIC_STREAM_EVENT *event);

  MsQuicTransportAdapter &adapter_;
  HQUIC handle_ = nullptr;
  bool unidirectional_ = false;
  uint64_t id_ = 0;
  std::shared_ptr<StreamSink> sink_;
};

} // namespace moq

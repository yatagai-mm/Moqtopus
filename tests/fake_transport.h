#pragma once
#include "msquic_transport_adapter.h"
#include <vector>

namespace fake {
struct Stream {
  moq::ByteBuffer sent;
  bool fin = false;
  std::vector<std::pair<QUIC_STREAM_SHUTDOWN_FLAGS, uint64_t>> shutdowns;
  std::function<void(QUIC_STREAM_EVENT *)> event;
  void receive(const moq::ByteBuffer &bytes, bool fin = false);
};
struct Connection {
  moq::detail::MsQuicTransportAdapter *adapter;
  moq::detail::MsQuicTransportAdapter::Callbacks callbacks;
  std::vector<std::unique_ptr<Stream>> streams;
  std::vector<moq::ByteBuffer> datagrams;
  moq::SessionCloseErrorCode close_code = moq::SessionCloseErrorCode::NoError;
  bool fail_send = false;
  Stream &peer_stream(bool unidirectional);
};
extern Connection *connection;
} // namespace fake

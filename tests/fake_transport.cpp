#include "fake_transport.h"
#include <stdexcept>

namespace fake {
Connection *connection = nullptr;

void Stream::receive(const moq::ByteBuffer &bytes, bool fin) {
  QUIC_BUFFER buffer{static_cast<uint32_t>(bytes.size()), const_cast<uint8_t *>(bytes.data())};
  QUIC_STREAM_EVENT received{};
  received.Type = QUIC_STREAM_EVENT_RECEIVE;
  received.RECEIVE.Flags = fin ? QUIC_RECEIVE_FLAG_FIN : QUIC_RECEIVE_FLAG_NONE;
  received.RECEIVE.BufferCount = 1;
  received.RECEIVE.Buffers = &buffer;
  received.RECEIVE.TotalBufferLength = bytes.size();
  event(&received);
}

Stream &Connection::peer_stream(bool unidirectional) {
  auto stream = adapter->open_stream(unidirectional);
  callbacks.peer_stream_started(stream);
  return *streams.back();
}
} // namespace fake

namespace moq {
std::string quic_status_string(QUIC_STATUS) { return "fake transport failure"; }

MsQuicTransportAdapter::MsQuicTransportAdapter(MsQuicClientConfig, Callbacks callbacks)
    : callbacks_(std::move(callbacks)) {
  static const QUIC_API_TABLE api = [] {
    QUIC_API_TABLE table{};
    table.StreamSend = [](HQUIC handle, const QUIC_BUFFER *buffers, uint32_t count, QUIC_SEND_FLAGS flags,
                          void *context) -> QUIC_STATUS {
      if (fake::connection->fail_send)
        return QUIC_STATUS_INTERNAL_ERROR;
      auto &stream = *reinterpret_cast<fake::Stream *>(handle);
      for (uint32_t i = 0; i < count; ++i)
        stream.sent.insert(stream.sent.end(), buffers[i].Buffer, buffers[i].Buffer + buffers[i].Length);
      stream.fin = (flags & QUIC_SEND_FLAG_FIN) != 0;
      QUIC_STREAM_EVENT done{};
      done.Type = QUIC_STREAM_EVENT_SEND_COMPLETE;
      done.SEND_COMPLETE.ClientContext = context;
      stream.event(&done);
      return QUIC_STATUS_SUCCESS;
    };
    table.StreamShutdown = [](HQUIC handle, QUIC_STREAM_SHUTDOWN_FLAGS flags, QUIC_UINT62 code) -> QUIC_STATUS {
      reinterpret_cast<fake::Stream *>(handle)->shutdowns.emplace_back(flags, code);
      return QUIC_STATUS_SUCCESS;
    };
    table.StreamClose = [](HQUIC) {};
    return table;
  }();
  api_ = &api;
  if (fake::connection)
    throw std::runtime_error("only one fake connection at a time");
  fake::connection = new fake::Connection{this, callbacks_, {}, {}};
}

MsQuicTransportAdapter::~MsQuicTransportAdapter() {
  // Emit closure while the session still owns all callback targets.
  const auto active = streams_;
  for (const auto &entry : active) {
    QUIC_STREAM_EVENT closed{};
    closed.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE;
    entry.second->handle_event(entry.second->handle_, &closed);
  }
  delete fake::connection;
  fake::connection = nullptr;
}
void MsQuicTransportAdapter::start() { callbacks_.connected(); }
std::shared_ptr<StreamContext> MsQuicTransportAdapter::open_stream(bool unidirectional) {
  auto record = std::make_unique<fake::Stream>();
  auto stream =
      std::shared_ptr<StreamContext>(new StreamContext(*this, reinterpret_cast<HQUIC>(record.get()), unidirectional));
  stream->id_ = fake::connection->streams.size() * 4 + (unidirectional ? 2 : 0);
  record->event = [weak = std::weak_ptr<StreamContext>(stream)](QUIC_STREAM_EVENT *event) {
    if (auto active = weak.lock())
      active->handle_event(active->handle_, event);
  };
  streams_.emplace(stream.get(), stream);
  fake::connection->streams.push_back(std::move(record));
  return stream;
}
bool MsQuicTransportAdapter::send_datagram(ByteBuffer bytes) {
  fake::connection->datagrams.push_back(std::move(bytes));
  return true;
}
void MsQuicTransportAdapter::shutdown(SessionCloseErrorCode code) {
  fake::connection->close_code = code;
  callbacks_.shutdown_complete(true);
}
void MsQuicTransportAdapter::remove_stream(StreamContext *stream) { streams_.erase(stream); }
} // namespace moq

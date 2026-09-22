#include "stream_context.h"

#include "msquic_transport_adapter.h"

#include <spdlog/spdlog.h>

#include <utility>
#include <vector>

namespace moq {
// StreamContext manages a single QUIC stream. Receive events are handed to the
// installed StreamSink without copying: the sink parses the QUIC buffers in place.
StreamContext::StreamContext(MsQuicTransportAdapter &adapter, HQUIC handle, bool unidirectional)
    : adapter_(adapter), handle_(handle), unidirectional_(unidirectional) {}

bool StreamContext::send(ByteBuffer bytes, bool fin) {
  if (!handle_) {
    return false;
  }
  auto *pending = new PendingSend(std::move(bytes));
  const QUIC_SEND_FLAGS flags = fin ? QUIC_SEND_FLAG_FIN : QUIC_SEND_FLAG_NONE;
  const QUIC_STATUS status = adapter_.api_->StreamSend(handle_, &pending->buffer, 1, flags, pending);
  if (QUIC_FAILED(status)) {
    delete pending;
    return false;
  }
  return true;
}

void StreamContext::abort_receive(uint64_t error_code) {
  if (handle_) {
    adapter_.api_->StreamShutdown(handle_, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, error_code);
  }
}

bool StreamContext::finish_send() {
  if (!handle_) {
    return false;
  }
  return !QUIC_FAILED(adapter_.api_->StreamShutdown(handle_, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0));
}

void StreamContext::abort_send(uint64_t error_code) {
  if (handle_) {
    adapter_.api_->StreamShutdown(handle_, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND, error_code);
  }
}

QUIC_STATUS QUIC_API StreamContext::stream_callback(HQUIC stream, void *context, QUIC_STREAM_EVENT *event) {
  return static_cast<StreamContext *>(context)->handle_event(stream, event);
}

QUIC_STATUS StreamContext::handle_event(HQUIC stream, QUIC_STREAM_EVENT *event) {
  // Local copy so the sink can swap the stream over to a new sink mid-call.
  const std::shared_ptr<StreamSink> sink = sink_;
  switch (event->Type) {
  case QUIC_STREAM_EVENT_START_COMPLETE:
    id_ = event->START_COMPLETE.ID;
    if (QUIC_FAILED(event->START_COMPLETE.Status)) {
      adapter_.callbacks_.transport_error("StreamStart failed: " + quic_status_string(event->START_COMPLETE.Status));
    }
    break;
  case QUIC_STREAM_EVENT_RECEIVE: {
    const bool fin = (event->RECEIVE.Flags & QUIC_RECEIVE_FLAG_FIN) != 0;
    SPDLOG_TRACE("Stream {} received {} bytes fin={}", id(), event->RECEIVE.TotalBufferLength, fin);
    if (!sink) {
      break;
    }
    BytesView stack_chunks[4];
    std::vector<BytesView> spill;
    const uint32_t count = event->RECEIVE.BufferCount;
    BytesView *chunks = stack_chunks;
    if (count > 4) {
      spill.resize(count);
      chunks = spill.data();
    }
    for (uint32_t index = 0; index < count; ++index) {
      chunks[index] = BytesView{event->RECEIVE.Buffers[index].Buffer, event->RECEIVE.Buffers[index].Length};
    }
    sink->on_receive(chunks, count, fin);
    break;
  }
  case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
    if (sink) {
      sink->on_receive(nullptr, 0, true);
    }
    break;
  case QUIC_STREAM_EVENT_SEND_COMPLETE:
    delete static_cast<PendingSend *>(event->SEND_COMPLETE.ClientContext);
    break;
  case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
    if (sink) {
      sink->on_peer_send_aborted(event->PEER_SEND_ABORTED.ErrorCode);
    }
    break;
  case QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED:
    if (sink) {
      sink->on_peer_receive_aborted(event->PEER_RECEIVE_ABORTED.ErrorCode);
    }
    break;
  case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
    if (handle_) {
      adapter_.api_->StreamClose(stream);
      handle_ = nullptr;
    }
    sink_.reset(); // break the StreamContext <-> sink ownership cycle
    if (sink) {
      sink->on_stream_closed();
    }
    adapter_.remove_stream(this);
    break;
  default:
    break;
  }
  return QUIC_STATUS_SUCCESS;
}

} // namespace moq

#pragma once

#include "moq/client_config.h"
#include "moq/errors.h"
#include "stream_context.h"

#include <msquic.h>

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace moq {

std::string quic_status_string(QUIC_STATUS status);

class MsQuicTransportAdapter {
public:
  struct Callbacks {
    std::function<void()> connected;
    std::function<void(std::shared_ptr<StreamContext>)> peer_stream_started;
    // datagram bytes are only valid during the call
    std::function<void(BytesView)> datagram_received;
    std::function<void(std::string)> transport_error;
    std::function<void(bool)> shutdown_complete;
  };

  MsQuicTransportAdapter(MsQuicClientConfig config, Callbacks callbacks);
  ~MsQuicTransportAdapter();

  MsQuicTransportAdapter(const MsQuicTransportAdapter &) = delete;
  MsQuicTransportAdapter &operator=(const MsQuicTransportAdapter &) = delete;

  void start();
  std::shared_ptr<StreamContext> open_stream(bool unidirectional);
  // Fire-and-forget: a false return or a dropped datagram is not reported.
  bool send_datagram(ByteBuffer bytes);
  void shutdown(moq::SessionCloseErrorCode error_code);

private:
  friend class StreamContext;

  static QUIC_STATUS QUIC_API connection_callback(HQUIC connection, void *context, QUIC_CONNECTION_EVENT *event);
  void remove_stream(StreamContext *stream);

  MsQuicClientConfig config_;
  Callbacks callbacks_;
  const QUIC_API_TABLE *api_ = nullptr;
  HQUIC registration_ = nullptr;
  HQUIC configuration_ = nullptr;
  HQUIC connection_ = nullptr;
  std::mutex shutdown_mutex_;
  std::condition_variable shutdown_cv_;
  bool shutdown_requested_ = false;
  bool shutdown_complete_ = false;
  std::mutex streams_mutex_;
  std::unordered_map<StreamContext *, std::shared_ptr<StreamContext>> streams_;
};

} // namespace moq

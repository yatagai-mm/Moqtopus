#include "msquic_transport_adapter.h"

#include <spdlog/spdlog.h>
#include <stdexcept>
#include <utility>

#include <msquichelper.h>

namespace moq {

std::string quic_status_string(QUIC_STATUS status) {
  switch (status) {
  case QUIC_STATUS_BAD_CERTIFICATE:
    return "BAD_CERTIFICATE";
  case QUIC_STATUS_UNSUPPORTED_CERTIFICATE:
    return "UNSUPPORTED_CERTIFICATE";
  case QUIC_STATUS_REVOKED_CERTIFICATE:
    return "REVOKED_CERTIFICATE";
  case QUIC_STATUS_EXPIRED_CERTIFICATE:
    return "EXPIRED_CERTIFICATE";
  case QUIC_STATUS_UNKNOWN_CERTIFICATE:
    return "UNKNOWN_CERTIFICATE";
  case QUIC_STATUS_REQUIRED_CERTIFICATE:
    return "REQUIRED_CERTIFICATE";
  default:
    return QuicStatusToString(status);
  }
}

static void throw_if_failed(QUIC_STATUS status, const char *what) {
  if (QUIC_FAILED(status)) {
    throw std::runtime_error(std::string(what) + " failed: " + quic_status_string(status));
  }
}

MsQuicTransportAdapter::MsQuicTransportAdapter(MsQuicClientConfig config, Callbacks callbacks)
    : config_(std::move(config)), callbacks_(std::move(callbacks)) {
  throw_if_failed(MsQuicOpen2(&api_), "MsQuicOpen2");

  QUIC_REGISTRATION_CONFIG registration_config{};
  registration_config.AppName = "moqtopus";
  registration_config.ExecutionProfile = QUIC_EXECUTION_PROFILE_LOW_LATENCY;
  throw_if_failed(api_->RegistrationOpen(&registration_config, &registration_), "RegistrationOpen");

  QUIC_BUFFER alpn{};
  alpn.Length = sizeof(kAlpn) - 1;
  alpn.Buffer = reinterpret_cast<uint8_t *>(const_cast<char *>(kAlpn));

  QUIC_SETTINGS settings{};
  settings.IdleTimeoutMs = static_cast<uint64_t>(config_.idle_timeout.count());
  settings.IsSet.IdleTimeoutMs = TRUE;
  settings.PeerBidiStreamCount = 128;
  settings.IsSet.PeerBidiStreamCount = TRUE;
  settings.PeerUnidiStreamCount = 1024;
  settings.IsSet.PeerUnidiStreamCount = TRUE;
  settings.DatagramReceiveEnabled = TRUE;
  settings.IsSet.DatagramReceiveEnabled = TRUE;

  throw_if_failed(
      api_->ConfigurationOpen(registration_, &alpn, 1, &settings, sizeof(settings), nullptr, &configuration_),
      "ConfigurationOpen");

  QUIC_CREDENTIAL_CONFIG credential{};
  credential.Type = QUIC_CREDENTIAL_TYPE_NONE;
  credential.Flags = QUIC_CREDENTIAL_FLAG_CLIENT;
  if (config_.disable_certificate_validation) {
    credential.Flags |= QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;
  }
  throw_if_failed(api_->ConfigurationLoadCredential(configuration_, &credential), "ConfigurationLoadCredential");
  throw_if_failed(api_->ConnectionOpen(registration_, connection_callback, this, &connection_), "ConnectionOpen");
}

MsQuicTransportAdapter::~MsQuicTransportAdapter() {
  {
    std::unique_lock<std::mutex> lock(shutdown_mutex_);
    if (shutdown_requested_) {
      shutdown_cv_.wait_for(lock, config_.idle_timeout, [this] { return shutdown_complete_; });
    }
  }

  std::unordered_map<StreamContext *, std::shared_ptr<StreamContext>> streams;
  {
    std::lock_guard<std::mutex> lock(streams_mutex_);
    streams.swap(streams_);
  }
  // StreamClose may synchronously invoke a shutdown callback that calls
  // remove_stream(), so it must run without streams_mutex_ held.
  for (auto &entry : streams) {
    if (entry.second->handle_) {
      api_->StreamClose(entry.second->handle_);
      entry.second->handle_ = nullptr;
    }
  }
  if (connection_) {
    api_->ConnectionClose(connection_);
  }
  if (configuration_) {
    api_->ConfigurationClose(configuration_);
  }
  if (registration_) {
    api_->RegistrationClose(registration_);
  }
  if (api_) {
    MsQuicClose(api_);
  }
}

void MsQuicTransportAdapter::start() {
  throw_if_failed(api_->ConnectionStart(connection_, configuration_, QUIC_ADDRESS_FAMILY_UNSPEC, config_.host.c_str(),
                                        config_.port),
                  "ConnectionStart");
}

std::shared_ptr<StreamContext> MsQuicTransportAdapter::open_stream(bool unidirectional) {
  HQUIC handle = nullptr;
  const QUIC_STREAM_OPEN_FLAGS open_flags =
      unidirectional ? QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL : QUIC_STREAM_OPEN_FLAG_NONE;
  throw_if_failed(api_->StreamOpen(connection_, open_flags, StreamContext::stream_callback, nullptr, &handle),
                  "StreamOpen");

  auto stream = std::shared_ptr<StreamContext>(new StreamContext(*this, handle, unidirectional));
  api_->SetCallbackHandler(handle, reinterpret_cast<void *>(StreamContext::stream_callback), stream.get());
  {
    std::lock_guard<std::mutex> lock(streams_mutex_);
    streams_.emplace(stream.get(), stream);
  }
  const QUIC_STATUS status = api_->StreamStart(handle, QUIC_STREAM_START_FLAG_IMMEDIATE);
  if (QUIC_FAILED(status)) {
    remove_stream(stream.get());
    api_->StreamClose(handle);
    stream->handle_ = nullptr;
    throw std::runtime_error(std::string("StreamStart failed: ") + quic_status_string(status));
  }
  return stream;
}

bool MsQuicTransportAdapter::send_datagram(ByteBuffer bytes) {
  if (!connection_) {
    return false;
  }
  auto *pending = new PendingSend(std::move(bytes));
  const QUIC_STATUS status = api_->DatagramSend(connection_, &pending->buffer, 1, QUIC_SEND_FLAG_NONE, pending);
  if (QUIC_FAILED(status)) {
    delete pending;
    return false;
  }
  return true;
}

void MsQuicTransportAdapter::shutdown(moq::SessionCloseErrorCode error_code) {
  if (connection_) {
    spdlog::debug("MsQuicTransportAdapter shutdown requested: code={}", static_cast<uint64_t>(error_code));
    {
      std::lock_guard<std::mutex> lock(shutdown_mutex_);
      shutdown_requested_ = true;
    }
    api_->ConnectionShutdown(connection_, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, static_cast<uint64_t>(error_code));
  }
}

QUIC_STATUS QUIC_API MsQuicTransportAdapter::connection_callback(HQUIC, void *context, QUIC_CONNECTION_EVENT *event) {
  return static_cast<MsQuicTransportAdapter *>(context)->handle_connection_event(event);
}

QUIC_STATUS MsQuicTransportAdapter::handle_connection_event(QUIC_CONNECTION_EVENT *event) {
  switch (event->Type) {
  case QUIC_CONNECTION_EVENT_CONNECTED:
    callbacks_.connected();
    break;
  case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED: {
    const bool unidirectional = (event->PEER_STREAM_STARTED.Flags & QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL) != 0;
    auto stream =
        std::shared_ptr<StreamContext>(new StreamContext(*this, event->PEER_STREAM_STARTED.Stream, unidirectional));
    stream->id_ = GetStreamID(api_, event->PEER_STREAM_STARTED.Stream);
    api_->SetCallbackHandler(event->PEER_STREAM_STARTED.Stream,
                             reinterpret_cast<void *>(StreamContext::stream_callback), stream.get());
    {
      std::lock_guard<std::mutex> lock(streams_mutex_);
      streams_.emplace(stream.get(), stream);
    }
    callbacks_.peer_stream_started(stream);
    break;
  }
  case QUIC_CONNECTION_EVENT_DATAGRAM_RECEIVED:
    callbacks_.datagram_received(
        BytesView{event->DATAGRAM_RECEIVED.Buffer->Buffer, event->DATAGRAM_RECEIVED.Buffer->Length});
    break;
  case QUIC_CONNECTION_EVENT_DATAGRAM_SEND_STATE_CHANGED:
    if (QUIC_DATAGRAM_SEND_STATE_IS_FINAL(event->DATAGRAM_SEND_STATE_CHANGED.State)) {
      delete static_cast<PendingSend *>(event->DATAGRAM_SEND_STATE_CHANGED.ClientContext);
    }
    break;
  case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT: {
    const QUIC_STATUS status = event->SHUTDOWN_INITIATED_BY_TRANSPORT.Status;
    const uint64_t error_code = event->SHUTDOWN_INITIATED_BY_TRANSPORT.ErrorCode;
    const std::string message = "Connection shutdown initiated by transport: status=" + quic_status_string(status) +
                                " (" + std::to_string(status) + "), error_code=" + std::to_string(error_code);
    callbacks_.transport_error(message);
    break;
  }
  case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
    callbacks_.transport_error("peer shutdown: error_code=" +
                               std::to_string(event->SHUTDOWN_INITIATED_BY_PEER.ErrorCode));
    break;
  case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE: {
    const bool handshake_completed = event->SHUTDOWN_COMPLETE.HandshakeCompleted != FALSE;
    spdlog::debug("MsQuic connection shutdown complete: handshake_completed={}", handshake_completed);
    callbacks_.shutdown_complete(handshake_completed);
    {
      std::lock_guard<std::mutex> lock(shutdown_mutex_);
      shutdown_complete_ = true;
    }
    shutdown_cv_.notify_all();
    break;
  }
  default:
    break;
  }
  return QUIC_STATUS_SUCCESS;
}

void MsQuicTransportAdapter::remove_stream(StreamContext *stream) {
  std::lock_guard<std::mutex> lock(streams_mutex_);
  streams_.erase(stream);
}

} // namespace moq

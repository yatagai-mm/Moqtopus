#include "session.h"

namespace moq::detail {

// Session management that are common to both publisher and subscriber.
void Session::start() {
  MsQuicTransportAdapter::Callbacks callbacks;
  callbacks.connected = [this] {
    const auto lock = lock_session();
    if (state_.phase != SessionPhase::Init)
      return;
    state_.phase = SessionPhase::SetupInProgress;
    try {
      const auto authority =
          config_.authority.empty() ? config_.host + ":" + std::to_string(config_.port) : config_.authority;
      auto stream = transport_->open_stream(true);
      if (!stream->send(codec::encode_setup(authority, config_.path))) {
        throw std::runtime_error("StreamSend failed for SETUP");
      }
      state_.local_setup_sent = true;
      maybe_ready();
    } catch (const std::exception &error) {
      begin_close(SessionCloseErrorCode::InternalError, error.what());
    }
  };
  callbacks.peer_stream_started = [this](std::shared_ptr<StreamContext> stream) {
    on_peer_stream_started(std::move(stream));
  };
  callbacks.datagram_received = [this](BytesView bytes) {
    const auto lock = lock_session();
    on_datagram(bytes);
  };
  callbacks.transport_error = [this](std::string error) {
    const auto lock = lock_session();
    begin_close(SessionCloseErrorCode::InternalError, std::move(error));
  };
  callbacks.shutdown_complete = [this](bool handshake_completed) {
    const auto lock = lock_session();
    if (!handshake_completed && !state_.close_reason) {
      state_.close_reason = SessionCloseReason{SessionCloseErrorCode::InternalError, "QUIC handshake did not complete"};
    }
    state_.phase = SessionPhase::Closed;
    fail_ready_waiters("MOQT session closed");
  };
  transport_ = std::make_unique<MsQuicTransportAdapter>(config_, std::move(callbacks));
  transport_->start();
}

std::future<void> Session::ready() {
  const auto lock = lock_session();
  std::promise<void> promise;
  auto future = promise.get_future();
  if (state_.phase == SessionPhase::Ready) {
    promise.set_value();
  } else if (state_.phase >= SessionPhase::Closing) {
    fail(promise, "MOQT session closed before SETUP completed");
  } else {
    ready_waiters_.push_back(std::move(promise));
  }
  return future;
}

SessionStateSnapshot Session::state() const {
  const auto lock = lock_session();
  auto snapshot = state_;
  snapshot.active_subscriptions = active_subscriptions();
  return snapshot;
}

void Session::close(SessionCloseErrorCode error) {
  const auto lock = lock_session();
  begin_close(error, "local close");
}

void Session::close_and_wait() {
  std::unique_ptr<MsQuicTransportAdapter> transport;
  {
    const auto lock = lock_session();
    begin_close(SessionCloseErrorCode::NoError, "local close");
    transport = std::move(transport_);
  }
  // Drain callbacks outside the lock, while all derived members are still alive.
  transport.reset();
}

void Session::handle_peer_setup() {
  const auto lock = lock_session();
  state_.peer_setup_received = true;
  maybe_ready();
}

void Session::maybe_ready() {
  if (!state_.local_setup_sent || !state_.peer_setup_received || state_.phase >= SessionPhase::Ready)
    return;
  state_.phase = SessionPhase::Ready;
  state_.negotiated_version = kAlpn;
  for (auto &waiter : ready_waiters_)
    waiter.set_value();
  ready_waiters_.clear();
  on_ready();
}

void Session::protocol_violation(std::string error) {
  const auto lock = lock_session();
  begin_close(SessionCloseErrorCode::ProtocolViolation, std::move(error));
}

void Session::begin_close(SessionCloseErrorCode code, std::string reason) {
  if (state_.phase >= SessionPhase::Closing)
    return;
  state_.phase = SessionPhase::Closing;
  state_.close_reason = SessionCloseReason{code, reason};
  fail_ready_waiters(reason.empty() ? "MOQT session closing" : reason);
  terminate_subscriptions(reason);
  if (transport_)
    transport_->shutdown(code);
}

void Session::fail_ready_waiters(const std::string &reason) {
  for (auto &waiter : ready_waiters_)
    fail(waiter, reason);
  ready_waiters_.clear();
}

RequestId Session::allocate_request_id() {
  if (next_request_id_ > std::numeric_limits<RequestId>::max() - 2) {
    throw std::overflow_error("MOQT Request ID space exhausted");
  }
  const auto id = next_request_id_;
  next_request_id_ += 2; // client Request IDs are even
  return id;
}

} // namespace moq::detail

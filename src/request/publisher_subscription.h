#pragma once

#include "moq/codec.h"
#include "moq/errors.h"
#include "moq/types.h"
#include "send_data_plane.h"
#include "stream_context.h"

#include <memory>
#include <mutex>
#include <string>

namespace moq::detail {

// Session-side services the publisher request FSM needs. All methods except
// lock_session() assume the session lock is already held.
class PublisherRequestOwner {
public:
  virtual ~PublisherRequestOwner() = default;

  virtual std::unique_lock<std::recursive_mutex> lock_session() = 0;
  virtual void protocol_violation(std::string error) = 0;
  virtual void invalid_request_id(std::string error) = 0;
  virtual bool consume_peer_request_id(RequestId request_id, std::string &error) = 0;
  virtual SendDataPlane &send_plane() = 0;
  virtual void subscription_transport_event(RequestId request_id, const TrackNamespace &track_namespace,
                                            const TrackName &track_name, const std::string &event,
                                            uint64_t error_code) = 0;
  virtual void subscription_closed(RequestId subscribe_request_id) = 0;
};

// One accepted peer SUBSCRIBE. The session decodes and accepts the SUBSCRIBE
// itself (sending SUBSCRIBE_OK); the FSM then owns the bidi request stream:
// REQUEST_UPDATE handling, peer cancellation, and PUBLISH_DONE on termination.
class PublisherSubscriptionFSM final : public StreamSink {
public:
  PublisherSubscriptionFSM(RequestId request_id, TrackNamespace track_namespace, TrackName track_name,
                           TrackAlias track_alias, std::shared_ptr<StreamContext> stream,
                           std::weak_ptr<PublisherRequestOwner> owner);

  // StreamSink: entered from the transport thread; takes the session lock.
  void on_receive(const BytesView *chunks, size_t count, bool fin) override;
  void on_peer_send_aborted(uint64_t error_code) override;
  void on_peer_receive_aborted(uint64_t error_code) override; // subscriber STOP_SENDING
  void on_stream_closed() override;

  // The remaining entry points assume the session lock is held.

  // Hand over bytes the stream gate buffered past the SUBSCRIBE frame.
  void seed(ByteBuffer leftover, bool fin);
  // Local termination: close all data streams, then PUBLISH_DONE + FIN.
  void finish(PublishDoneCode code, const std::string &reason);
  // Peer cancellation or session close: reset all streams, destroy state.
  void cancel(uint64_t reset_error_code);

  moq::SubscriptionPhase phase() const { return phase_; }
  RequestId request_id() const { return request_id_; }
  TrackAlias track_alias() const { return track_alias_; }

private:
  void process_buffer(PublisherRequestOwner &owner, bool fin);
  void handle_request_update(PublisherRequestOwner &owner, const codec::ControlMessage &message);
  void reject_update_and_finish(PublisherRequestOwner &owner, RequestErrorCode code, const std::string &reason);

  RequestId request_id_;
  TrackNamespace track_namespace_;
  TrackName track_name_;
  TrackAlias track_alias_;
  std::shared_ptr<StreamContext> stream_;
  std::weak_ptr<PublisherRequestOwner> owner_;
  ByteBuffer buffer_;
  moq::SubscriptionPhase phase_ = moq::SubscriptionPhase::Established;
};

} // namespace moq::detail

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "google/cloud/bigtable/internal/sessions/session/session_impl.h"
#include "google/cloud/bigtable/internal/sessions/logging_helper.h"
#include "google/cloud/bigtable/internal/sessions/session/write_status.h"
#include "google/cloud/bigtable/internal/sessions/transport/current_cq.h"
#include "google/cloud/bigtable/internal/sessions/transport/session_stream.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/internal/getenv.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/protobuf/any.pb.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/unknown_field_set.h>
#include <google/rpc/error_details.pb.h>
#include <google/rpc/status.pb.h>
#include <grpcpp/support/string_ref.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

namespace {
std::atomic<double> g_keepalive_timeout_multiplier{3.0};
std::atomic<int> g_max_session_write_queue_size{10};
std::atomic<std::int64_t> g_goaway_drain_timeout_ms{30000};
}  // namespace

double SessionImpl::GetKeepaliveTimeoutMultiplier() {
  if (auto env = google::cloud::internal::GetEnv(
          "GOOGLE_CLOUD_BIGTABLE_SESSIONS_KEEPALIVE_MULTIPLIER")) {
    char* end = nullptr;
    double const v = std::strtod(env->c_str(), &end);
    if (end != env->c_str() && v > 0.0) return v;
  }
  return g_keepalive_timeout_multiplier.load(std::memory_order_relaxed);
}

void SessionImpl::SetKeepaliveTimeoutMultiplier(double multiplier) {
  g_keepalive_timeout_multiplier.store(multiplier, std::memory_order_relaxed);
}

int SessionImpl::GetMaxWriteQueueSize() {
  if (auto env = google::cloud::internal::GetEnv(
          "GOOGLE_CLOUD_BIGTABLE_SESSIONS_MAX_WRITE_QUEUE_SIZE")) {
    int v = 0;
    if (absl::SimpleAtoi(*env, &v) && v > 0) return v;
  }
  return g_max_session_write_queue_size.load(std::memory_order_relaxed);
}

void SessionImpl::SetMaxWriteQueueSize(int max_size) {
  g_max_session_write_queue_size.store(max_size, std::memory_order_relaxed);
}

std::chrono::nanoseconds SessionImpl::GetGoAwayDrainTimeout() {
  if (auto env = google::cloud::internal::GetEnv(
          "GOOGLE_CLOUD_BIGTABLE_SESSIONS_GOAWAY_DRAIN_TIMEOUT_MS")) {
    std::int64_t v = 0;
    if (absl::SimpleAtoi(*env, &v) && v > 0) {
      return std::chrono::milliseconds(v);
    }
  }
  return std::chrono::milliseconds(
      g_goaway_drain_timeout_ms.load(std::memory_order_relaxed));
}

void SessionImpl::SetGoAwayDrainTimeout(std::chrono::nanoseconds timeout) {
  std::int64_t const ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count();
  g_goaway_drain_timeout_ms.store(ms > 0 ? ms : 30000,
                                  std::memory_order_relaxed);
}

std::shared_ptr<SessionImpl> SessionImpl::Create(
    google::bigtable::v2::Bigtable::Stub* stub,
    std::weak_ptr<SessionListener> listener, std::string_view table_name,
    std::string_view app_profile_id,
    google::bigtable::v2::OpenTableRequest::Permission permission,
    google::cloud::CompletionQueue event_cq, ScheduleFn scheduler,
    std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config,
    SessionStream::MetadataHook metadata_hook,
    SessionStream::OpenConfig open_config, bool start_open) {
  auto session = std::shared_ptr<SessionImpl>(new SessionImpl(
      std::move(listener), std::move(event_cq), std::move(scheduler)));
  session->open_table_name_ = std::string(table_name);
  session->open_app_profile_id_ = std::string(app_profile_id);
  session->open_permission_ = permission;
  std::weak_ptr<SessionStreamListener> weak_stream_listener = session;
  session->stream_ =
      SessionStream::Create(stub, weak_stream_listener, *session->event_cq_,
                            std::move(refresh_config), std::move(metadata_hook),
                            std::move(open_config));

  if (start_open) {
    session->StartOpen();
  }
  return session;
}

void SessionImpl::StartOpen() {
  if (stream_ != nullptr) {
    stream_->StartOpen(open_table_name_, open_app_profile_id_,
                       open_permission_);
  }
}

void SessionImpl::Cancel() {
  state_.store(State::kClosing, std::memory_order_relaxed);
  if (stream_ != nullptr) {
    stream_->Cancel();
  }
}

std::shared_ptr<SessionImpl> SessionImpl::CreateForTesting(
    std::shared_ptr<SessionStream> stream,
    std::weak_ptr<SessionListener> listener,
    google::cloud::CompletionQueue event_cq, ScheduleFn scheduler) {
  auto session = std::shared_ptr<SessionImpl>(new SessionImpl(
      std::move(listener), std::move(event_cq), std::move(scheduler)));
  session->stream_ = std::move(stream);
  if (session->stream_ != nullptr) {
    session->stream_->listener_ = session;
  }
  session->state_.store(State::kReady);  // For easy mock injection testing
  return session;
}

SessionImpl::SessionImpl(std::weak_ptr<SessionListener> listener,
                         google::cloud::CompletionQueue event_cq,
                         ScheduleFn scheduler)
    : listener_(std::move(listener)),
      event_cq_(std::move(event_cq)),
      event_cq_impl_(
          ::google::cloud::internal::GetCompletionQueueImpl(*event_cq_).get()),
      scheduler_(std::move(scheduler)),
      last_heartbeat_time_ns_(ToNanos(Clock::now())) {
  last_active_ns_.store(ToNanos(Clock::now()));
  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      3, "SessionImpl",
      absl::StrCat("SessionImpl created: 0x",
                   absl::Hex(reinterpret_cast<std::uintptr_t>(this))));
}

SessionImpl::~SessionImpl() {
  // Safety net: a session must never be destroyed with a caller still waiting.
  // Fail the outstanding vRPC (retryable) and release the server stream.
  GenericCallback cb;
  bool write_completed = false;
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (active_rpc_.rpc_id.exchange(0, std::memory_order_acq_rel) != 0) {
      cb = std::move(active_rpc_.callback);
      write_completed =
          active_rpc_.write_completed.load(std::memory_order_acquire);
    }
  }
  if (!cb) return;
  // The server still holds this vRPC; release the stream so it is not leaked.
  if (stream_) stream_->Cancel();
  auto status = google::cloud::internal::UnavailableError("Session destroyed");
  if (!write_completed) {
    google::cloud::internal::SetPayload(status, kWriteStatusPayloadUrl, "0");
  }
  // Never run user/retry code from a destructor (the last reference may be
  // released under a pool lock): hand it to the event loop. The task does not
  // capture `this`.
  auto task = [cb = std::move(cb), status = std::move(status)]() mutable {
    cb(std::move(status));
  };
  if (event_cq_.has_value()) {
    event_cq_->RunAsync(std::move(task));
  } else {
    task();
  }
}

bool SessionImpl::IsRunningOnEventLoopThread() const {
  return event_cq_.has_value() && current_cq_impl == event_cq_impl_;
}

void SessionImpl::RunAsync(std::function<void()> task) {
  if (event_cq_.has_value()) {
    event_cq_->RunAsync(std::move(task));
  }
}

void SessionImpl::StartVirtualRpc(
    grpc::ClientContext* context,
    google::bigtable::v2::VirtualRpcRequest request, GenericCallback callback,
    bool is_write_request) {
  (void)is_write_request;
  last_active_ns_.store(ToNanos(Clock::now()), std::memory_order_relaxed);

  std::unique_lock<std::mutex> l(state_mutex_);
  if (state_.load(std::memory_order_relaxed) != State::kReady) {
    int const remaining =
        active_rpc_count_.fetch_sub(1, std::memory_order_acq_rel) - 1;
    bool should_close_stream = false;
    if (remaining == 0 &&
        active_rpc_.rpc_id.load(std::memory_order_relaxed) == 0 &&
        state_.load(std::memory_order_relaxed) == State::kDraining) {
      state_.store(State::kClosing, std::memory_order_release);
      should_close_stream = true;
    }
    l.unlock();
    if (should_close_stream) {
      InitiateStreamClose();
    }
    google::cloud::Status status = google::cloud::internal::UnavailableError(
        "Session is not ready for virtual RPCs");
    google::cloud::internal::SetPayload(status, kWriteStatusPayloadUrl, "0");
    MarkSessionTurnover(status);
    callback(status);
    return;
  }

  std::int64_t const rpc_id = request.rpc_id();
  std::int64_t expected = 0;
  if (!active_rpc_.rpc_id.compare_exchange_strong(expected, rpc_id,
                                                  std::memory_order_seq_cst)) {
    l.unlock();
    active_rpc_count_.fetch_sub(1, std::memory_order_relaxed);
    google::cloud::Status status =
        google::cloud::internal::ResourceExhaustedError(
            "No free slots in session");
    google::cloud::internal::SetPayload(status, kWriteStatusPayloadUrl, "0");
    callback(status);
    return;
  }
  last_dispatched_rpc_id_.store(
      (std::max)(last_dispatched_rpc_id_.load(std::memory_order_relaxed),
                 rpc_id),
      std::memory_order_release);

  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      4, "SessionImpl",
      absl::StrCat("Initiating vRPC, rpc_id: ", rpc_id,
                   ", state: ", static_cast<int>(state_.load())));

  active_rpc_.start_time = Clock::now();
  if (auto_finalize_.load(std::memory_order_acquire)) {
    auto wrapped = [this, cb = std::move(callback), rpc_id](auto resp) mutable {
      if (!this->IsCancelledAwaitingDrain(rpc_id)) {
        this->FinalizeActiveRpc(rpc_id);
      }
      std::move(cb)(std::move(resp));
    };
    active_rpc_.callback = std::move(wrapped);
  } else {
    active_rpc_.callback = std::move(callback);
  }
  active_rpc_.context = context;
  active_rpc_.sent_at_ns.store(kUnsetTimeNs, std::memory_order_release);
  last_sent_at_ns_.store(kUnsetTimeNs, std::memory_order_release);
  active_rpc_.write_completed.store(false, std::memory_order_release);
  active_rpc_.cancelled_awaiting_drain.store(false, std::memory_order_release);
  last_heartbeat_time_ns_.store(ToNanos(Clock::now()),
                                std::memory_order_relaxed);
  latency_tracker_.OnRequestStart();

  google::bigtable::v2::SessionRequest outer_request;
  *outer_request.mutable_virtual_rpc() = std::move(request);

  std::int64_t const prev_writing_rpc_id =
      writing_rpc_id_.exchange(rpc_id, std::memory_order_acq_rel);
  write_queue_size_.store(1, std::memory_order_release);

  l.unlock();

  std::int64_t const sent_ns = ToNanos(Clock::now());
  active_rpc_.sent_at_ns.store(sent_ns, std::memory_order_release);
  last_sent_at_ns_.store(sent_ns, std::memory_order_release);
  google::cloud::Status status = stream_->Write(std::move(outer_request));
  if (!status.ok()) {
    active_rpc_.sent_at_ns.store(kUnsetTimeNs, std::memory_order_release);
    last_sent_at_ns_.store(kUnsetTimeNs, std::memory_order_release);
    GenericCallback cb;
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      std::int64_t expected_writing = rpc_id;
      if (writing_rpc_id_.compare_exchange_strong(expected_writing,
                                                  prev_writing_rpc_id,
                                                  std::memory_order_acq_rel)) {
        if (prev_writing_rpc_id == 0) {
          write_queue_size_.store(0, std::memory_order_release);
        }
      }

      std::int64_t expected_active = rpc_id;
      if (active_rpc_.rpc_id.compare_exchange_strong(
              expected_active, 0, std::memory_order_seq_cst)) {
        cb = std::move(active_rpc_.callback);
        active_rpc_.callback = nullptr;
        active_rpc_.context = nullptr;
      }
      active_rpc_count_.fetch_sub(1, std::memory_order_relaxed);
      latency_tracker_.OnRequestFailed();
    }

    if (cb) {
      google::cloud::internal::SetPayload(status, kWriteStatusPayloadUrl, "0");
      cb(status);
    }
  }
}

void SessionImpl::FinalizeActiveRpc(std::int64_t rpc_id) {
  if (rpc_id == 0) return;
  bool should_close_stream = false;
  {
    std::lock_guard<std::mutex> l(state_mutex_);
    if (active_rpc_.rpc_id.load(std::memory_order_relaxed) == rpc_id) {
      if (active_rpc_.cancelled_awaiting_drain.load(
              std::memory_order_acquire)) {
        return;
      }
      active_rpc_.rpc_id.store(0, std::memory_order_release);
      active_rpc_.callback = nullptr;
      active_rpc_.context = nullptr;
      active_rpc_.write_completed.store(false, std::memory_order_release);
      active_rpc_.cancelled_awaiting_drain.store(false,
                                                 std::memory_order_release);
      active_rpc_count_.store(0, std::memory_order_release);

      if (state_.load(std::memory_order_relaxed) == State::kDraining) {
        state_.store(State::kClosing, std::memory_order_release);
        should_close_stream = true;
      }
    }
  }

  if (should_close_stream) {
    InitiateStreamClose();
  }
}

bool SessionImpl::Acquire() {
  if (state_.load(std::memory_order_acquire) != State::kReady) {
    return false;
  }
  if (writing_rpc_id_.load(std::memory_order_acquire) != 0 ||
      write_queue_size_.load(std::memory_order_acquire) > 0 ||
      (stream_ != nullptr && stream_->write_in_flight())) {
    return false;
  }
  int expected = 0;
  if (!active_rpc_count_.compare_exchange_strong(expected, 1,
                                                 std::memory_order_seq_cst)) {
    return false;
  }
  if (state_.load(std::memory_order_acquire) != State::kReady ||
      writing_rpc_id_.load(std::memory_order_acquire) != 0 ||
      write_queue_size_.load(std::memory_order_acquire) > 0 ||
      (stream_ != nullptr && stream_->write_in_flight())) {
    int const remaining =
        active_rpc_count_.fetch_sub(1, std::memory_order_seq_cst) - 1;
    if (remaining == 0 &&
        state_.load(std::memory_order_acquire) == State::kDraining) {
      bool should_close_stream = false;
      {
        std::lock_guard<std::mutex> l(state_mutex_);
        if (active_rpc_count_.load(std::memory_order_relaxed) == 0 &&
            active_rpc_.rpc_id.load(std::memory_order_relaxed) == 0 &&
            state_.load(std::memory_order_relaxed) == State::kDraining) {
          state_.store(State::kClosing, std::memory_order_release);
          should_close_stream = true;
        }
      }
      if (should_close_stream) {
        InitiateStreamClose();
      }
    }
    return false;
  }
  return true;
}

void SessionImpl::CancelVirtualRpc(std::int64_t rpc_id) {
  if (IsRunningOnEventLoopThread()) {
    CancelVirtualRpcSerialized(rpc_id);
  } else {
    RunAsync([weak_self = weak_from_this(), rpc_id]() {
      if (auto self = weak_self.lock()) {
        self->CancelVirtualRpcSerialized(rpc_id);
      }
    });
  }
}

bool SessionImpl::HasActiveContext(grpc::ClientContext* context) const {
  return active_rpc_.rpc_id.load(std::memory_order_acquire) != 0 &&
         active_rpc_.context == context;
}

void SessionImpl::CancelRpcByContext(grpc::ClientContext* context) {
  std::int64_t const rpc_id =
      active_rpc_.rpc_id.load(std::memory_order_acquire);
  if (rpc_id != 0 && active_rpc_.context == context) {
    CancelVirtualRpc(rpc_id);
  }
}

void SessionImpl::CancelVirtualRpcSerialized(std::int64_t rpc_id) {
  GenericCallback cb;
  bool was_write_completed = false;
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (active_rpc_.rpc_id.load(std::memory_order_acquire) == rpc_id &&
        active_rpc_.callback != nullptr) {
      was_write_completed =
          active_rpc_.write_completed.load(std::memory_order_acquire);
      if (was_write_completed) {
        // Drain-driven slot release: resolve the caller callback early without
        // tearing down the stream, while keeping the single-flight slot
        // occupied until the server's response frame arrives on the wire and
        // drains the slot.
        cb = std::move(active_rpc_.callback);
        active_rpc_.callback = nullptr;
        active_rpc_.context = nullptr;
        active_rpc_.cancelled_awaiting_drain.store(true,
                                                   std::memory_order_release);
        post_write_cancelling_rpc_id_.store(rpc_id, std::memory_order_release);
      } else {
        std::int64_t expected = rpc_id;
        if (active_rpc_.rpc_id.compare_exchange_strong(
                expected, 0, std::memory_order_seq_cst)) {
          cb = std::move(active_rpc_.callback);
          active_rpc_.callback = nullptr;
          active_rpc_.context = nullptr;
          active_rpc_count_.fetch_sub(1, std::memory_order_acq_rel);
          if (state_.load(std::memory_order_relaxed) != State::kClosed) {
            state_.store(State::kClosing, std::memory_order_release);
          }
        }
      }
    }
  }
  if (cb) {
    if (!was_write_completed) {
      latency_tracker_.OnRequestFailed();

      ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
          1, "SessionImpl",
          "Uncommitted Virtual RPC cancelled, tearing down stream.");
      stream_->Cancel();
    } else {
      ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
          2, "SessionImpl",
          absl::StrCat("Virtual RPC ", rpc_id,
                       " cancelled after write; keeping stream open until "
                       "wire response drains slot."));
    }

    google::cloud::Status status = google::cloud::internal::CancelledError(
        "Virtual RPC cancelled by client");
    google::cloud::internal::SetPayload(status, kWriteStatusPayloadUrl,
                                        was_write_completed ? "1" : "0");
    cb(status);
    if (was_write_completed) {
      std::int64_t expected = rpc_id;
      post_write_cancelling_rpc_id_.compare_exchange_strong(
          expected, 0, std::memory_order_acq_rel);
    }
  }
}

void SessionImpl::InitiateStreamClose() {
  google::bigtable::v2::CloseSessionRequest::CloseSessionReason reason;
  ScheduleFn scheduler;
  {
    std::lock_guard<std::mutex> l(state_mutex_);
    reason = close_reason_;
    scheduler = scheduler_;
  }
  stream_->Close(reason);
  if (scheduler != nullptr) {
    scheduler(std::chrono::milliseconds(200), [self = shared_from_this()]() {
      if (self->state_.load(std::memory_order_acquire) != State::kClosed &&
          self->stream_ != nullptr) {
        self->stream_->Cancel();
      }
    });
  }
}

void SessionImpl::Close() {
  {
    std::unique_lock<std::mutex> l(state_mutex_);
    if (state_.load(std::memory_order_relaxed) == State::kConnecting) {
      state_.store(State::kClosing, std::memory_order_release);
      l.unlock();
      stream_->Cancel();
      return;
    }
  }
  CloseSerialized();
}

void SessionImpl::CloseSerialized() {
  std::unique_lock<std::mutex> l(state_mutex_);
  State const current_state = state_.load(std::memory_order_relaxed);
  if (current_state == State::kClosed) {
    return;
  }
  state_.store(State::kClosing, std::memory_order_release);

  struct FailedRpc {
    GenericCallback callback;
    bool write_completed;
    std::int64_t rpc_id;
  };
  std::vector<FailedRpc> callbacks_to_fail;

  std::int64_t const last_admitted_rpc_id = last_admitted_rpc_id_;
  std::int64_t drained_rpc_id = 0;
  std::int64_t rpc_id = active_rpc_.rpc_id.load(std::memory_order_acquire);
  if (rpc_id != 0) {
    if (active_rpc_.rpc_id.compare_exchange_strong(rpc_id, 0,
                                                   std::memory_order_seq_cst)) {
      active_rpc_.context = nullptr;
      active_rpc_count_.fetch_sub(1, std::memory_order_acq_rel);
      latency_tracker_.OnRequestFailed();
      if (active_rpc_.cancelled_awaiting_drain.exchange(
              false, std::memory_order_acq_rel)) {
        drained_rpc_id = rpc_id;
      } else {
        callbacks_to_fail.push_back(
            {std::move(active_rpc_.callback),
             active_rpc_.write_completed.load(std::memory_order_acquire),
             rpc_id});
        active_rpc_.callback = nullptr;
      }
    }
  }
  auto listener = listener_.lock();

  l.unlock();

  if (drained_rpc_id != 0 && listener) {
    listener->OnSlotDrained(shared_from_this(), drained_rpc_id);
  }

  for (auto& failed_rpc : callbacks_to_fail) {
    if (failed_rpc.callback) {
      google::cloud::Status status =
          google::cloud::internal::UnavailableError("Session closed");
      MarkSessionTurnover(status);
      if (!failed_rpc.write_completed ||
          (last_admitted_rpc_id > 0 &&
           failed_rpc.rpc_id > last_admitted_rpc_id)) {
        google::cloud::internal::SetPayload(status, kWriteStatusPayloadUrl,
                                            "0");
      }
      failed_rpc.callback(status);
    }
  }

  InitiateStreamClose();
}

void SessionImpl::Drain() {
  {
    std::unique_lock<std::mutex> l(state_mutex_);
    if (state_.load(std::memory_order_relaxed) == State::kConnecting) {
      state_.store(State::kClosing, std::memory_order_release);
      l.unlock();
      stream_->Cancel();
      return;
    }
  }
  DrainSerialized();
}

void SessionImpl::DrainSerialized() {
  std::unique_lock<std::mutex> l(state_mutex_);
  State const current_state = state_.load(std::memory_order_relaxed);
  if (current_state != State::kReady && current_state != State::kDraining) {
    return;
  }

  bool const notify_go_away = (current_state == State::kReady);
  bool call_close = false;
  bool schedule_drain_timeout = false;
  ScheduleFn scheduler;
  if (active_rpc_count_.load(std::memory_order_relaxed) == 0 &&
      active_rpc_.rpc_id.load(std::memory_order_relaxed) == 0) {
    state_.store(State::kClosing, std::memory_order_release);
    call_close = true;
  } else {
    state_.store(State::kDraining, std::memory_order_release);
    schedule_drain_timeout = true;
    scheduler = scheduler_;
  }
  auto listener = notify_go_away ? listener_.lock() : nullptr;
  l.unlock();

  if (listener) {
    listener->OnGoAway(shared_from_this());
  }

  if (call_close) {
    InitiateStreamClose();
  } else if (schedule_drain_timeout) {
    if (scheduler != nullptr) {
      // Strong capture: once the pool drops a draining session, this timer is
      // its only owner. A weak capture let the session die with the in-flight
      // vRPC callback never invoked and the stream never cancelled (caller hung
      // until its deadline). The extension is bounded by the drain grace
      // timeout (default 30s).
      scheduler(effective_goaway_drain_timeout(), [self =
                                                       shared_from_this()]() {
        if (self->state_.load(std::memory_order_acquire) == State::kDraining) {
          self->CloseSerialized();
        }
      });
    } else {
      CloseSerialized();
    }
  }
}

SessionImpl::State SessionImpl::state() const { return state_.load(); }

int SessionImpl::active_rpc_count() const { return active_rpc_count_.load(); }

int SessionImpl::write_queue_size() const { return write_queue_size_.load(); }

std::string SessionImpl::state_str() const {
  switch (state_.load()) {
    case State::kConnecting:
      return "CONNECTING";
    case State::kReady:
      return "READY";
    case State::kDraining:
      return "DRAINING";
    case State::kClosing:
      return "CLOSING";
    case State::kClosed:
      return "CLOSED";
  }
  return "UNKNOWN";
}

bool SessionImpl::IsBusy() const {
  if (active_rpc_count_.load() >= 1) {
    return true;
  }
  return IsWriteCongested();
}

bool SessionImpl::IsWriteCongested() const {
  return write_queue_size_.load(std::memory_order_acquire) >=
             GetMaxWriteQueueSize() ||
         (stream_ != nullptr && stream_->write_in_flight());
}

bool SessionImpl::IsIdle(TimePoint now,
                         std::chrono::nanoseconds timeout) const {
  if (state() != State::kReady) return false;
  if (active_rpc_count_.load() > 0) return false;
  if (write_queue_size_.load() > 0) return false;
  return (now - GetLastActiveTime()) > timeout;
}

SessionImpl::TimePoint SessionImpl::GetLastActiveTime() const {
  return FromNanos(last_active_ns_.load());
}

void SessionImpl::SetAutoFinalize(bool enable) {
  auto_finalize_.store(enable, std::memory_order_release);
}

std::optional<google::bigtable::v2::SessionParametersResponse>
SessionImpl::session_parameters() const {
  std::lock_guard<std::mutex> l(state_mutex_);
  return session_params_;
}

std::optional<google::bigtable::v2::SessionRefreshConfig>
SessionImpl::refresh_config() const {
  std::lock_guard<std::mutex> l(state_mutex_);
  return refresh_config_;
}

std::optional<google::bigtable::v2::BackendIdentifier> SessionImpl::backend()
    const {
  std::lock_guard<std::mutex> l(state_mutex_);
  return backend_identifier_;
}

std::optional<std::string> SessionImpl::GetPeerId() const {
  std::lock_guard<std::mutex> l(state_mutex_);
  if (backend_identifier_.has_value() &&
      backend_identifier_->application_frontend_id() != 0) {
    return absl::StrCat(backend_identifier_->application_frontend_id());
  }
  if (peer_info_.has_value() && peer_info_->application_frontend_id != 0) {
    return absl::StrCat(peer_info_->application_frontend_id);
  }
  return std::nullopt;
}

void SessionImpl::OnPeerLoadInfo(PeerLoadInfo const& info) {
  {
    std::lock_guard<std::mutex> l(state_mutex_);
    peer_load_info_ = info;
  }
  if (info.weight > 0.0) {
    peer_weight_.store(1.0 / (std::max)(0.01, info.weight),
                       std::memory_order_relaxed);
  }
}

std::optional<SessionImpl::PeerLoadInfo> SessionImpl::peer_load_info() const {
  std::lock_guard<std::mutex> l(state_mutex_);
  return peer_load_info_;
}

std::optional<transport::PeerInfo> SessionImpl::peer_info() const {
  std::lock_guard<std::mutex> l(state_mutex_);
  return peer_info_;
}

std::optional<google::bigtable::v2::ClusterInformation>
SessionImpl::last_cluster_info() const {
  std::lock_guard<std::mutex> l(state_mutex_);
  return last_cluster_info_;
}

std::string SessionImpl::close_reason_str() const {
  std::lock_guard<std::mutex> l(state_mutex_);
  switch (close_reason_) {
    case google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_GOAWAY:
      return "GOAWAY";
    case google::bigtable::v2::CloseSessionRequest::
        CLOSE_SESSION_REASON_MISSED_HEARTBEAT:
      return "MISSED_HEARTBEAT";
    case google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_ERROR:
      return "ERROR";
    case google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_USER:
    default:
      if (!disconnect_status_.ok()) {
        return "ERROR";
      }
      return "USER";
  }
}

bool SessionImpl::IsCancelledAwaitingDrain(std::int64_t rpc_id) const {
  std::int64_t expected = rpc_id;
  bool const was_cancelling =
      post_write_cancelling_rpc_id_.compare_exchange_strong(
          expected, 0, std::memory_order_acq_rel);
  return was_cancelling ||
         (active_rpc_.rpc_id.load(std::memory_order_acquire) == rpc_id &&
          active_rpc_.cancelled_awaiting_drain.load(std::memory_order_acquire));
}

PeakEwmaTracker* SessionImpl::GetLatencyTracker() { return &latency_tracker_; }

google::bigtable::v2::Bigtable::Stub* SessionImpl::stub() const {
  return stream_ ? stream_->stub() : nullptr;
}

int SessionImpl::GetStubActiveCalls() const {
  if (auto listener = listener_.lock()) {
    return listener->GetActiveCallsForStub(stub());
  }
  return 0;
}

bool SessionImpl::in_idle_queue() const {
  return in_idle_queue_.load(std::memory_order_acquire);
}

void SessionImpl::set_in_idle_queue(bool v) {
  in_idle_queue_.store(v, std::memory_order_release);
}

void SessionImpl::OnInitialMetadata(
    std::multimap<grpc::string_ref, grpc::string_ref> const& metadata) {
  if (auto listener = listener_.lock()) {
    listener->OnInitialMetadata(shared_from_this(), metadata);
  }
  std::multimap<std::string, std::string> headers;
  for (auto const& [k, v] : metadata) {
    headers.emplace(std::string(k.data(), k.size()),
                    std::string(v.data(), v.size()));
  }
  auto parsed = transport::PeerInfo::FromHeaders(headers);
  if (parsed.present_in_headers || parsed.application_frontend_id != 0 ||
      parsed.google_frontend_id != 0 ||
      parsed.transport_type != transport::PeerInfo::TransportType::kUnknown) {
    std::lock_guard<std::mutex> l(state_mutex_);
    peer_info_ = parsed;
    if (!backend_identifier_.has_value()) {
      google::bigtable::v2::BackendIdentifier id;
      id.set_application_frontend_id(parsed.application_frontend_id);
      id.set_google_frontend_id(parsed.google_frontend_id);
      if (!parsed.application_frontend_zone.empty()) {
        id.set_application_frontend_zone(parsed.application_frontend_zone);
      } else if (!parsed.application_frontend_subzone.empty()) {
        id.set_application_frontend_zone(parsed.application_frontend_subzone);
      }
      backend_identifier_ = std::move(id);
    }
  }
}

void SessionImpl::OnSessionReady(
    google::bigtable::v2::OpenSessionResponse const& response) {
  if (IsRunningOnEventLoopThread()) {
    OnSessionReadySerialized(response);
  } else {
    RunAsync([weak_self = weak_from_this(), response]() {
      if (auto self = weak_self.lock()) {
        self->OnSessionReadySerialized(response);
      }
    });
  }
}

void SessionImpl::OnSessionReadySerialized(
    google::bigtable::v2::OpenSessionResponse const& response) {
  std::unique_lock<std::mutex> l(state_mutex_);
  if (response.has_backend()) {
    backend_identifier_ = response.backend();
    ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
        3, "SessionImpl",
        absl::StrCat("SessionImpl 0x",
                     absl::Hex(reinterpret_cast<std::uintptr_t>(this)),
                     " OnSessionReady set backend: ",
                     backend_identifier_->application_frontend_id()));
  } else {
    ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
        3, "SessionImpl",
        absl::StrCat("SessionImpl 0x",
                     absl::Hex(reinterpret_cast<std::uintptr_t>(this)),
                     " OnSessionReady - NO backend in response"));
  }
  State const current_state = state_.load(std::memory_order_relaxed);
  if (current_state != State::kConnecting && current_state != State::kReady) {
    return;
  }
  if (current_state == State::kConnecting) {
    state_.store(State::kReady, std::memory_order_release);
  }
  if (ready_at_ns_.load(std::memory_order_relaxed) == kUnsetTimeNs) {
    ready_at_ns_.store(ToNanos(Clock::now()), std::memory_order_release);
  }
  auto listener = listener_.lock();
  l.unlock();

  if (listener) {
    listener->OnSessionReady(shared_from_this(), response);
  }
}

void SessionImpl::OnMessage(google::bigtable::v2::SessionResponse message) {
  if (IsRunningOnEventLoopThread()) {
    OnMessageSerialized(std::move(message));
  } else {
    RunAsync(
        [weak_self = weak_from_this(), msg = std::move(message)]() mutable {
          if (auto self = weak_self.lock()) {
            self->OnMessageSerialized(std::move(msg));
          }
        });
  }
}

void SessionImpl::OnHeartbeat() {
  if (IsRunningOnEventLoopThread()) {
    std::int64_t const now_ns = ToNanos(Clock::now());
    last_heartbeat_time_ns_.store(now_ns, std::memory_order_relaxed);
    last_active_ns_.store(now_ns, std::memory_order_relaxed);
  } else {
    RunAsync([weak_self = weak_from_this()]() {
      if (auto self = weak_self.lock()) {
        std::int64_t const now_ns = ToNanos(Clock::now());
        self->last_heartbeat_time_ns_.store(now_ns, std::memory_order_relaxed);
        self->last_active_ns_.store(now_ns, std::memory_order_relaxed);
      }
    });
  }
}

void SessionImpl::OnMessageSerialized(
    google::bigtable::v2::SessionResponse message) {
  google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
      response_payload;
  bool deliver_vrpc_response = false;
  bool deliver_slot_drained = false;
  std::int64_t drained_rpc_id = 0;
  bool deliver_go_away = false;
  bool call_stream_close = false;
  GenericCallback callback;

  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      4, "SessionImpl",
      absl::StrCat(
          "[Session: 0x", absl::Hex(reinterpret_cast<std::uintptr_t>(this)),
          "] Received SessionResponse case: ", message.payload_case()));

  auto mark_known_frame = [this]() {
    std::int64_t const now_ns = ToNanos(Clock::now());
    last_heartbeat_time_ns_.store(now_ns, std::memory_order_relaxed);
    last_active_ns_.store(now_ns, std::memory_order_relaxed);
  };

  switch (message.payload_case()) {
    case google::bigtable::v2::SessionResponse::kSessionParameters: {
      mark_known_frame();
      {
        std::lock_guard<std::mutex> l(state_mutex_);
        session_params_ = std::move(*message.mutable_session_parameters());
      }
      ScheduleKeepAliveCheck();
      break;
    }

    case google::bigtable::v2::SessionResponse::kHeartbeat:
      mark_known_frame();
      break;

    case google::bigtable::v2::SessionResponse::kSessionRefreshConfig: {
      mark_known_frame();
      std::lock_guard<std::mutex> l(state_mutex_);
      refresh_config_ = std::move(*message.mutable_session_refresh_config());
      break;
    }

    case google::bigtable::v2::SessionResponse::PAYLOAD_NOT_SET: {
      // Check wire field 8 (SessionResponse.peer_load_info) in case the
      // server sends a PeerLoadInfo submessage over a googleapis proto snapshot
      // where field 8 is in UnknownFieldSet.
      auto const* reflection = message.GetReflection();
      if (reflection != nullptr) {
        auto const& unknown = reflection->GetUnknownFields(message);
        if (unknown.field_count() > 0) {
          google::protobuf::UnknownFieldSet sub_unknown;
          for (int i = 0; i < unknown.field_count(); ++i) {
            auto const& field = unknown.field(i);
            if (field.number() == 8 &&
                field.type() ==
                    google::protobuf::UnknownField::TYPE_LENGTH_DELIMITED) {
              mark_known_frame();
              if (sub_unknown.ParseFromString(field.length_delimited())) {
                PeerLoadInfo info;
                for (int j = 0; j < sub_unknown.field_count(); ++j) {
                  auto const& sf = sub_unknown.field(j);
                  if (sf.number() == 1 &&
                      sf.type() ==
                          google::protobuf::UnknownField::TYPE_VARINT) {
                    info.reason = static_cast<std::int64_t>(sf.varint());
                  } else if (sf.number() == 2 &&
                             sf.type() ==
                                 google::protobuf::UnknownField::TYPE_FIXED32) {
                    std::uint32_t const raw = sf.fixed32();
                    float w = 1.0F;
                    std::memcpy(&w, &raw, sizeof(float));
                    info.weight = static_cast<double>(w);
                  }
                }
                OnPeerLoadInfo(info);
              }
            }
          }
        }
      }
      break;
    }

    case google::bigtable::v2::SessionResponse::kVirtualRpc: {
      mark_known_frame();
      auto const& vrpc = message.virtual_rpc();
      std::int64_t const rpc_id = vrpc.rpc_id();
      {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (vrpc.has_cluster_info()) {
          last_cluster_info_ = vrpc.cluster_info();
        }
        if (active_rpc_.rpc_id.load(std::memory_order_acquire) == rpc_id) {
          if (active_rpc_.cancelled_awaiting_drain.load(
                  std::memory_order_acquire)) {
            active_rpc_.rpc_id.store(0, std::memory_order_release);
            active_rpc_.cancelled_awaiting_drain.store(
                false, std::memory_order_release);
            active_rpc_.write_completed.store(false, std::memory_order_release);
            active_rpc_count_.store(0, std::memory_order_release);
            latency_tracker_.OnRequestFailed();
            deliver_slot_drained = true;
            drained_rpc_id = rpc_id;
          } else if (active_rpc_.callback != nullptr) {
            response_payload = std::move(*message.mutable_virtual_rpc());
            deliver_vrpc_response = true;
            ok_vrpc_count_.fetch_add(1, std::memory_order_relaxed);

            auto const latency =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    Clock::now() - active_rpc_.start_time);
            latency_tracker_.OnRequestComplete(latency);
            callback = std::move(active_rpc_.callback);
            active_rpc_.callback = nullptr;
            active_rpc_.context = nullptr;
          }
        }
      }
      break;
    }

    case google::bigtable::v2::SessionResponse::kError: {
      mark_known_frame();
      auto const& err = message.error();
      std::int64_t const rpc_id = err.rpc_id();
      if (state_.load(std::memory_order_acquire) == State::kConnecting ||
          rpc_id == 0) {
        {
          std::lock_guard<std::mutex> lock(state_mutex_);
          if (err.has_cluster_info()) {
            last_cluster_info_ = err.cluster_info();
          }
          if (state_.load(std::memory_order_relaxed) != State::kClosed) {
            state_.store(State::kClosing, std::memory_order_release);
          }
          if (disconnect_status_.ok()) {
            disconnect_status_ = google::cloud::Status(
                static_cast<google::cloud::StatusCode>(err.status().code()),
                err.status().message());
          }
          close_reason_ = google::bigtable::v2::CloseSessionRequest::
              CLOSE_SESSION_REASON_ERROR;
        }
        stream_->Cancel();
        return;
      }
      {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (err.has_cluster_info()) {
          last_cluster_info_ = err.cluster_info();
        }
        if (active_rpc_.rpc_id.load(std::memory_order_acquire) == rpc_id) {
          if (active_rpc_.cancelled_awaiting_drain.load(
                  std::memory_order_acquire)) {
            active_rpc_.rpc_id.store(0, std::memory_order_release);
            active_rpc_.cancelled_awaiting_drain.store(
                false, std::memory_order_release);
            active_rpc_.write_completed.store(false, std::memory_order_release);
            active_rpc_count_.store(0, std::memory_order_release);
            latency_tracker_.OnRequestFailed();
            deliver_slot_drained = true;
            drained_rpc_id = rpc_id;
          } else if (active_rpc_.callback != nullptr) {
            google::cloud::Status err_status(
                static_cast<google::cloud::StatusCode>(err.status().code()),
                err.status().message());
            if (last_admitted_rpc_id_ > 0 && rpc_id > last_admitted_rpc_id_) {
              MarkSessionTurnover(err_status);
              google::cloud::internal::SetPayload(err_status,
                                                  kWriteStatusPayloadUrl, "0");
            } else {
              MarkServerResult(err_status);
              if (state_.load(std::memory_order_relaxed) == State::kDraining) {
                MarkSessionTurnover(err_status);
              }
            }
            if (err.has_retry_info()) {
              std::chrono::nanoseconds retry_delay(0);
              if (err.retry_info().has_retry_delay()) {
                auto const& d = err.retry_info().retry_delay();
                retry_delay = std::chrono::seconds(d.seconds()) +
                              std::chrono::nanoseconds(d.nanos());
              }
              SetServerRetryDelay(err_status, retry_delay);
            } else if (!err.status().details().empty()) {
              google::rpc::RetryInfo retry_info;
              for (auto const& detail : err.status().details()) {
                if (detail.UnpackTo(&retry_info)) {
                  std::chrono::nanoseconds retry_delay(0);
                  if (retry_info.has_retry_delay()) {
                    auto const& d = retry_info.retry_delay();
                    retry_delay = std::chrono::seconds(d.seconds()) +
                                  std::chrono::nanoseconds(d.nanos());
                  }
                  SetServerRetryDelay(err_status, retry_delay);
                  break;
                }
              }
            }
            response_payload = std::move(err_status);
            deliver_vrpc_response = true;
            error_vrpc_count_.fetch_add(1, std::memory_order_relaxed);

            latency_tracker_.OnRequestFailed();
            callback = std::move(active_rpc_.callback);
            active_rpc_.callback = nullptr;
            active_rpc_.context = nullptr;
          }
        }
      }
      break;
    }

    case google::bigtable::v2::SessionResponse::kGoAway: {
      mark_known_frame();
      ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
          2, "SessionImpl",
          absl::StrCat("[Session: 0x",
                       absl::Hex(reinterpret_cast<std::uintptr_t>(this)),
                       "] Received GoAway (reason=", message.go_away().reason(),
                       ", description=", message.go_away().description(),
                       ", last_rpc_id_admitted=",
                       message.go_away().last_rpc_id_admitted(), ")"));
      bool entered_draining = false;
      ScheduleFn scheduler;
      {
        std::lock_guard<std::mutex> lock(state_mutex_);
        State const cur_state = state_.load(std::memory_order_relaxed);
        if (cur_state == State::kClosing || cur_state == State::kClosed) {
          break;
        }
        if (message.go_away().last_rpc_id_admitted() > 0) {
          last_admitted_rpc_id_ = message.go_away().last_rpc_id_admitted();
        }
        close_reason_ = google::bigtable::v2::CloseSessionRequest::
            CLOSE_SESSION_REASON_GOAWAY;
        std::int64_t current_id =
            active_rpc_.rpc_id.load(std::memory_order_acquire);
        if (current_id > 0 && last_admitted_rpc_id_ > 0 &&
            current_id > last_admitted_rpc_id_ &&
            writing_rpc_id_.load(std::memory_order_acquire) == 0) {
          if (active_rpc_.rpc_id.compare_exchange_strong(
                  current_id, 0, std::memory_order_seq_cst)) {
            active_rpc_.context = nullptr;
            active_rpc_.write_completed.store(false, std::memory_order_release);
            active_rpc_count_.fetch_sub(1, std::memory_order_acq_rel);
            if (active_rpc_.cancelled_awaiting_drain.exchange(
                    false, std::memory_order_acq_rel)) {
              latency_tracker_.OnRequestFailed();
              deliver_slot_drained = true;
              drained_rpc_id = current_id;
            } else if (active_rpc_.callback != nullptr) {
              google::cloud::Status unadm_status =
                  google::cloud::internal::UnavailableError(
                      "Session received GoAway and vRPC was not admitted");
              MarkSessionTurnover(unadm_status);
              google::cloud::internal::SetPayload(unadm_status,
                                                  kWriteStatusPayloadUrl, "0");
              response_payload = std::move(unadm_status);
              deliver_vrpc_response = true;
              latency_tracker_.OnRequestFailed();
              callback = std::move(active_rpc_.callback);
              active_rpc_.callback = nullptr;
            }
          }
        }
        if (cur_state == State::kReady || cur_state == State::kConnecting) {
          if (active_rpc_count_.load(std::memory_order_acquire) == 0 &&
              active_rpc_.rpc_id.load(std::memory_order_acquire) == 0 &&
              write_queue_.empty()) {
            state_.store(State::kClosing, std::memory_order_release);
            call_stream_close = true;
          } else {
            state_.store(State::kDraining, std::memory_order_release);
            entered_draining = true;
            scheduler = scheduler_;
          }
          deliver_go_away = true;
        } else if (cur_state == State::kDraining &&
                   active_rpc_count_.load(std::memory_order_acquire) == 0 &&
                   active_rpc_.rpc_id.load(std::memory_order_acquire) == 0 &&
                   write_queue_.empty()) {
          state_.store(State::kClosing, std::memory_order_release);
          call_stream_close = true;
        }
      }
      if (entered_draining && scheduler != nullptr) {
        // Strong capture keeps the draining session alive until its bounded
        // deadline (default 30s; see DrainSerialized()).
        scheduler(effective_goaway_drain_timeout(),
                  [self = shared_from_this()]() {
                    if (self->state_.load(std::memory_order_acquire) ==
                        State::kDraining) {
                      self->CloseSerialized();
                    }
                  });
      }
      break;
    }

    default:
      break;
  }

  if ((deliver_vrpc_response || deliver_slot_drained) &&
      state_.load(std::memory_order_relaxed) == State::kDraining &&
      active_rpc_count_.load(std::memory_order_relaxed) == 0) {
    state_.store(State::kClosing, std::memory_order_release);
    call_stream_close = true;
  }

  if (call_stream_close) {
    InitiateStreamClose();
  }

  if (deliver_vrpc_response && callback) {
    callback(std::move(response_payload));
  }

  if (deliver_slot_drained) {
    if (auto listener = listener_.lock()) {
      listener->OnSlotDrained(shared_from_this(), drained_rpc_id);
    }
  }

  if (deliver_go_away) {
    if (auto listener = listener_.lock()) {
      listener->OnGoAway(shared_from_this());
    }
  }
}

std::chrono::nanoseconds SessionImpl::GetKeepAliveInterval() const {
  if (!session_params_.has_value() || !session_params_->has_keep_alive()) {
    return std::chrono::nanoseconds(0);
  }
  return std::chrono::seconds(session_params_->keep_alive().seconds()) +
         std::chrono::nanoseconds(session_params_->keep_alive().nanos());
}

void SessionImpl::ScheduleKeepAliveCheck() {
  std::chrono::nanoseconds interval(0);
  ScheduleFn scheduler;
  {
    std::lock_guard<std::mutex> l(state_mutex_);
    State const cur_state = state_.load(std::memory_order_relaxed);
    if (cur_state == State::kClosed || cur_state == State::kClosing) {
      return;
    }
    interval = GetKeepAliveInterval();
    scheduler = scheduler_;
  }
  if (interval.count() <= 0 || scheduler == nullptr) {
    return;
  }

  scheduler(interval, [weak_self = weak_from_this()]() {
    TimePoint const scheduled_time = Clock::now();
    if (auto self = weak_self.lock()) {
      self->RunAsync([weak_self, scheduled_time]() {
        if (auto s = weak_self.lock()) {
          s->CheckKeepAliveSerialized(scheduled_time);
        }
      });
    }
  });
}

void SessionImpl::CheckKeepAliveSerialized(TimePoint scheduled_time) {
  State const cur_state = state_.load(std::memory_order_acquire);
  if (cur_state == State::kConnecting) {
    // SessionParameters can arrive before OpenSessionResponse transitions the
    // session to kReady. Keep rescheduling the watchdog while connecting so it
    // is not permanently dropped.
    last_heartbeat_time_ns_.store(ToNanos(scheduled_time),
                                  std::memory_order_relaxed);
    ScheduleKeepAliveCheck();
    return;
  }
  if (cur_state != State::kReady) return;

  // The server only sends keepalive heartbeats while a virtual RPC is in flight
  // on the session. When the session is idle, refresh `last_heartbeat_time_ns_`
  // and reschedule without terminating.
  if (active_rpc_count_.load(std::memory_order_acquire) == 0 &&
      active_rpc_.rpc_id.load(std::memory_order_acquire) == 0) {
    last_heartbeat_time_ns_.store(ToNanos(scheduled_time),
                                  std::memory_order_relaxed);
    ScheduleKeepAliveCheck();
    return;
  }

  std::chrono::nanoseconds interval(0);
  {
    std::lock_guard<std::mutex> l(state_mutex_);
    interval = GetKeepAliveInterval();
  }
  double const multiplier = effective_keepalive_timeout_multiplier();
  auto const keep_alive_timeout =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::duration<double, std::nano>(
              static_cast<double>(interval.count()) * multiplier));
  TimePoint const last_heartbeat =
      FromNanos(last_heartbeat_time_ns_.load(std::memory_order_relaxed));

  if (scheduled_time - last_heartbeat > keep_alive_timeout) {
    ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
        1, "SessionImpl",
        absl::StrCat("[Session: 0x",
                     absl::Hex(reinterpret_cast<std::uintptr_t>(this)),
                     "] Missed server heartbeats. Terminating session stream "
                     "context."));
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      if (state_.load(std::memory_order_relaxed) != State::kClosed) {
        state_.store(State::kClosing, std::memory_order_release);
      }
      if (disconnect_status_.ok()) {
        disconnect_status_ = google::cloud::internal::UnavailableError(
            "Missed server keepalive heartbeat boundaries");
      }
      close_reason_ = google::bigtable::v2::CloseSessionRequest::
          CLOSE_SESSION_REASON_MISSED_HEARTBEAT;
    }
    stream_->Cancel();
  } else {
    ScheduleKeepAliveCheck();
  }
}

void SessionImpl::OnWriteDone(bool ok) {
  if (IsRunningOnEventLoopThread()) {
    OnWriteDoneSerialized(ok);
  } else {
    RunAsync([weak_self = weak_from_this(), ok]() {
      if (auto self = weak_self.lock()) {
        self->OnWriteDoneSerialized(ok);
      }
    });
  }
}

void SessionImpl::OnWriteDoneSerialized(bool ok) {
  std::unique_lock<std::mutex> l(state_mutex_);
  if (state_.load(std::memory_order_relaxed) == State::kClosed) {
    return;
  }
  std::int64_t const completed_write_rpc_id =
      writing_rpc_id_.exchange(0, std::memory_order_acq_rel);
  if (write_queue_size_.load(std::memory_order_relaxed) > 0) {
    write_queue_size_.fetch_sub(1, std::memory_order_release);
  }

  if (!ok && state_.load(std::memory_order_relaxed) != State::kClosed) {
    state_.store(State::kClosing, std::memory_order_release);
  }

  GenericCallback error_callback;
  google::cloud::Status error_status;
  std::int64_t drained_rpc_id = 0;
  bool should_close_stream = false;

  if (completed_write_rpc_id != 0) {
    if (active_rpc_.rpc_id.load(std::memory_order_acquire) ==
        completed_write_rpc_id) {
      bool const unadmitted_by_goaway =
          ok && state_.load(std::memory_order_relaxed) == State::kDraining &&
          last_admitted_rpc_id_ > 0 &&
          completed_write_rpc_id > last_admitted_rpc_id_;
      if (ok && !unadmitted_by_goaway) {
        active_rpc_.write_completed.store(true, std::memory_order_release);
      } else {
        if (!ok) {
          ::google::cloud::bigtable_internal::sessions::internal::
              LogVerboseWarning(
                  1, "SessionImpl",
                  absl::StrCat("vRPC write failed on socket, rpc_id: ",
                               completed_write_rpc_id));
          error_status =
              google::cloud::internal::UnavailableError("Write failed");
        } else {
          error_status = google::cloud::internal::UnavailableError(
              "Session received GoAway and vRPC was not admitted");
          MarkSessionTurnover(error_status);
        }
        google::cloud::internal::SetPayload(error_status,
                                            kWriteStatusPayloadUrl, "0");

        std::int64_t expected = completed_write_rpc_id;
        if (active_rpc_.rpc_id.compare_exchange_strong(
                expected, 0, std::memory_order_seq_cst)) {
          active_rpc_.context = nullptr;
          active_rpc_.write_completed.store(false, std::memory_order_release);
          active_rpc_count_.fetch_sub(1, std::memory_order_acq_rel);
          if (active_rpc_.cancelled_awaiting_drain.exchange(
                  false, std::memory_order_acq_rel)) {
            latency_tracker_.OnRequestFailed();
            drained_rpc_id = completed_write_rpc_id;
          } else if (active_rpc_.callback != nullptr) {
            error_callback = std::move(active_rpc_.callback);
            active_rpc_.callback = nullptr;
            latency_tracker_.OnRequestFailed();
          }
        }
        if (unadmitted_by_goaway &&
            active_rpc_count_.load(std::memory_order_relaxed) == 0 &&
            active_rpc_.rpc_id.load(std::memory_order_relaxed) == 0 &&
            write_queue_.empty()) {
          state_.store(State::kClosing, std::memory_order_release);
          should_close_stream = true;
        }
      }
    }
  }

  auto listener = listener_.lock();
  l.unlock();

  if (should_close_stream) {
    InitiateStreamClose();
  }

  if (drained_rpc_id != 0 && listener) {
    listener->OnSlotDrained(shared_from_this(), drained_rpc_id);
  }

  if (error_callback) {
    error_callback(error_status);
  }

  if (!ok) {
    if (stream_ != nullptr) {
      stream_->Cancel();
    }
    return;
  }

  if (listener) {
    listener->OnWriteDone(shared_from_this());
  }
}

void SessionImpl::OnClosed(google::cloud::Status status) {
  if (IsRunningOnEventLoopThread()) {
    OnClosedSerialized(std::move(status));
  } else {
    RunAsync(
        [weak_self = weak_from_this(), status = std::move(status)]() mutable {
          if (auto self = weak_self.lock()) {
            self->OnClosedSerialized(std::move(status));
          }
        });
  }
}

void SessionImpl::OnClosedSerialized(google::cloud::Status status) {
  std::unique_lock<std::mutex> l(state_mutex_);
  State const prev_state = state_.load(std::memory_order_relaxed);
  if (prev_state == State::kClosed) {
    return;
  }
  bool const has_explicit_disconnect_status = !disconnect_status_.ok();
  if (has_explicit_disconnect_status) {
    status = disconnect_status_;
  }
  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      3, "SessionImpl",
      absl::StrCat("OnClosedSerialized - Session stream terminated, status: ",
                   status.message()));

  state_.store(State::kClosed, std::memory_order_release);
  writing_rpc_id_.store(0, std::memory_order_release);
  write_queue_size_.store(0, std::memory_order_release);
  last_active_ns_.store(ToNanos(Clock::now()), std::memory_order_relaxed);

  struct FailedRpc {
    GenericCallback callback;
    bool write_completed;
    std::int64_t rpc_id;
  };
  std::vector<FailedRpc> callbacks_to_fail;

  std::int64_t const last_admitted_rpc_id = last_admitted_rpc_id_;
  std::int64_t drained_rpc_id = 0;
  std::int64_t rpc_id = active_rpc_.rpc_id.load(std::memory_order_acquire);
  if (rpc_id != 0) {
    if (active_rpc_.rpc_id.compare_exchange_strong(rpc_id, 0,
                                                   std::memory_order_seq_cst)) {
      active_rpc_.context = nullptr;
      active_rpc_count_.fetch_sub(1, std::memory_order_acq_rel);
      latency_tracker_.OnRequestFailed();
      if (active_rpc_.cancelled_awaiting_drain.exchange(
              false, std::memory_order_acq_rel)) {
        drained_rpc_id = rpc_id;
      } else {
        callbacks_to_fail.push_back(
            {std::move(active_rpc_.callback),
             active_rpc_.write_completed.load(std::memory_order_acquire),
             rpc_id});
        active_rpc_.callback = nullptr;
      }
    }
  }

  scheduler_ = nullptr;

  auto listener = listener_.lock();

  l.unlock();

  if (drained_rpc_id != 0 && listener) {
    listener->OnSlotDrained(shared_from_this(), drained_rpc_id);
  }

  google::cloud::Status base_fail_status = status;
  if (prev_state != State::kConnecting && !has_explicit_disconnect_status &&
      status.code() != google::cloud::StatusCode::kUnimplemented) {
    base_fail_status = google::cloud::internal::UnavailableError(
        status.ok()
            ? "Session transport terminated"
            : absl::StrCat("Session transport terminated: ", status.message()));
  } else if (base_fail_status.ok()) {
    base_fail_status = google::cloud::internal::UnavailableError(
        "Session transport terminated");
  }

  // Cleanly closed Ready/Draining/Closing sessions (e.g., GOAWAY drain, idle
  // eviction, or user/pool Close) report OK to the pool listener so
  // `session/durations` records `status="OK"`, while any in-flight vRPCs still
  // receive `base_fail_status` (`UNAVAILABLE`).
  google::cloud::Status const close_status_for_listener =
      (prev_state != State::kConnecting && !has_explicit_disconnect_status &&
       status.ok())
          ? status
          : base_fail_status;

  for (auto& failed_rpc : callbacks_to_fail) {
    if (failed_rpc.callback) {
      google::cloud::Status fail_status = base_fail_status;
      MarkSessionTurnover(fail_status);
      if (!failed_rpc.write_completed ||
          (last_admitted_rpc_id > 0 &&
           failed_rpc.rpc_id > last_admitted_rpc_id)) {
        google::cloud::internal::SetPayload(fail_status, kWriteStatusPayloadUrl,
                                            "0");
      }
      failed_rpc.callback(fail_status);
    }
  }

  if (listener) {
    listener->OnSessionClosed(shared_from_this(), close_status_for_listener);
  }
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

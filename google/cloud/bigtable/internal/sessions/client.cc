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

#include "google/cloud/bigtable/internal/sessions/client.h"
#include "google/cloud/bigtable/internal/sessions/latch.h"
#include "google/cloud/bigtable/internal/sessions/logging_helper.h"
#include "google/cloud/bigtable/internal/sessions/sessions_options.h"
#include "absl/strings/str_cat.h"
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/duration.pb.h>
#include <grpcpp/client_context.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
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

using WallClock = std::chrono::system_clock;
using WallTimePoint = WallClock::time_point;

constexpr auto kDefaultOperationTimeout = std::chrono::seconds(60);
constexpr auto kSyncGrace = std::chrono::seconds(2);

WallTimePoint ContextDeadline(grpc::ClientContext const* context) {
  if (context == nullptr) {
    return (WallTimePoint::max)();
  }
  return context->deadline();
}

WallTimePoint EffectiveDeadline(
    grpc::ClientContext const* context,
    middleware::RetryingVRpc::CallPolicy const& policy) {
  return std::min(ContextDeadline(context), policy.deadline);
}

google::protobuf::Duration CalculateVrpcDeadline(WallTimePoint deadline) {
  google::protobuf::Duration proto_duration;
  std::chrono::nanoseconds duration = kDefaultOperationTimeout;
  if (deadline != (WallTimePoint::max)()) {
    duration = std::max(std::chrono::duration_cast<std::chrono::nanoseconds>(
                            deadline - WallClock::now()),
                        std::chrono::nanoseconds::zero());
  }
  std::int64_t const nanos = duration.count();
  proto_duration.set_seconds(nanos / 1000000000LL);
  proto_duration.set_nanos(static_cast<std::int32_t>(nanos % 1000000000LL));
  return proto_duration;
}

thread_local std::shared_ptr<middleware::RetryingVRpc>* g_sync_loop_sink =
    nullptr;

}  // namespace

Client::Client(SyncWaiter::Config const& sync_wait, SyncSpinGate* spin_gate)
    : read_waiter_(sync_wait, spin_gate), write_waiter_(sync_wait, spin_gate) {}

std::shared_ptr<Client> Client::Create(
    std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
    std::string_view table_name, std::string_view app_profile_id,
    google::bigtable::v2::OpenTableRequest::Permission permission,
    google::cloud::Options options, ScheduleFn scheduler) {
  auto client = std::shared_ptr<Client>(
      new Client(SyncWaiter::ConfigFromOptions(options), nullptr));
  client->scheduler_ = std::move(scheduler);
  client->pool_ = SessionPoolImpl::Create(
      std::move(stubs), table_name, app_profile_id, permission,
      std::move(options), client->scheduler_);
  return client;
}

std::shared_ptr<Client> Client::Create(
    std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
    std::string_view authorized_view_name, std::string_view app_profile_id,
    google::bigtable::v2::OpenAuthorizedViewRequest::Permission permission,
    google::cloud::Options options, ScheduleFn scheduler) {
  auto client = std::shared_ptr<Client>(
      new Client(SyncWaiter::ConfigFromOptions(options), nullptr));
  client->scheduler_ = std::move(scheduler);
  options.set<SessionsOpenAuthorizedViewPermissionOption>(permission);
  client->pool_ = SessionPoolImpl::Create(
      std::move(stubs), authorized_view_name, app_profile_id, permission,
      std::move(options), client->scheduler_);
  return client;
}

std::shared_ptr<Client> Client::Create(
    std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
    std::string_view materialized_view_name, std::string_view app_profile_id,
    google::bigtable::v2::OpenMaterializedViewRequest::Permission permission,
    google::cloud::Options options, ScheduleFn scheduler) {
  auto client = std::shared_ptr<Client>(
      new Client(SyncWaiter::ConfigFromOptions(options), nullptr));
  client->scheduler_ = std::move(scheduler);
  options.set<SessionsOpenMaterializedViewPermissionOption>(permission);
  client->pool_ = SessionPoolImpl::Create(
      std::move(stubs), materialized_view_name, app_profile_id, permission,
      std::move(options), client->scheduler_);
  return client;
}

std::shared_ptr<Client> Client::CreateWithPool(
    std::shared_ptr<SessionPoolImpl> pool, ScheduleFn scheduler,
    SyncWaiter::Config sync_wait, SyncSpinGate* spin_gate) {
  auto client = std::shared_ptr<Client>(new Client(sync_wait, spin_gate));
  client->pool_ = std::move(pool);
  client->scheduler_ = std::move(scheduler);
  return client;
}

std::shared_ptr<Client> Client::CreateForTesting(
    std::shared_ptr<SessionPoolImpl> pool, ScheduleFn scheduler,
    SyncWaiter::Config sync_wait, SyncSpinGate* spin_gate) {
  return CreateWithPool(std::move(pool), std::move(scheduler), sync_wait,
                        spin_gate);
}

std::int64_t Client::AllocateRpcId() {
  if (pool_ != nullptr) {
    return pool_->AllocateRpcId();
  }
  return next_rpc_id_.fetch_add(1, std::memory_order_relaxed);
}

std::shared_ptr<middleware::RetryingVRpc> Client::StartOperation(
    grpc::ClientContext* context, std::string serialized_payload,
    bool is_write_request, bool is_idempotent,
    middleware::RetryingVRpc::CallPolicy policy,
    middleware::RetryingVRpc::GenericCallback generic_cb) {
  if (pool_ != nullptr && context != nullptr) {
    pool_->ClearCancelledContext(context);
  }
  {
    std::lock_guard<std::mutex> l(mutex_);
    backend_latency_ = std::nullopt;
    cluster_info_ = std::nullopt;
  }
  policy.deadline = EffectiveDeadline(context, policy);
  WallTimePoint const deadline = policy.deadline;

  auto attempt_counter = std::make_shared<std::atomic<std::int64_t>>(1);
  auto on_attempt_start = policy.on_attempt_start;
  auto on_attempt_complete = policy.on_attempt_complete;
  auto operation_factory = [w_pool = std::weak_ptr<SessionPoolImpl>(pool_),
                            payload = std::move(serialized_payload), context,
                            deadline, is_write_request, attempt_counter,
                            on_attempt_start = std::move(on_attempt_start),
                            on_attempt_complete =
                                std::move(on_attempt_complete),
                            w_client = weak_from_this()](
                               middleware::RetryingVRpc::GenericCallback
                                   attempt_cb) {
    if (on_attempt_start) {
      on_attempt_start();
    }
    auto const attempt_wall_start = WallClock::now();
    auto const attempt_steady_start = SessionImpl::Clock::now();
    auto report_and_cb =
        [attempt_cb = std::move(attempt_cb), on_attempt_complete,
         attempt_steady_start, w_client](
            google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
                resp) mutable {
          auto const* session_info =
              SessionPoolImpl::CurrentCompletedAttemptSessionInfo();
          std::optional<google::bigtable::v2::ClusterInformation>
              attempt_cluster_info;
          if (resp.ok() && resp->has_cluster_info()) {
            attempt_cluster_info = resp->cluster_info();
          } else if (session_info != nullptr &&
                     session_info->cluster_info.has_value()) {
            attempt_cluster_info = session_info->cluster_info;
          }
          if (attempt_cluster_info.has_value()) {
            if (auto c = w_client.lock()) {
              std::lock_guard<std::mutex> l(c->mutex_);
              c->cluster_info_ = *attempt_cluster_info;
            }
          }
          if (on_attempt_complete) {
            middleware::RetryingVRpc::AttemptTelemetry telemetry;
            telemetry.status = resp.status();
            telemetry.delivered_response = resp.ok();
            if (attempt_cluster_info.has_value()) {
              telemetry.cluster_id = attempt_cluster_info->cluster_id();
              telemetry.zone_id = attempt_cluster_info->zone_id();
            }
            if (session_info != nullptr) {
              if (session_info->peer_info.has_value()) {
                telemetry.has_peer_info = true;
                telemetry.transport_type =
                    transport::PeerInfo::TransportTypeToString(
                        session_info->peer_info->transport_type);
                telemetry.transport_region =
                    session_info->peer_info->application_frontend_region;
                telemetry.transport_zone =
                    session_info->peer_info->application_frontend_zone;
                telemetry.transport_subzone =
                    session_info->peer_info->application_frontend_subzone;
              }
              if (session_info->sent_at.has_value()) {
                telemetry.throttling_latency = std::max(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        *session_info->sent_at - attempt_steady_start),
                    std::chrono::nanoseconds::zero());
              }
            }
            on_attempt_complete(telemetry);
          }
          attempt_cb(std::move(resp));
        };
    auto client = w_client.lock();
    if (!client) {
      report_and_cb(
          google::cloud::internal::CancelledError("Client destroyed"));
      return;
    }
    auto pool = w_pool.lock();
    if (!pool) {
      report_and_cb(google::cloud::internal::FailedPreconditionError(
          "Client has no session pool"));
      return;
    }
    std::int64_t const rpc_id = client->AllocateRpcId();
    std::int64_t const attempt_number =
        attempt_counter->fetch_add(1, std::memory_order_relaxed);
    google::bigtable::v2::VirtualRpcRequest v_request;
    v_request.set_rpc_id(rpc_id);
    v_request.set_payload(payload);
    *v_request.mutable_deadline() = CalculateVrpcDeadline(deadline);
    auto* meta = v_request.mutable_metadata();
    meta->set_attempt_number(attempt_number);
    std::int64_t const now_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            attempt_wall_start.time_since_epoch())
            .count();
    meta->mutable_attempt_start()->set_seconds(now_ns / 1000000000LL);
    meta->mutable_attempt_start()->set_nanos(
        static_cast<std::int32_t>(now_ns % 1000000000LL));

    google::cloud::Status status =
        pool->StartVirtualRpc(context, std::move(v_request),
                              std::move(report_and_cb), is_write_request);
    if (!status.ok()) {
      ::google::cloud::bigtable_internal::sessions::internal::LogVerboseWarning(
          2, "Client",
          absl::StrCat("StartVirtualRpc failed: ", status.message()));
    }
  };

  auto cancellable_op = [deadline, factory = std::move(operation_factory)](
                            middleware::RetryingVRpc::GenericCallback cb) {
    middleware::CancellableVRpc::Intercept(deadline, factory, std::move(cb));
  };

  auto loop_holder =
      std::make_shared<std::weak_ptr<middleware::RetryingVRpc>>();

  auto wrapped_cb =
      [w_client = weak_from_this(), context, loop_holder,
       cb = std::move(generic_cb)](
          google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              v_resp) mutable {
        std::shared_ptr<middleware::RetryingVRpc> removed_loop;
        if (auto client = w_client.lock()) {
          auto self_loop = loop_holder->lock();
          std::lock_guard<std::mutex> l(client->mutex_);
          if (v_resp.ok() && v_resp->has_cluster_info()) {
            client->cluster_info_ = v_resp->cluster_info();
          }
          if (context != nullptr) {
            auto it = client->active_retry_loops_.find(context);
            if (it != client->active_retry_loops_.end()) {
              auto& vec = it->second;
              for (auto vit = vec.begin(); vit != vec.end(); ++vit) {
                if (self_loop == nullptr || *vit == self_loop) {
                  removed_loop = std::move(*vit);
                  vec.erase(vit);
                  break;
                }
              }
              if (vec.empty()) {
                client->active_retry_loops_.erase(it);
              }
            }
          }
          if (v_resp.ok() && v_resp->has_stats() &&
              v_resp->stats().has_backend_latency()) {
            auto const& b = v_resp->stats().backend_latency();
            client->backend_latency_ = std::chrono::seconds(b.seconds()) +
                                       std::chrono::nanoseconds(b.nanos());
          } else {
            client->backend_latency_ = std::nullopt;
          }
        }
        removed_loop.reset();
        cb(std::move(v_resp));
      };

  middleware::RetryingVRpc::Options retry_options;
  retry_options.max_attempts = kMaxAttempts;
  retry_options.is_idempotent = is_idempotent;
  retry_options.policy = std::move(policy);
  auto loop = middleware::RetryingVRpc::Create(
      std::move(cancellable_op), std::move(wrapped_cb),
      std::move(retry_options), scheduler_);
  *loop_holder = loop;
  bool pre_cancelled = false;
  if (context != nullptr) {
    std::lock_guard<std::mutex> l(mutex_);
    if (pre_cancelled_contexts_.erase(context) > 0) {
      pre_cancelled = true;
    } else {
      active_retry_loops_[context].push_back(loop);
    }
  }
  if (g_sync_loop_sink != nullptr) {
    *g_sync_loop_sink = loop;
  }
  if (pre_cancelled) {
    loop->Cancel();
    return loop;
  }
  loop->StartAttempt();
  return loop;
}

namespace {

template <typename Response>
struct SyncState {
  Latch done;
  google::cloud::StatusOr<Response> result;
  WallTimePoint completed_at;
};

template <typename Response, typename Has, typename Mutable>
google::cloud::StatusOr<Response> DecodeTableResponse(
    google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse> v_resp,
    Has has, Mutable mut, char const* op_name) {
  if (!v_resp.ok()) return std::move(v_resp).status();
  google::bigtable::v2::TableResponse table_resp;
  if (!table_resp.ParseFromString(v_resp->payload())) {
    return google::cloud::internal::InternalError(
        "Failed to decode inner TableResponse payload bytes");
  }
  if (!(table_resp.*has)()) {
    return google::cloud::internal::InternalError(
        absl::StrCat("Server returned mismatched payload type for ", op_name));
  }
  return std::move(*(table_resp.*mut)());
}

}  // namespace

void Client::ReadRowAsync(
    grpc::ClientContext* context,
    google::bigtable::v2::SessionReadRowRequest request,
    std::function<void(
        google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>)>
        callback,
    middleware::RetryingVRpc::CallPolicy policy) {
  StartReadRow(context, std::move(request), std::move(callback),
               std::move(policy));
}

std::shared_ptr<middleware::RetryingVRpc> Client::StartReadRow(
    grpc::ClientContext* context,
    google::bigtable::v2::SessionReadRowRequest request,
    std::function<void(
        google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>)>
        callback,
    middleware::RetryingVRpc::CallPolicy policy) {
  using Response = google::bigtable::v2::SessionReadRowResponse;
  auto generic_cb =
      [cb = std::move(callback)](
          google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              v_resp) mutable {
        cb(DecodeTableResponse<Response>(
            std::move(v_resp),
            &google::bigtable::v2::TableResponse::has_read_row,
            &google::bigtable::v2::TableResponse::mutable_read_row, "ReadRow"));
      };
  google::bigtable::v2::TableRequest table_payload;
  *table_payload.mutable_read_row() = std::move(request);
  std::string serialized_payload;
  (void)table_payload.SerializeToString(&serialized_payload);
  return StartOperation(context, std::move(serialized_payload),
                        /*is_write_request=*/false, /*is_idempotent=*/true,
                        std::move(policy), std::move(generic_cb));
}

void Client::MutateRowAsync(
    grpc::ClientContext* context,
    google::bigtable::v2::SessionMutateRowRequest request,
    std::function<void(google::cloud::StatusOr<
                       google::bigtable::v2::SessionMutateRowResponse>)>
        callback,
    bool is_idempotent, middleware::RetryingVRpc::CallPolicy policy) {
  StartMutateRow(context, std::move(request), std::move(callback),
                 is_idempotent, std::move(policy));
}

std::shared_ptr<middleware::RetryingVRpc> Client::StartMutateRow(
    grpc::ClientContext* context,
    google::bigtable::v2::SessionMutateRowRequest request,
    std::function<void(google::cloud::StatusOr<
                       google::bigtable::v2::SessionMutateRowResponse>)>
        callback,
    bool is_idempotent, middleware::RetryingVRpc::CallPolicy policy) {
  using Response = google::bigtable::v2::SessionMutateRowResponse;
  auto generic_cb =
      [cb = std::move(callback)](
          google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              v_resp) mutable {
        cb(DecodeTableResponse<Response>(
            std::move(v_resp),
            &google::bigtable::v2::TableResponse::has_mutate_row,
            &google::bigtable::v2::TableResponse::mutable_mutate_row,
            "MutateRow"));
      };
  google::bigtable::v2::TableRequest table_payload;
  *table_payload.mutable_mutate_row() = std::move(request);
  std::string serialized_payload;
  (void)table_payload.SerializeToString(&serialized_payload);
  return StartOperation(context, std::move(serialized_payload),
                        /*is_write_request=*/true, is_idempotent,
                        std::move(policy), std::move(generic_cb));
}

template <typename Response, typename Starter>
google::cloud::StatusOr<Response> Client::RunSync(
    grpc::ClientContext* context, middleware::RetryingVRpc::CallPolicy policy,
    std::string_view op_name, SyncWaiter& waiter, Starter&& start) {
  grpc::ClientContext fallback_context;
  grpc::ClientContext* effective_context =
      context != nullptr ? context : &fallback_context;

  if (EffectiveDeadline(effective_context, policy) == (WallTimePoint::max)()) {
    policy.deadline = WallClock::now() + kDefaultOperationTimeout;
  }
  WallTimePoint const deadline = EffectiveDeadline(effective_context, policy);

  bool const timed = waiter.enabled();
  auto const issued_at = timed ? WallClock::now() : WallTimePoint{};
  auto state = std::make_shared<SyncState<Response>>();
  std::shared_ptr<middleware::RetryingVRpc> sync_loop;
  {
    auto* prev_sink = g_sync_loop_sink;
    g_sync_loop_sink = &sync_loop;
    start(
        effective_context,
        [state, timed](google::cloud::StatusOr<Response> resp) {
          state->result = std::move(resp);
          if (timed) state->completed_at = WallClock::now();
          state->done.Notify();
        },
        std::move(policy));
    g_sync_loop_sink = prev_sink;
  }
  auto const max_tp = (WallTimePoint::max)();
  auto const wait_deadline =
      (deadline <= max_tp - kSyncGrace) ? (deadline + kSyncGrace) : max_tp;
  if (waiter.Wait(state->done, issued_at, wait_deadline)) {
    if (timed && state->result.ok()) {
      waiter.RecordLatency(std::chrono::duration_cast<std::chrono::nanoseconds>(
          state->completed_at - issued_at));
    }
    return std::move(state->result);
  }
  if (pool_ != nullptr) {
    pool_->CancelByContext(effective_context,
                           google::cloud::internal::DeadlineExceededError(
                               absl::StrCat(op_name, " deadline exceeded")));
  }
  if (sync_loop) {
    sync_loop->Cancel();
  }
  std::vector<std::shared_ptr<middleware::RetryingVRpc>> loops;
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = active_retry_loops_.find(effective_context);
    if (it != active_retry_loops_.end()) loops = it->second;
  }
  for (auto& loop : loops) {
    if (loop) loop->Cancel();
  }
  return google::cloud::internal::DeadlineExceededError(
      absl::StrCat(op_name, " timed out waiting for session response"));
}

google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
Client::ReadRow(grpc::ClientContext* context,
                google::bigtable::v2::SessionReadRowRequest request,
                middleware::RetryingVRpc::CallPolicy policy) {
  using Response = google::bigtable::v2::SessionReadRowResponse;
  return RunSync<Response>(
      context, std::move(policy), "ReadRow", read_waiter_,
      [&](grpc::ClientContext* effective_ctx,
          std::function<void(google::cloud::StatusOr<Response>)> cb,
          middleware::RetryingVRpc::CallPolicy p) {
        ReadRowAsync(effective_ctx, std::move(request), std::move(cb),
                     std::move(p));
      });
}

void Client::ReadRowAsync(
    google::bigtable::v2::SessionReadRowRequest request,
    std::function<void(
        google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>)>
        callback) {
  ReadRowAsync(nullptr, std::move(request), std::move(callback));
}

google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
Client::ReadRow(google::bigtable::v2::SessionReadRowRequest request) {
  return ReadRow(nullptr, std::move(request));
}

google::cloud::StatusOr<google::bigtable::v2::SessionMutateRowResponse>
Client::MutateRow(grpc::ClientContext* context,
                  google::bigtable::v2::SessionMutateRowRequest request,
                  bool is_idempotent,
                  middleware::RetryingVRpc::CallPolicy policy) {
  using Response = google::bigtable::v2::SessionMutateRowResponse;
  return RunSync<Response>(
      context, std::move(policy), "MutateRow", write_waiter_,
      [&](grpc::ClientContext* effective_ctx,
          std::function<void(google::cloud::StatusOr<Response>)> cb,
          middleware::RetryingVRpc::CallPolicy p) {
        MutateRowAsync(effective_ctx, std::move(request), std::move(cb),
                       is_idempotent, std::move(p));
      });
}

void Client::MutateRowAsync(
    google::bigtable::v2::SessionMutateRowRequest request,
    std::function<void(google::cloud::StatusOr<
                       google::bigtable::v2::SessionMutateRowResponse>)>
        callback,
    bool is_idempotent) {
  MutateRowAsync(nullptr, std::move(request), std::move(callback),
                 is_idempotent);
}

google::cloud::StatusOr<google::bigtable::v2::SessionMutateRowResponse>
Client::MutateRow(google::bigtable::v2::SessionMutateRowRequest request,
                  bool is_idempotent) {
  return MutateRow(nullptr, std::move(request), is_idempotent);
}

void Client::CheckAndMutateRowAsync(
    grpc::ClientContext* context,
    google::bigtable::v2::SessionCheckAndMutateRowRequest request,
    std::function<void(google::cloud::StatusOr<
                       google::bigtable::v2::SessionCheckAndMutateRowResponse>)>
        callback,
    middleware::RetryingVRpc::CallPolicy policy) {
  using Response = google::bigtable::v2::SessionCheckAndMutateRowResponse;
  auto generic_cb =
      [cb = std::move(callback)](
          google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              v_resp) mutable {
        cb(DecodeTableResponse<Response>(
            std::move(v_resp),
            &google::bigtable::v2::TableResponse::has_check_and_mutate_row,
            &google::bigtable::v2::TableResponse::mutable_check_and_mutate_row,
            "CheckAndMutateRow"));
      };
  google::bigtable::v2::TableRequest table_payload;
  *table_payload.mutable_check_and_mutate_row() = std::move(request);
  std::string serialized_payload;
  (void)table_payload.SerializeToString(&serialized_payload);
  StartOperation(context, std::move(serialized_payload),
                 /*is_write_request=*/true, /*is_idempotent=*/false,
                 std::move(policy), std::move(generic_cb));
}

google::cloud::StatusOr<google::bigtable::v2::SessionCheckAndMutateRowResponse>
Client::CheckAndMutateRow(
    grpc::ClientContext* context,
    google::bigtable::v2::SessionCheckAndMutateRowRequest request,
    middleware::RetryingVRpc::CallPolicy policy) {
  using Response = google::bigtable::v2::SessionCheckAndMutateRowResponse;
  return RunSync<Response>(
      context, std::move(policy), "CheckAndMutateRow", write_waiter_,
      [&](grpc::ClientContext* effective_ctx,
          std::function<void(google::cloud::StatusOr<Response>)> cb,
          middleware::RetryingVRpc::CallPolicy p) {
        CheckAndMutateRowAsync(effective_ctx, std::move(request), std::move(cb),
                               std::move(p));
      });
}

google::cloud::StatusOr<google::bigtable::v2::SessionCheckAndMutateRowResponse>
Client::CheckAndMutateRow(
    google::bigtable::v2::SessionCheckAndMutateRowRequest request) {
  return CheckAndMutateRow(nullptr, std::move(request));
}

void Client::ReadModifyWriteRowAsync(
    grpc::ClientContext* context,
    google::bigtable::v2::SessionReadModifyWriteRowRequest request,
    std::function<
        void(google::cloud::StatusOr<
             google::bigtable::v2::SessionReadModifyWriteRowResponse>)>
        callback,
    middleware::RetryingVRpc::CallPolicy policy) {
  using Response = google::bigtable::v2::SessionReadModifyWriteRowResponse;
  auto generic_cb =
      [cb = std::move(callback)](
          google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              v_resp) mutable {
        cb(DecodeTableResponse<Response>(
            std::move(v_resp),
            &google::bigtable::v2::TableResponse::has_read_modify_write_row,
            &google::bigtable::v2::TableResponse::mutable_read_modify_write_row,
            "ReadModifyWriteRow"));
      };
  google::bigtable::v2::TableRequest table_payload;
  *table_payload.mutable_read_modify_write_row() = std::move(request);
  std::string serialized_payload;
  (void)table_payload.SerializeToString(&serialized_payload);
  StartOperation(context, std::move(serialized_payload),
                 /*is_write_request=*/true, /*is_idempotent=*/false,
                 std::move(policy), std::move(generic_cb));
}

google::cloud::StatusOr<google::bigtable::v2::SessionReadModifyWriteRowResponse>
Client::ReadModifyWriteRow(
    grpc::ClientContext* context,
    google::bigtable::v2::SessionReadModifyWriteRowRequest request,
    middleware::RetryingVRpc::CallPolicy policy) {
  using Response = google::bigtable::v2::SessionReadModifyWriteRowResponse;
  return RunSync<Response>(
      context, std::move(policy), "ReadModifyWriteRow", write_waiter_,
      [&](grpc::ClientContext* effective_ctx,
          std::function<void(google::cloud::StatusOr<Response>)> cb,
          middleware::RetryingVRpc::CallPolicy p) {
        ReadModifyWriteRowAsync(effective_ctx, std::move(request),
                                std::move(cb), std::move(p));
      });
}

google::cloud::StatusOr<google::bigtable::v2::SessionReadModifyWriteRowResponse>
Client::ReadModifyWriteRow(
    google::bigtable::v2::SessionReadModifyWriteRowRequest request) {
  return ReadModifyWriteRow(nullptr, std::move(request));
}

void Client::MutateRowsAsync(
    grpc::ClientContext* context,
    google::bigtable::v2::SessionMutateRowsRequest request,
    std::function<void(google::cloud::StatusOr<
                       google::bigtable::v2::SessionMutateRowsResponse>)>
        callback,
    middleware::RetryingVRpc::CallPolicy policy) {
  using Response = google::bigtable::v2::SessionMutateRowsResponse;
  auto generic_cb =
      [cb = std::move(callback)](
          google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              v_resp) mutable {
        cb(DecodeTableResponse<Response>(
            std::move(v_resp),
            &google::bigtable::v2::TableResponse::has_mutate_rows,
            &google::bigtable::v2::TableResponse::mutable_mutate_rows,
            "MutateRows"));
      };
  google::bigtable::v2::TableRequest table_payload;
  *table_payload.mutable_mutate_rows() = std::move(request);
  std::string serialized_payload;
  (void)table_payload.SerializeToString(&serialized_payload);
  StartOperation(context, std::move(serialized_payload),
                 /*is_write_request=*/true, /*is_idempotent=*/true,
                 std::move(policy), std::move(generic_cb));
}

google::cloud::StatusOr<google::bigtable::v2::SessionMutateRowsResponse>
Client::MutateRows(grpc::ClientContext* context,
                   google::bigtable::v2::SessionMutateRowsRequest request,
                   middleware::RetryingVRpc::CallPolicy policy) {
  using Response = google::bigtable::v2::SessionMutateRowsResponse;
  return RunSync<Response>(
      context, std::move(policy), "MutateRows", write_waiter_,
      [&](grpc::ClientContext* effective_ctx,
          std::function<void(google::cloud::StatusOr<Response>)> cb,
          middleware::RetryingVRpc::CallPolicy p) {
        MutateRowsAsync(effective_ctx, std::move(request), std::move(cb),
                        std::move(p));
      });
}

google::cloud::StatusOr<google::bigtable::v2::SessionMutateRowsResponse>
Client::MutateRows(google::bigtable::v2::SessionMutateRowsRequest request) {
  return MutateRows(nullptr, std::move(request));
}

void Client::Close() {
  std::vector<std::shared_ptr<middleware::RetryingVRpc>> loops;
  {
    std::lock_guard<std::mutex> l(mutex_);
    for (auto& [ctx, vec] : active_retry_loops_) {
      (void)ctx;
      for (auto& loop : vec) {
        if (loop) loops.push_back(std::move(loop));
      }
    }
    active_retry_loops_.clear();
    pre_cancelled_contexts_.clear();
  }
  for (auto& loop : loops) {
    loop->Cancel();
  }
  if (pool_ != nullptr) {
    pool_->Close();
  }
}

void Client::CancelContext(grpc::ClientContext* context) {
  if (context == nullptr) return;
  std::vector<std::shared_ptr<middleware::RetryingVRpc>> loops;
  {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = active_retry_loops_.find(context);
    if (it != active_retry_loops_.end() && !it->second.empty()) {
      loops = it->second;
    } else {
      if (pre_cancelled_contexts_.size() > 4096) {
        pre_cancelled_contexts_.clear();
      }
      pre_cancelled_contexts_.insert(context);
    }
  }
  if (pool_ != nullptr) {
    pool_->CancelByContext(context);
  }
  for (auto& loop : loops) {
    if (loop != nullptr) {
      loop->Cancel();
    }
  }
}

google::cloud::Status Client::CloseGracefully(
    std::chrono::nanoseconds timeout) {
  if (pool_ == nullptr) {
    std::vector<std::shared_ptr<middleware::RetryingVRpc>> loops;
    {
      std::lock_guard<std::mutex> l(mutex_);
      for (auto& [ctx, vec] : active_retry_loops_) {
        (void)ctx;
        for (auto& loop : vec) {
          if (loop) loops.push_back(std::move(loop));
        }
      }
      active_retry_loops_.clear();
    }
    for (auto& loop : loops) {
      loop->Cancel();
    }
    return google::cloud::internal::FailedPreconditionError(
        "Session pool is null");
  }
  auto status = pool_->CloseGracefully(timeout);
  std::vector<std::shared_ptr<middleware::RetryingVRpc>> loops;
  {
    std::lock_guard<std::mutex> l(mutex_);
    for (auto& [ctx, vec] : active_retry_loops_) {
      (void)ctx;
      for (auto& loop : vec) {
        if (loop) loops.push_back(std::move(loop));
      }
    }
    active_retry_loops_.clear();
  }
  for (auto& loop : loops) {
    loop->Cancel();
  }
  return status;
}

google::cloud::Status Client::WaitForPreWarming(
    std::chrono::nanoseconds timeout) {
  return pool_ != nullptr ? pool_->WaitForPreWarming(timeout)
                          : google::cloud::internal::FailedPreconditionError(
                                "Session pool is null");
}

std::size_t Client::active_sessions_count() const {
  return pool_ != nullptr ? pool_->active_sessions_count() : 0;
}

std::size_t Client::pending_queue_size() const {
  return pool_ != nullptr ? pool_->pending_queue_size() : 0;
}

std::size_t Client::active_calls_count() const {
  return pool_ != nullptr ? pool_->active_calls_count() : 0;
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

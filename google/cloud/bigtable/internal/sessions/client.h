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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_CLIENT_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_CLIENT_H

#include "google/cloud/bigtable/internal/sessions/client/sync_waiter.h"
#include "google/cloud/bigtable/internal/sessions/middleware/cancellable_vrpc.h"
#include "google/cloud/bigtable/internal/sessions/middleware/retrying_vrpc.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_pool_impl.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/options.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/version.h"
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/bigtable/v2/session.pb.h>
#include <grpcpp/grpcpp.h>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

/// Proto-native session client that dispatches `ReadRow` and `MutateRow`
/// virtual RPCs through the retry and cancellation middleware over a
/// `SessionPoolImpl`.
class Client : public std::enable_shared_from_this<Client> {
 public:
  using ScheduleFn = middleware::RetryingVRpc::ScheduleFn;

  /// Creates a client backed by a new session pool for `(table_name,
  /// app_profile_id, permission)`. Synchronous calls use
  /// `SyncWaiter::ConfigFromOptions(options)`.
  static std::shared_ptr<Client> Create(
      std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
      std::string_view table_name, std::string_view app_profile_id,
      google::bigtable::v2::OpenTableRequest::Permission permission,
      google::cloud::Options options, ScheduleFn scheduler = nullptr);

  /// Creates a client backed by a new session pool for an AuthorizedView.
  static std::shared_ptr<Client> Create(
      std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
      std::string_view authorized_view_name, std::string_view app_profile_id,
      google::bigtable::v2::OpenAuthorizedViewRequest::Permission permission,
      google::cloud::Options options, ScheduleFn scheduler = nullptr);

  /// Creates a client backed by a new session pool for a MaterializedView.
  static std::shared_ptr<Client> Create(
      std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
      std::string_view materialized_view_name, std::string_view app_profile_id,
      google::bigtable::v2::OpenMaterializedViewRequest::Permission permission,
      google::cloud::Options options, ScheduleFn scheduler = nullptr);

  /// Creates a client sharing an existing `pool` (for multi-shard client
  /// directories). A null `spin_gate` defaults to `SyncSpinGate::Global()`.
  static std::shared_ptr<Client> CreateWithPool(
      std::shared_ptr<SessionPoolImpl> pool, ScheduleFn scheduler = nullptr,
      SyncWaiter::Config sync_wait = {}, SyncSpinGate* spin_gate = nullptr);

  /// Creates a client over an existing `pool` for unit testing.
  static std::shared_ptr<Client> CreateForTesting(
      std::shared_ptr<SessionPoolImpl> pool, ScheduleFn scheduler = nullptr,
      SyncWaiter::Config sync_wait = {}, SyncSpinGate* spin_gate = nullptr);

  virtual ~Client() = default;

  /// Executes an asynchronous point `ReadRow` with retry and cancellation
  /// middleware.
  virtual void ReadRowAsync(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionReadRowRequest request,
      std::function<void(google::cloud::StatusOr<
                         google::bigtable::v2::SessionReadRowResponse>)>
          callback,
      middleware::RetryingVRpc::CallPolicy policy);

  void ReadRowAsync(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionReadRowRequest request,
      std::function<void(google::cloud::StatusOr<
                         google::bigtable::v2::SessionReadRowResponse>)>
          callback) {
    ReadRowAsync(context, std::move(request), std::move(callback), {});
  }

  /// Synchronous point `ReadRow` helper.
  google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse> ReadRow(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionReadRowRequest request,
      middleware::RetryingVRpc::CallPolicy policy = {});

  /// Overloads that allocate an internal `grpc::ClientContext`.
  void ReadRowAsync(
      google::bigtable::v2::SessionReadRowRequest request,
      std::function<void(google::cloud::StatusOr<
                         google::bigtable::v2::SessionReadRowResponse>)>
          callback);
  google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse> ReadRow(
      google::bigtable::v2::SessionReadRowRequest request);

  /// Executes an asynchronous point `MutateRow` with retry and cancellation
  /// middleware.
  virtual void MutateRowAsync(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionMutateRowRequest request,
      std::function<void(google::cloud::StatusOr<
                         google::bigtable::v2::SessionMutateRowResponse>)>
          callback,
      bool is_idempotent, middleware::RetryingVRpc::CallPolicy policy);

  void MutateRowAsync(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionMutateRowRequest request,
      std::function<void(google::cloud::StatusOr<
                         google::bigtable::v2::SessionMutateRowResponse>)>
          callback,
      bool is_idempotent = false) {
    MutateRowAsync(context, std::move(request), std::move(callback),
                   is_idempotent, {});
  }

  /// Synchronous point `MutateRow` helper.
  google::cloud::StatusOr<google::bigtable::v2::SessionMutateRowResponse>
  MutateRow(grpc::ClientContext* context,
            google::bigtable::v2::SessionMutateRowRequest request,
            bool is_idempotent = false,
            middleware::RetryingVRpc::CallPolicy policy = {});

  /// Overloads that allocate an internal `grpc::ClientContext`.
  void MutateRowAsync(
      google::bigtable::v2::SessionMutateRowRequest request,
      std::function<void(google::cloud::StatusOr<
                         google::bigtable::v2::SessionMutateRowResponse>)>
          callback,
      bool is_idempotent = false);
  google::cloud::StatusOr<google::bigtable::v2::SessionMutateRowResponse>
  MutateRow(google::bigtable::v2::SessionMutateRowRequest request,
            bool is_idempotent = false);

  virtual void CheckAndMutateRowAsync(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionCheckAndMutateRowRequest request,
      std::function<
          void(google::cloud::StatusOr<
               google::bigtable::v2::SessionCheckAndMutateRowResponse>)>
          callback,
      middleware::RetryingVRpc::CallPolicy policy);

  void CheckAndMutateRowAsync(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionCheckAndMutateRowRequest request,
      std::function<
          void(google::cloud::StatusOr<
               google::bigtable::v2::SessionCheckAndMutateRowResponse>)>
          callback) {
    CheckAndMutateRowAsync(context, std::move(request), std::move(callback),
                           {});
  }

  google::cloud::StatusOr<
      google::bigtable::v2::SessionCheckAndMutateRowResponse>
  CheckAndMutateRow(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionCheckAndMutateRowRequest request,
      middleware::RetryingVRpc::CallPolicy policy = {});
  google::cloud::StatusOr<
      google::bigtable::v2::SessionCheckAndMutateRowResponse>
  CheckAndMutateRow(
      google::bigtable::v2::SessionCheckAndMutateRowRequest request);

  virtual void ReadModifyWriteRowAsync(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionReadModifyWriteRowRequest request,
      std::function<
          void(google::cloud::StatusOr<
               google::bigtable::v2::SessionReadModifyWriteRowResponse>)>
          callback,
      middleware::RetryingVRpc::CallPolicy policy);

  void ReadModifyWriteRowAsync(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionReadModifyWriteRowRequest request,
      std::function<
          void(google::cloud::StatusOr<
               google::bigtable::v2::SessionReadModifyWriteRowResponse>)>
          callback) {
    ReadModifyWriteRowAsync(context, std::move(request), std::move(callback),
                            {});
  }

  google::cloud::StatusOr<
      google::bigtable::v2::SessionReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionReadModifyWriteRowRequest request,
      middleware::RetryingVRpc::CallPolicy policy = {});
  google::cloud::StatusOr<
      google::bigtable::v2::SessionReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::SessionReadModifyWriteRowRequest request);

  virtual void MutateRowsAsync(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionMutateRowsRequest request,
      std::function<void(google::cloud::StatusOr<
                         google::bigtable::v2::SessionMutateRowsResponse>)>
          callback,
      middleware::RetryingVRpc::CallPolicy policy);

  void MutateRowsAsync(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionMutateRowsRequest request,
      std::function<void(google::cloud::StatusOr<
                         google::bigtable::v2::SessionMutateRowsResponse>)>
          callback) {
    MutateRowsAsync(context, std::move(request), std::move(callback), {});
  }

  google::cloud::StatusOr<google::bigtable::v2::SessionMutateRowsResponse>
  MutateRows(grpc::ClientContext* context,
             google::bigtable::v2::SessionMutateRowsRequest request,
             middleware::RetryingVRpc::CallPolicy policy = {});
  google::cloud::StatusOr<google::bigtable::v2::SessionMutateRowsResponse>
  MutateRows(google::bigtable::v2::SessionMutateRowsRequest request);

  void SetFirstRpcId(std::int64_t first_rpc_id) {
    if (pool_ != nullptr) {
      pool_->SetFirstRpcId(first_rpc_id);
    }
    next_rpc_id_.store(first_rpc_id, std::memory_order_relaxed);
  }
  std::optional<std::chrono::nanoseconds> backend_latency() const {
    std::lock_guard<std::mutex> l(mutex_);
    return backend_latency_;
  }
  std::multimap<grpc::string_ref, grpc::string_ref> initial_metadata() const {
    return pool_ ? pool_->initial_metadata()
                 : std::multimap<grpc::string_ref, grpc::string_ref>{};
  }
  std::optional<google::bigtable::v2::BackendIdentifier> backend() const {
    return pool_ ? pool_->backend() : std::nullopt;
  }
  std::optional<google::bigtable::v2::SessionRefreshConfig>
  session_refresh_config() const {
    return pool_ ? pool_->session_refresh_config() : std::nullopt;
  }
  google::cloud::Status last_connection_error() const {
    return pool_ ? pool_->last_connection_error() : google::cloud::Status{};
  }

  /// Shuts down the managed pool and cancels in-flight operations.
  virtual void Close();

  /// Cancels any active retry loop associated with `context`.
  virtual void CancelContext(grpc::ClientContext* context);

  /// Gracefully shuts down the managed pool, draining active operations up to
  /// `timeout`.
  virtual google::cloud::Status CloseGracefully(
      std::chrono::nanoseconds timeout);

  virtual google::cloud::Status WaitForPreWarming(
      std::chrono::nanoseconds timeout);

  virtual std::size_t active_sessions_count() const;
  virtual std::size_t pending_queue_size() const;
  virtual std::size_t active_calls_count() const;
  virtual std::chrono::nanoseconds GetReadRttEwma() const {
    return pool_ ? pool_->GetReadRttEwma() : std::chrono::nanoseconds::zero();
  }
  virtual std::chrono::nanoseconds GetWriteRttEwma() const {
    return pool_ ? pool_->GetWriteRttEwma() : std::chrono::nanoseconds::zero();
  }
  std::shared_ptr<SessionPoolImpl> pool() const { return pool_; }

  std::optional<google::bigtable::v2::ClusterInformation> cluster_info() const {
    std::lock_guard<std::mutex> l(mutex_);
    return cluster_info_;
  }

  /// Forwards updated server session pool configuration to the underlying pool.
  virtual void UpdatePoolConfig(
      google::bigtable::v2::SessionClientConfiguration::
          SessionPoolConfiguration const& config) {
    if (pool_ != nullptr) {
      pool_->UpdateConfig(config);
    }
  }

  SyncWaiter const& read_sync_waiter() const { return read_waiter_; }
  SyncWaiter const& write_sync_waiter() const { return write_waiter_; }

  SyncWaiter::Config sync_wait_config() const {
    SyncWaiter::Config config;
    config.spin_budget = read_waiter_.spin_budget();
    config.max_spinners = read_waiter_.max_spinners();
    return config;
  }

 protected:
  Client() = default;
  Client(SyncWaiter::Config const& sync_wait, SyncSpinGate* spin_gate);

 public:
  std::int64_t AllocateRpcId();

  /// Maximum attempts per operation (matches the retry oracle cap).
  static constexpr int kMaxAttempts = 3;

 private:
  std::shared_ptr<middleware::RetryingVRpc> StartOperation(
      grpc::ClientContext* context, std::string serialized_payload,
      bool is_write_request, bool is_idempotent,
      middleware::RetryingVRpc::CallPolicy policy,
      middleware::RetryingVRpc::GenericCallback generic_cb);
  std::shared_ptr<middleware::RetryingVRpc> StartReadRow(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionReadRowRequest request,
      std::function<void(google::cloud::StatusOr<
                         google::bigtable::v2::SessionReadRowResponse>)>
          callback,
      middleware::RetryingVRpc::CallPolicy policy);
  std::shared_ptr<middleware::RetryingVRpc> StartMutateRow(
      grpc::ClientContext* context,
      google::bigtable::v2::SessionMutateRowRequest request,
      std::function<void(google::cloud::StatusOr<
                         google::bigtable::v2::SessionMutateRowResponse>)>
          callback,
      bool is_idempotent, middleware::RetryingVRpc::CallPolicy policy);
  template <typename Response, typename Starter>
  google::cloud::StatusOr<Response> RunSync(
      grpc::ClientContext* context, middleware::RetryingVRpc::CallPolicy policy,
      std::string_view op_name, SyncWaiter& waiter, Starter&& start);

  std::shared_ptr<SessionPoolImpl> pool_;
  ScheduleFn scheduler_;

  mutable std::mutex mutex_;
  std::optional<google::bigtable::v2::ClusterInformation> cluster_info_;
  std::unordered_map<grpc::ClientContext*,
                     std::vector<std::shared_ptr<middleware::RetryingVRpc>>>
      active_retry_loops_;
  std::unordered_set<grpc::ClientContext*> pre_cancelled_contexts_;
  std::optional<std::chrono::nanoseconds> backend_latency_;
  std::atomic<std::int64_t> next_rpc_id_{1};

  SyncWaiter read_waiter_;
  SyncWaiter write_waiter_;
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_CLIENT_H

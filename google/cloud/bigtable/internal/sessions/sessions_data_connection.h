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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SESSIONS_DATA_CONNECTION_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SESSIONS_DATA_CONNECTION_H

#include "google/cloud/bigtable/data_connection.h"
#include "google/cloud/bigtable/instance_resource.h"
#include "google/cloud/bigtable/internal/operation_context_factory.h"
#include "google/cloud/bigtable/internal/sessions/client.h"
#include "google/cloud/bigtable/internal/sessions/client/client_configuration_manager.h"
#include "google/cloud/bigtable/internal/sessions/pool/pool_executors.h"
#include "google/cloud/bigtable/internal/sessions/shim/client_directory.h"
#include "google/cloud/bigtable/internal/sessions/shim/diverter.h"
#include "google/cloud/internal/unified_grpc_credentials.h"
#include "google/cloud/options.h"
#include "google/cloud/status_or.h"
#include "google/cloud/version.h"
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/bigtable/v2/session.pb.h>
#include <array>
#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

/// Layer 6 (Shim): Mixed-mode `bigtable::DataConnection` decorator that routes
/// eligible point operations (`ReadRow`, `AsyncReadRow`, `Apply`, `AsyncApply`)
/// over multiplexed Bigtable Sessions while delegating streaming and multi-row
/// operations (and fallback traffic) to `classic_connection_`.
class SessionsDataConnection
    : public google::cloud::bigtable::DataConnection,
      public std::enable_shared_from_this<SessionsDataConnection> {
 public:
  static constexpr std::size_t kNumClientShards = 16;
  using ClientShards = std::array<std::shared_ptr<Client>, kNumClientShards>;
  using ClientFactoryFn = std::function<std::shared_ptr<Client>(
      std::string_view table_name, std::string_view app_profile_id,
      google::bigtable::v2::OpenTableRequest::Permission permission,
      Options options, Client::ScheduleFn scheduler)>;

  static std::shared_ptr<SessionsDataConnection> Create(
      std::shared_ptr<google::cloud::bigtable::DataConnection>
          classic_connection,
      std::shared_ptr<OperationContextFactory> operation_context_factory,
      std::vector<google::cloud::bigtable::InstanceResource> const& instances,
      Options options);

  static std::shared_ptr<SessionsDataConnection> CreateForTesting(
      std::shared_ptr<google::cloud::bigtable::DataConnection>
          classic_connection,
      std::shared_ptr<OperationContextFactory> operation_context_factory,
      std::vector<google::bigtable::v2::Bigtable::Stub*> stubs, Options options,
      ClientFactoryFn client_factory = nullptr,
      std::size_t max_directory_entries =
          shim::ClientDirectory<ClientShards>::kDefaultMaxEntries);

  ~SessionsDataConnection() override;

  /// Explicitly closes all session pools, stops configuration polling, and
  /// shuts down shared session executors. Idempotent.
  void Close();

  /// Pre-warms read and/or write session pools for `(table_name,
  /// app_profile_id)`.
  void PreWarm(std::string const& table_name,
               std::string const& app_profile_id = "default",
               bool prewarm_read = true, bool prewarm_write = false);

  void SetSessionLoadForTesting(double load) {
    read_row_diverter_.SetSessionLoad(load);
    mutate_row_diverter_.SetSessionLoad(load);
  }
  void SetDiversionLoadsForTesting(client::DiversionLoads const& loads) {
    read_row_diverter_.SetSessionLoad(loads.read_row);
    mutate_row_diverter_.SetSessionLoad(loads.mutate_row);
  }

  shim::Diverter const& read_row_diverter() const { return read_row_diverter_; }
  shim::Diverter const& mutate_row_diverter() const {
    return mutate_row_diverter_;
  }
  std::shared_ptr<PoolExecutors> executors() const { return executors_; }
  std::size_t directory_size() const {
    std::lock_guard<std::mutex> l(mu_);
    return client_directory_.size();
  }

  // --- bigtable::DataConnection overrides ---
  Options options() override { return options_; }

  Status Apply(std::string const& table_name,
               google::cloud::bigtable::SingleRowMutation mut) override;

  future<Status> AsyncApply(
      std::string const& table_name,
      google::cloud::bigtable::SingleRowMutation mut) override;

  std::vector<google::cloud::bigtable::FailedMutation> BulkApply(
      std::string const& table_name,
      google::cloud::bigtable::BulkMutation mut) override;

  future<std::vector<google::cloud::bigtable::FailedMutation>> AsyncBulkApply(
      std::string const& table_name,
      google::cloud::bigtable::BulkMutation mut) override;

  google::cloud::bigtable::RowReader ReadRowsFull(
      google::cloud::bigtable::ReadRowsParams params) override;

  StatusOr<std::pair<bool, google::cloud::bigtable::Row>> ReadRow(
      std::string const& table_name, std::string row_key,
      google::cloud::bigtable::Filter filter) override;

  StatusOr<google::cloud::bigtable::MutationBranch> CheckAndMutateRow(
      std::string const& table_name, std::string row_key,
      google::cloud::bigtable::Filter filter,
      std::vector<google::cloud::bigtable::Mutation> true_mutations,
      std::vector<google::cloud::bigtable::Mutation> false_mutations) override;

  future<StatusOr<google::cloud::bigtable::MutationBranch>>
  AsyncCheckAndMutateRow(
      std::string const& table_name, std::string row_key,
      google::cloud::bigtable::Filter filter,
      std::vector<google::cloud::bigtable::Mutation> true_mutations,
      std::vector<google::cloud::bigtable::Mutation> false_mutations) override;

  StatusOr<std::vector<google::cloud::bigtable::RowKeySample>> SampleRows(
      std::string const& table_name) override;

  future<StatusOr<std::vector<google::cloud::bigtable::RowKeySample>>>
  AsyncSampleRows(std::string const& table_name) override;

  StatusOr<google::cloud::bigtable::Row> ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest request) override;

  future<StatusOr<google::cloud::bigtable::Row>> AsyncReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest request) override;

  void AsyncReadRows(
      std::string const& table_name,
      std::function<future<bool>(google::cloud::bigtable::Row)> on_row,
      std::function<void(Status)> on_finish,
      google::cloud::bigtable::RowSet row_set, std::int64_t rows_limit,
      google::cloud::bigtable::Filter filter) override;

  future<StatusOr<std::pair<bool, google::cloud::bigtable::Row>>> AsyncReadRow(
      std::string const& table_name, std::string row_key,
      google::cloud::bigtable::Filter filter) override;

  StatusOr<google::cloud::bigtable::PreparedQuery> PrepareQuery(
      google::cloud::bigtable::PrepareQueryParams const& params) override;

  future<StatusOr<google::cloud::bigtable::PreparedQuery>> AsyncPrepareQuery(
      google::cloud::bigtable::PrepareQueryParams const& params) override;

  google::cloud::bigtable::RowStream ExecuteQuery(
      google::cloud::bigtable::ExecuteQueryParams params) override;

 private:
  SessionsDataConnection(
      std::shared_ptr<google::cloud::bigtable::DataConnection>
          classic_connection,
      std::shared_ptr<OperationContextFactory> operation_context_factory,
      Options options, std::size_t max_directory_entries);

  void InitializeChannels();
  void EnsureConfigManager(std::string_view instance_name);
  std::shared_ptr<Client> GetClient(
      std::string const& table_name, std::string const& app_profile_id,
      google::bigtable::v2::OpenTableRequest::Permission permission);
  Options BuildPoolOptions(
      std::string const& table_name, std::string const& app_profile_id,
      google::bigtable::v2::OpenTableRequest::Permission permission) const;
  void TripAllBreakers();

  std::shared_ptr<google::cloud::bigtable::DataConnection> classic_connection_;
  std::shared_ptr<OperationContextFactory> operation_context_factory_;
  Options options_;
  std::shared_ptr<PoolExecutors> executors_;
  Client::ScheduleFn scheduler_;
  std::shared_ptr<google::cloud::internal::GrpcAuthenticationStrategy> auth_;
  std::vector<std::unique_ptr<google::bigtable::v2::Bigtable::Stub>>
      owned_stubs_;
  std::vector<google::bigtable::v2::Bigtable::Stub*> stub_ptrs_;
  std::function<void(grpc::ClientContext&)> context_setup_;
  ClientFactoryFn client_factory_;

  shim::Diverter read_row_diverter_{1.0};
  shim::Diverter mutate_row_diverter_{1.0};

  mutable std::mutex mu_;
  std::atomic<bool> closed_{false};
  shim::ClientDirectory<ClientShards> client_directory_;
  std::unordered_map<std::string,
                     std::shared_ptr<client::ClientConfigurationManager>>
      config_managers_;
  std::unordered_map<std::string,
                     google::bigtable::v2::SessionClientConfiguration::
                         SessionPoolConfiguration>
      instance_pool_configs_;
};

std::shared_ptr<google::cloud::bigtable::DataConnection>
MakeSessionsDataConnection(
    std::shared_ptr<google::cloud::bigtable::DataConnection> classic_connection,
    std::shared_ptr<OperationContextFactory> operation_context_factory,
    std::vector<google::cloud::bigtable::InstanceResource> const& instances,
    Options options);

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SESSIONS_DATA_CONNECTION_H

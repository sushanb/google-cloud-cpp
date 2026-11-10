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

#include "google/cloud/bigtable/internal/sessions/sessions_data_connection.h"
#include "google/cloud/bigtable/internal/defaults.h"
#include "google/cloud/bigtable/internal/sessions/client/session_client.h"
#include "google/cloud/bigtable/internal/sessions/session/write_status.h"
#include "google/cloud/bigtable/internal/sessions/sessions_options.h"
#include "google/cloud/bigtable/internal/sessions/telemetry/session_tracer.h"
#include "google/cloud/bigtable/options.h"
#include "google/cloud/bigtable/retry_policy.h"
#include "google/cloud/bigtable/version.h"
#include "google/cloud/common_options.h"
#include "google/cloud/credentials.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/internal/api_client_header.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include <google/bigtable/v2/data.pb.h>
#include <google/bigtable/v2/feature_flags.pb.h>
#include <algorithm>
#include <chrono>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

std::string_view InstanceNameFromTableName(std::string_view table_name) {
  auto const pos = table_name.rfind("/tables/");
  if (pos == std::string_view::npos) return {};
  return table_name.substr(0, pos);
}

struct ParsedTableResource {
  std::string project_id;
  std::string instance_id;
  std::string table_id;
};

ParsedTableResource ParseTableName(std::string_view table_name) {
  std::vector<std::string_view> parts = absl::StrSplit(table_name, '/');
  ParsedTableResource r;
  if (parts.size() >= 6 && parts[0] == "projects" && parts[2] == "instances" &&
      parts[4] == "tables") {
    r.project_id = std::string(parts[1]);
    r.instance_id = std::string(parts[3]);
    r.table_id = std::string(parts[5]);
  }
  return r;
}

std::size_t CurrentThreadShardIndex() {
  thread_local std::size_t const kIdx =
      std::hash<std::thread::id>{}(std::this_thread::get_id()) %
      SessionsDataConnection::kNumClientShards;
  return kIdx;
}

bool HasPerCallCredentialsOverride(Options const& call_options,
                                   Options const& connection_options) {
  if (call_options.has<UnifiedCredentialsOption>()) {
    if (!connection_options.has<UnifiedCredentialsOption>() ||
        call_options.get<UnifiedCredentialsOption>().get() !=
            connection_options.get<UnifiedCredentialsOption>().get()) {
      return true;
    }
  }
  if (call_options.has<GrpcCredentialOption>()) {
    if (!connection_options.has<GrpcCredentialOption>() ||
        call_options.get<GrpcCredentialOption>().get() !=
            connection_options.get<GrpcCredentialOption>().get()) {
      return true;
    }
  }
  return false;
}

std::pair<bool, google::cloud::bigtable::Row> TranslateReadRowResponse(
    google::bigtable::v2::SessionReadRowResponse response) {
  if (!response.has_row()) {
    return std::make_pair(false, google::cloud::bigtable::Row("", {}));
  }
  auto& row = *response.mutable_row();
  if (row.key().empty() && row.families().empty()) {
    return std::make_pair(false, google::cloud::bigtable::Row("", {}));
  }
  std::vector<google::cloud::bigtable::Cell> cells;
  for (auto& family : *row.mutable_families()) {
    for (auto& column : *family.mutable_columns()) {
      for (auto& cell : *column.mutable_cells()) {
        std::vector<std::string> labels;
        std::move(cell.mutable_labels()->begin(), cell.mutable_labels()->end(),
                  std::back_inserter(labels));
        cells.emplace_back(row.key(), family.name(), column.qualifier(),
                           cell.timestamp_micros(),
                           std::move(*cell.mutable_value()), std::move(labels));
      }
    }
  }
  return std::make_pair(
      true, google::cloud::bigtable::Row(std::move(*row.mutable_key()),
                                         std::move(cells)));
}

middleware::RetryingVRpc::CallPolicy BuildCallPolicy(
    Options const& options,
    std::shared_ptr<OperationContext> const& operation_context) {
  middleware::RetryingVRpc::CallPolicy policy;

  std::shared_ptr<google::cloud::bigtable::DataRetryPolicy> retry;
  if (options.has<google::cloud::bigtable::DataRetryPolicyOption>() &&
      options.get<google::cloud::bigtable::DataRetryPolicyOption>() !=
          nullptr) {
    retry = std::shared_ptr<google::cloud::bigtable::DataRetryPolicy>(
        options.get<google::cloud::bigtable::DataRetryPolicyOption>()->clone());
    if (auto const* timed = dynamic_cast<
            google::cloud::bigtable::DataLimitedTimeRetryPolicy const*>(
            retry.get())) {
      policy.deadline =
          std::chrono::system_clock::now() + timed->maximum_duration();
    }
    policy.on_failure = [retry](Status const& s) {
      // Normalize uncommitted turnovers and server RetryInfo grants so
      // SafeGrpcRetry never rejects them solely on status code, while still
      // enforcing error-count and elapsed-time budgets.
      if (middleware::NeverLeftClient(s) ||
          (IsServerResult(s) && GetServerRetryDelay(s).has_value())) {
        Status const normalized =
            (s.code() == StatusCode::kUnavailable ||
             s.code() == StatusCode::kAborted)
                ? s
                : Status(StatusCode::kUnavailable, s.message());
        return retry->OnFailure(normalized);
      }
      return retry->OnFailure(s);
    };
  }

  if (options.has<google::cloud::bigtable::DataBackoffPolicyOption>() &&
      options.get<google::cloud::bigtable::DataBackoffPolicyOption>() !=
          nullptr) {
    auto backoff = std::shared_ptr<BackoffPolicy>(
        options.get<google::cloud::bigtable::DataBackoffPolicyOption>()
            ->clone());
    policy.next_backoff = [backoff]() {
      return std::chrono::duration_cast<std::chrono::nanoseconds>(
          backoff->OnCompletion());
    };
  }

  if (operation_context != nullptr) {
    policy.on_attempt_start = [operation_context]() {
      operation_context->SessionPreCall();
    };
    policy.on_attempt_complete =
        [operation_context](
            middleware::RetryingVRpc::AttemptTelemetry const& t) {
          SessionAttemptInfo info;
          info.attempt_status = t.status;
          info.cluster_id = t.cluster_id;
          info.zone_id = t.zone_id;
          info.transport_type = t.transport_type;
          info.transport_region = t.transport_region;
          info.transport_zone = t.transport_zone;
          info.transport_subzone = t.transport_subzone;
          info.has_peer_info = t.has_peer_info;
          info.throttling_latency = t.throttling_latency;
          operation_context->SessionPostCall(info);
          if (t.delivered_response) {
            operation_context->SessionElementDelivery();
          }
        };
  }

  return policy;
}

}  // namespace

std::shared_ptr<SessionsDataConnection> SessionsDataConnection::Create(
    std::shared_ptr<google::cloud::bigtable::DataConnection> classic_connection,
    std::shared_ptr<OperationContextFactory> operation_context_factory,
    std::vector<google::cloud::bigtable::InstanceResource> const& instances,
    Options options) {
  auto conn =
      std::shared_ptr<SessionsDataConnection>(new SessionsDataConnection(
          std::move(classic_connection), std::move(operation_context_factory),
          std::move(options),
          shim::ClientDirectory<ClientShards>::kDefaultMaxEntries));
  conn->InitializeChannels();
  for (auto const& instance : instances) {
    conn->EnsureConfigManager(instance.FullName());
  }
  return conn;
}

std::shared_ptr<SessionsDataConnection>
SessionsDataConnection::CreateForTesting(
    std::shared_ptr<google::cloud::bigtable::DataConnection> classic_connection,
    std::shared_ptr<OperationContextFactory> operation_context_factory,
    std::vector<google::bigtable::v2::Bigtable::Stub*> stubs, Options options,
    ClientFactoryFn client_factory, std::size_t max_directory_entries) {
  auto conn =
      std::shared_ptr<SessionsDataConnection>(new SessionsDataConnection(
          std::move(classic_connection), std::move(operation_context_factory),
          std::move(options), max_directory_entries));
  conn->stub_ptrs_ = std::move(stubs);
  conn->client_factory_ = std::move(client_factory);
  std::size_t num_loops = 1;
  if (conn->options_.has<SessionsNumEventLoopsOption>() &&
      conn->options_.get<SessionsNumEventLoopsOption>() > 0) {
    num_loops = static_cast<std::size_t>(
        conn->options_.get<SessionsNumEventLoopsOption>());
  }
  conn->executors_ = PoolExecutors::Create(num_loops, 2);
  std::weak_ptr<PoolExecutors> w_exec = conn->executors_;
  conn->scheduler_ = [w_exec](std::chrono::nanoseconds delay,
                              std::function<void()> task) {
    auto exec = w_exec.lock();
    if (!exec) return;
    exec->cq(0).MakeRelativeTimer(delay).then([t = std::move(task)](auto f) {
      if (f.get().ok()) {
        t();
      }
    });
  };
  if (conn->options_.has<SessionsMetadataHookOption>()) {
    conn->context_setup_ = conn->options_.get<SessionsMetadataHookOption>();
  }
  return conn;
}

SessionsDataConnection::SessionsDataConnection(
    std::shared_ptr<google::cloud::bigtable::DataConnection> classic_connection,
    std::shared_ptr<OperationContextFactory> operation_context_factory,
    Options options, std::size_t max_directory_entries)
    : classic_connection_(std::move(classic_connection)),
      operation_context_factory_(
          operation_context_factory != nullptr
              ? std::move(operation_context_factory)
              : std::make_shared<SimpleOperationContextFactory>()),
      options_(google::cloud::internal::MergeOptions(
          std::move(options), classic_connection_ != nullptr
                                  ? classic_connection_->options()
                                  : DataConnection::options())),
      client_directory_(max_directory_entries) {}

SessionsDataConnection::~SessionsDataConnection() { Close(); }

void SessionsDataConnection::InitializeChannels() {
  std::size_t num_loops = 0;
  if (options_.has<SessionsNumEventLoopsOption>() &&
      options_.get<SessionsNumEventLoopsOption>() > 0) {
    num_loops =
        static_cast<std::size_t>(options_.get<SessionsNumEventLoopsOption>());
  }
  executors_ = PoolExecutors::Create(num_loops, 0);
  std::weak_ptr<PoolExecutors> w_exec = executors_;
  scheduler_ = [w_exec](std::chrono::nanoseconds delay,
                        std::function<void()> task) {
    auto exec = w_exec.lock();
    if (!exec) return;
    exec->cq(0).MakeRelativeTimer(delay).then([t = std::move(task)](auto f) {
      if (f.get().ok()) {
        t();
      }
    });
  };

  auth_ = google::cloud::internal::CreateAuthenticationStrategy(
      executors_->cq(0), options_);
  int const num_channels = std::max(1, options_.get<GrpcNumChannelsOption>());
  owned_stubs_.reserve(static_cast<std::size_t>(num_channels));
  stub_ptrs_.reserve(static_cast<std::size_t>(num_channels));
  for (int i = 0; i < num_channels; ++i) {
    auto args = google::cloud::internal::MakeChannelArguments(options_);
    args.SetInt("grpc.channel_id", i);
    auto channel =
        auth_->CreateChannel(options_.get<EndpointOption>(), std::move(args));
    owned_stubs_.push_back(
        google::bigtable::v2::Bigtable::NewStub(std::move(channel)));
    stub_ptrs_.push_back(owned_stubs_.back().get());
  }

  std::string const api_client_header =
      google::cloud::internal::HandCraftedLibClientHeader();
  std::function<void(grpc::ClientContext&)> user_hook;
  if (options_.has<SessionsMetadataHookOption>()) {
    user_hook = options_.get<SessionsMetadataHookOption>();
  }
  auto auth = auth_;
  Options opts = options_;
  context_setup_ = [user_hook = std::move(user_hook), api_client_header, auth,
                    opts = std::move(opts)](grpc::ClientContext& ctx) {
    if (user_hook) user_hook(ctx);
    google::cloud::internal::ConfigureContext(ctx, opts);
    google::cloud::internal::SetMetadata(ctx, opts, {}, api_client_header);
    if (auth && auth->RequiresConfigureContext()) {
      (void)auth->ConfigureContext(ctx);
    }
  };
}

void SessionsDataConnection::Close() {
  if (closed_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  std::vector<std::shared_ptr<client::ClientConfigurationManager>> managers;
  std::vector<ClientShards> all_shards;
  {
    std::lock_guard<std::mutex> l(mu_);
    managers.reserve(config_managers_.size());
    for (auto& [instance, mgr] : config_managers_) {
      (void)instance;
      if (mgr) managers.push_back(std::move(mgr));
    }
    config_managers_.clear();
    client_directory_.ForEach(
        [&](auto const& entry) { all_shards.push_back(entry.value); });
    client_directory_.Clear();
  }

  for (auto& mgr : managers) {
    mgr->Stop();
  }
  for (auto& shards : all_shards) {
    for (auto& shard : shards) {
      if (shard != nullptr) {
        shard->Close();
      }
    }
  }
  if (executors_ != nullptr) {
    executors_->Shutdown();
  }
}

void SessionsDataConnection::TripAllBreakers() {
  read_row_diverter_.TripUnimplementedBreaker();
  mutate_row_diverter_.TripUnimplementedBreaker();
}

void SessionsDataConnection::EnsureConfigManager(
    std::string_view instance_name) {
  if (instance_name.empty() || stub_ptrs_.empty() ||
      closed_.load(std::memory_order_acquire)) {
    return;
  }
  std::shared_ptr<client::ClientConfigurationManager> manager_to_start;
  std::string const instance_key(instance_name);
  {
    std::lock_guard<std::mutex> l(mu_);
    if (closed_.load(std::memory_order_acquire)) return;
    if (config_managers_.find(instance_key) != config_managers_.end()) {
      return;
    }
    std::string const app_profile =
        options_.has<google::cloud::bigtable::AppProfileIdOption>()
            ? options_.get<google::cloud::bigtable::AppProfileIdOption>()
            : std::string{};
    std::weak_ptr<SessionsDataConnection> w_self = weak_from_this();
    manager_to_start = client::ClientConfigurationManager::Create(
        stub_ptrs_, instance_key, app_profile, scheduler_,
        /*on_session_load=*/nullptr,
        /*on_pool_config=*/
        [w_self,
         instance_key](google::bigtable::v2::SessionClientConfiguration::
                           SessionPoolConfiguration const& pool_cfg) {
          auto self = w_self.lock();
          if (!self) return;
          std::vector<std::shared_ptr<Client>> primary_clients;
          {
            std::lock_guard<std::mutex> lk(self->mu_);
            self->instance_pool_configs_[instance_key] = pool_cfg;
            self->client_directory_.ForEach([&](auto const& entry) {
              if (InstanceNameFromTableName(entry.table_name) == instance_key &&
                  entry.value[0] != nullptr) {
                primary_clients.push_back(entry.value[0]);
              }
            });
          }
          for (auto const& c : primary_clients) {
            c->UpdatePoolConfig(pool_cfg);
          }
        },
        /*on_diversion_loads=*/
        [w_self](client::DiversionLoads const& loads) {
          if (auto self = w_self.lock()) {
            self->read_row_diverter_.SetSessionLoad(loads.read_row);
            self->mutate_row_diverter_.SetSessionLoad(loads.mutate_row);
          }
        },
        context_setup_);
    config_managers_.emplace(instance_key, manager_to_start);
  }
  // Never hold SessionsDataConnection::mu_ across Start()
  // (SESSION_COMPONENT_SPEC.md §B8).
  if (manager_to_start != nullptr) {
    manager_to_start->Start();
  }
}

Options SessionsDataConnection::BuildPoolOptions(
    std::string const& table_name, std::string const& app_profile_id,
    google::bigtable::v2::OpenTableRequest::Permission permission) const {
  Options pool_opts = options_;
  if (executors_ != nullptr) {
    pool_opts.set<SessionsPoolExecutorsOption>(executors_);
  }
  if (context_setup_) {
    pool_opts.set<SessionsMetadataHookOption>(context_setup_);
  }
  bool const csm_enabled =
      options_.has<google::cloud::bigtable::EnableMetricsOption>() &&
      options_.get<google::cloud::bigtable::EnableMetricsOption>();
  pool_opts.set<SessionsClientSideMetricsEnabledOption>(csm_enabled);

  if (!pool_opts.has<SessionsFeatureFlagsOption>()) {
    google::bigtable::v2::FeatureFlags flags;
    flags.set_sessions_compatible(true);
    flags.set_sessions_required(classic_connection_ == nullptr);
    flags.set_reverse_scans(true);
    flags.set_last_scanned_row_responses(true);
    flags.set_routing_cookie(true);
    flags.set_retry_info(true);
    flags.set_peer_info(true);
    flags.set_client_side_metrics_enabled(csm_enabled);
    if (google::cloud::bigtable::internal::IsDirectPath(options_)) {
      flags.set_traffic_director_enabled(true);
      flags.set_direct_access_requested(true);
    }
    pool_opts.set<SessionsFeatureFlagsOption>(std::move(flags));
  }

  auto const parsed = ParseTableName(table_name);
  std::string const effective_profile =
      app_profile_id.empty() ? "default" : app_profile_id;
  std::string_view const dir =
      (permission == google::bigtable::v2::OpenTableRequest::PERMISSION_WRITE)
          ? "WRITE"
          : "READ";

  telemetry::SessionTracerConfig tracer_cfg;
  tracer_cfg.project_id = parsed.project_id;
  tracer_cfg.instance_id = parsed.instance_id;
  tracer_cfg.app_profile_id = effective_profile;
  tracer_cfg.client_name =
      "cpp.Bigtable/" + google::cloud::bigtable::version_string();
  tracer_cfg.session_type = "SESSION_TYPE_TABLE";
  tracer_cfg.session_name =
      absl::StrCat(table_name, "/", effective_profile, "/", dir);
  tracer_cfg.use_short_instrument_names = true;

#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
  if (auto const* metrics_factory =
          dynamic_cast<MetricsOperationContextFactory const*>(
              operation_context_factory_.get())) {
    tracer_cfg.meter_provider = metrics_factory->provider();
    tracer_cfg.client_uid = metrics_factory->client_uid();
    auto const& cl = metrics_factory->client_resource_labels();
    tracer_cfg.client_project = cl.client_project;
    tracer_cfg.region = cl.region;
    tracer_cfg.cloud_platform = cl.cloud_platform;
    tracer_cfg.host_id = cl.host_id;
    tracer_cfg.host_name = cl.host_name;
  }
#endif
  pool_opts.set<telemetry::SessionsTracerConfigOption>(std::move(tracer_cfg));
  return pool_opts;
}

std::shared_ptr<Client> SessionsDataConnection::GetClient(
    std::string const& table_name, std::string const& app_profile_id,
    google::bigtable::v2::OpenTableRequest::Permission permission) {
  if (closed_.load(std::memory_order_acquire)) return nullptr;
  std::string const effective_profile =
      app_profile_id.empty() ? "default" : app_profile_id;
  int const perm_key = static_cast<int>(permission);

  if (auto const* entry =
          client_directory_.Find(table_name, perm_key, effective_profile)) {
    return entry->value[CurrentThreadShardIndex()];
  }

  std::string_view const instance_name = InstanceNameFromTableName(table_name);
  EnsureConfigManager(instance_name);

  {
    std::lock_guard<std::mutex> l(mu_);
    if (closed_.load(std::memory_order_acquire)) return nullptr;
    if (auto const* existing =
            client_directory_.Find(table_name, perm_key, effective_profile)) {
      return existing->value[CurrentThreadShardIndex()];
    }
    if (client_directory_.size() >= client_directory_.max_entries()) {
      return nullptr;
    }
  }

  // Construct pool and 16 shards outside SessionsDataConnection::mu_ (§B8).
  Options pool_opts =
      BuildPoolOptions(table_name, effective_profile, permission);
  std::shared_ptr<Client> first_client;
  if (client_factory_) {
    first_client = client_factory_(table_name, effective_profile, permission,
                                   std::move(pool_opts), scheduler_);
  } else {
    first_client = Client::Create(stub_ptrs_, table_name, effective_profile,
                                  permission, std::move(pool_opts), scheduler_);
  }
  if (first_client == nullptr || first_client->pool() == nullptr) {
    // Failed opens must NOT be cached in ClientDirectory
    // (SESSION_CLIENT_SPEC.md #1).
    if (first_client != nullptr) first_client->Close();
    return nullptr;
  }

  ClientShards shards;
  shards[0] = first_client;
  auto const sync_cfg = first_client->sync_wait_config();
  for (std::size_t i = 1; i < kNumClientShards; ++i) {
    shards[i] = Client::CreateWithPool(first_client->pool(), scheduler_,
                                       sync_cfg, nullptr);
  }

  ClientShards raced_duplicate_to_close{};
  bool inserted = false;
  std::shared_ptr<Client> result;
  std::optional<google::bigtable::v2::SessionClientConfiguration::
                    SessionPoolConfiguration>
      seeded_cfg;
  {
    std::lock_guard<std::mutex> l(mu_);
    if (closed_.load(std::memory_order_acquire)) {
      raced_duplicate_to_close = std::move(shards);
    } else if (auto const* existing = client_directory_.Find(
                   table_name, perm_key, effective_profile)) {
      result = existing->value[CurrentThreadShardIndex()];
      raced_duplicate_to_close = std::move(shards);
    } else {
      auto const it_cfg =
          instance_pool_configs_.find(std::string(instance_name));
      if (it_cfg != instance_pool_configs_.end()) {
        seeded_cfg = it_cfg->second;
      }
      auto const* placed = client_directory_.Insert(table_name, perm_key,
                                                    shards, effective_profile);
      if (placed != nullptr) {
        inserted = true;
        result = placed->value[CurrentThreadShardIndex()];
      } else {
        raced_duplicate_to_close = std::move(shards);
      }
    }
  }

  for (auto& dup : raced_duplicate_to_close) {
    if (dup != nullptr) dup->Close();
  }
  if (inserted && seeded_cfg.has_value() && first_client != nullptr) {
    first_client->UpdatePoolConfig(*seeded_cfg);
  }
  return result;
}

void SessionsDataConnection::PreWarm(std::string const& table_name,
                                     std::string const& app_profile_id,
                                     bool prewarm_read, bool prewarm_write) {
  if (prewarm_read) {
    (void)GetClient(table_name, app_profile_id,
                    google::bigtable::v2::OpenTableRequest::PERMISSION_READ);
  }
  if (prewarm_write) {
    (void)GetClient(table_name, app_profile_id,
                    google::bigtable::v2::OpenTableRequest::PERMISSION_WRITE);
  }
}

StatusOr<std::pair<bool, google::cloud::bigtable::Row>>
SessionsDataConnection::ReadRow(std::string const& table_name,
                                std::string row_key,
                                google::cloud::bigtable::Filter filter) {
  auto current = google::cloud::internal::SaveCurrentOptions();
  if (HasPerCallCredentialsOverride(*current, options_) ||
      !read_row_diverter_.UseSession()) {
    return classic_connection_->ReadRow(table_name, std::move(row_key),
                                        std::move(filter));
  }

  std::string const& app_profile =
      current->get<google::cloud::bigtable::AppProfileIdOption>();
  auto client =
      GetClient(table_name, app_profile,
                google::bigtable::v2::OpenTableRequest::PERMISSION_READ);
  if (client == nullptr) {
    return classic_connection_->ReadRow(table_name, std::move(row_key),
                                        std::move(filter));
  }

  google::bigtable::v2::SessionReadRowRequest req;
  req.set_key(row_key);
  auto const& filter_proto = filter.as_proto();
  if (filter_proto.filter_case() !=
      google::bigtable::v2::RowFilter::FILTER_NOT_SET) {
    *req.mutable_filter() = filter_proto;
  }

  grpc::ClientContext ctx;
  google::cloud::internal::ConfigureContext(ctx, *current);
  auto op_ctx = operation_context_factory_->ReadRow(table_name, app_profile);
  auto policy = BuildCallPolicy(*current, op_ctx);

  auto resp = client->ReadRow(&ctx, std::move(req), std::move(policy));
  op_ctx->OnDone(resp.status());
  if (!resp.ok()) {
    if (resp.status().code() == StatusCode::kUnimplemented &&
        classic_connection_ != nullptr) {
      TripAllBreakers();
      return classic_connection_->ReadRow(table_name, std::move(row_key),
                                          std::move(filter));
    }
    return std::move(resp).status();
  }
  return TranslateReadRowResponse(*std::move(resp));
}

future<StatusOr<std::pair<bool, google::cloud::bigtable::Row>>>
SessionsDataConnection::AsyncReadRow(std::string const& table_name,
                                     std::string row_key,
                                     google::cloud::bigtable::Filter filter) {
  auto current = google::cloud::internal::SaveCurrentOptions();
  if (HasPerCallCredentialsOverride(*current, options_) ||
      !read_row_diverter_.UseSession()) {
    return classic_connection_->AsyncReadRow(table_name, std::move(row_key),
                                             std::move(filter));
  }

  std::string const& app_profile =
      current->get<google::cloud::bigtable::AppProfileIdOption>();
  auto client =
      GetClient(table_name, app_profile,
                google::bigtable::v2::OpenTableRequest::PERMISSION_READ);
  if (client == nullptr) {
    return classic_connection_->AsyncReadRow(table_name, std::move(row_key),
                                             std::move(filter));
  }

  google::bigtable::v2::SessionReadRowRequest req;
  req.set_key(row_key);
  auto const& filter_proto = filter.as_proto();
  if (filter_proto.filter_case() !=
      google::bigtable::v2::RowFilter::FILTER_NOT_SET) {
    *req.mutable_filter() = filter_proto;
  }

  auto ctx = std::make_shared<grpc::ClientContext>();
  google::cloud::internal::ConfigureContext(*ctx, *current);
  auto op_ctx = operation_context_factory_->ReadRow(table_name, app_profile);
  auto policy = BuildCallPolicy(*current, op_ctx);

  std::weak_ptr<Client> w_client = client;
  std::weak_ptr<grpc::ClientContext> w_ctx = ctx;
  auto shared_p = std::make_shared<
      promise<StatusOr<std::pair<bool, google::cloud::bigtable::Row>>>>(
      [w_client, w_ctx]() {
        if (auto c = w_ctx.lock()) {
          c->TryCancel();
          if (auto cl = w_client.lock()) {
            cl->CancelContext(c.get());
          }
        }
      });
  auto fut = shared_p->get_future();

  client->ReadRowAsync(
      ctx.get(), std::move(req),
      [self = shared_from_this(), ctx, op_ctx, current = std::move(current),
       table_name, row_key = std::move(row_key), filter = std::move(filter),
       shared_p](
          StatusOr<google::bigtable::v2::SessionReadRowResponse> resp) mutable {
        op_ctx->OnDone(resp.status());
        if (!resp.ok()) {
          if (resp.status().code() == StatusCode::kUnimplemented &&
              self->classic_connection_ != nullptr) {
            self->TripAllBreakers();
            google::cloud::internal::OptionsSpan span(current);
            self->classic_connection_
                ->AsyncReadRow(table_name, std::move(row_key),
                               std::move(filter))
                .then([shared_p](auto f) mutable {
                  shared_p->set_value(f.get());
                });
            return;
          }
          shared_p->set_value(std::move(resp).status());
          return;
        }
        shared_p->set_value(TranslateReadRowResponse(*std::move(resp)));
      },
      std::move(policy));
  return fut;
}

Status SessionsDataConnection::Apply(
    std::string const& table_name,
    google::cloud::bigtable::SingleRowMutation mut) {
  auto current = google::cloud::internal::SaveCurrentOptions();
  if (HasPerCallCredentialsOverride(*current, options_) ||
      !mutate_row_diverter_.UseSession()) {
    return classic_connection_->Apply(table_name, std::move(mut));
  }

  std::string const& app_profile =
      current->get<google::cloud::bigtable::AppProfileIdOption>();
  auto client =
      GetClient(table_name, app_profile,
                google::bigtable::v2::OpenTableRequest::PERMISSION_WRITE);
  if (client == nullptr) {
    return classic_connection_->Apply(table_name, std::move(mut));
  }

  google::bigtable::v2::MutateRowRequest mr_proto;
  mut.MoveTo(mr_proto);
  auto idempotent_policy =
      current->get<google::cloud::bigtable::IdempotentMutationPolicyOption>()
          ->clone();
  bool const is_idempotent = std::all_of(
      mr_proto.mutations().begin(), mr_proto.mutations().end(),
      [&idempotent_policy](google::bigtable::v2::Mutation const& m) {
        return idempotent_policy->is_idempotent(m);
      });

  google::bigtable::v2::SessionMutateRowRequest req;
  req.set_key(mr_proto.row_key());
  *req.mutable_mutations() = mr_proto.mutations();

  grpc::ClientContext ctx;
  google::cloud::internal::ConfigureContext(ctx, *current);
  auto op_ctx = operation_context_factory_->MutateRow(table_name, app_profile);
  auto policy = BuildCallPolicy(*current, op_ctx);

  auto resp =
      client->MutateRow(&ctx, std::move(req), is_idempotent, std::move(policy));
  op_ctx->OnDone(resp.status());
  if (!resp.ok()) {
    if (resp.status().code() == StatusCode::kUnimplemented &&
        classic_connection_ != nullptr) {
      TripAllBreakers();
      if (is_idempotent || middleware::NeverLeftClient(resp.status())) {
        return classic_connection_->Apply(
            table_name,
            google::cloud::bigtable::SingleRowMutation(std::move(mr_proto)));
      }
    }
    return std::move(resp).status();
  }
  return Status{};
}

future<Status> SessionsDataConnection::AsyncApply(
    std::string const& table_name,
    google::cloud::bigtable::SingleRowMutation mut) {
  auto current = google::cloud::internal::SaveCurrentOptions();
  if (HasPerCallCredentialsOverride(*current, options_) ||
      !mutate_row_diverter_.UseSession()) {
    return classic_connection_->AsyncApply(table_name, std::move(mut));
  }

  std::string const& app_profile =
      current->get<google::cloud::bigtable::AppProfileIdOption>();
  auto client =
      GetClient(table_name, app_profile,
                google::bigtable::v2::OpenTableRequest::PERMISSION_WRITE);
  if (client == nullptr) {
    return classic_connection_->AsyncApply(table_name, std::move(mut));
  }

  google::bigtable::v2::MutateRowRequest mr_proto;
  mut.MoveTo(mr_proto);
  auto idempotent_policy =
      current->get<google::cloud::bigtable::IdempotentMutationPolicyOption>()
          ->clone();
  bool const is_idempotent = std::all_of(
      mr_proto.mutations().begin(), mr_proto.mutations().end(),
      [&idempotent_policy](google::bigtable::v2::Mutation const& m) {
        return idempotent_policy->is_idempotent(m);
      });

  google::bigtable::v2::SessionMutateRowRequest req;
  req.set_key(mr_proto.row_key());
  *req.mutable_mutations() = mr_proto.mutations();

  auto ctx = std::make_shared<grpc::ClientContext>();
  google::cloud::internal::ConfigureContext(*ctx, *current);
  auto op_ctx = operation_context_factory_->MutateRow(table_name, app_profile);
  auto policy = BuildCallPolicy(*current, op_ctx);

  std::weak_ptr<Client> w_client = client;
  std::weak_ptr<grpc::ClientContext> w_ctx = ctx;
  auto shared_p = std::make_shared<promise<Status>>([w_client, w_ctx]() {
    if (auto c = w_ctx.lock()) {
      c->TryCancel();
      if (auto cl = w_client.lock()) {
        cl->CancelContext(c.get());
      }
    }
  });
  auto fut = shared_p->get_future();

  client->MutateRowAsync(
      ctx.get(), std::move(req),
      [self = shared_from_this(), ctx, op_ctx, current = std::move(current),
       table_name, mr_proto = std::move(mr_proto), is_idempotent,
       shared_p](StatusOr<google::bigtable::v2::SessionMutateRowResponse>
                     resp) mutable {
        op_ctx->OnDone(resp.status());
        if (!resp.ok()) {
          if (resp.status().code() == StatusCode::kUnimplemented &&
              self->classic_connection_ != nullptr) {
            self->TripAllBreakers();
            if (is_idempotent || middleware::NeverLeftClient(resp.status())) {
              google::cloud::internal::OptionsSpan span(current);
              self->classic_connection_
                  ->AsyncApply(table_name,
                               google::cloud::bigtable::SingleRowMutation(
                                   std::move(mr_proto)))
                  .then([shared_p](auto f) mutable {
                    shared_p->set_value(f.get());
                  });
              return;
            }
          }
          shared_p->set_value(std::move(resp).status());
          return;
        }
        shared_p->set_value(Status{});
      },
      is_idempotent, std::move(policy));
  return fut;
}

std::vector<google::cloud::bigtable::FailedMutation>
SessionsDataConnection::BulkApply(std::string const& table_name,
                                  google::cloud::bigtable::BulkMutation mut) {
  return classic_connection_->BulkApply(table_name, std::move(mut));
}

future<std::vector<google::cloud::bigtable::FailedMutation>>
SessionsDataConnection::AsyncBulkApply(
    std::string const& table_name, google::cloud::bigtable::BulkMutation mut) {
  return classic_connection_->AsyncBulkApply(table_name, std::move(mut));
}

google::cloud::bigtable::RowReader SessionsDataConnection::ReadRowsFull(
    google::cloud::bigtable::ReadRowsParams params) {
  return classic_connection_->ReadRowsFull(std::move(params));
}

StatusOr<google::cloud::bigtable::MutationBranch>
SessionsDataConnection::CheckAndMutateRow(
    std::string const& table_name, std::string row_key,
    google::cloud::bigtable::Filter filter,
    std::vector<google::cloud::bigtable::Mutation> true_mutations,
    std::vector<google::cloud::bigtable::Mutation> false_mutations) {
  return classic_connection_->CheckAndMutateRow(
      table_name, std::move(row_key), std::move(filter),
      std::move(true_mutations), std::move(false_mutations));
}

future<StatusOr<google::cloud::bigtable::MutationBranch>>
SessionsDataConnection::AsyncCheckAndMutateRow(
    std::string const& table_name, std::string row_key,
    google::cloud::bigtable::Filter filter,
    std::vector<google::cloud::bigtable::Mutation> true_mutations,
    std::vector<google::cloud::bigtable::Mutation> false_mutations) {
  return classic_connection_->AsyncCheckAndMutateRow(
      table_name, std::move(row_key), std::move(filter),
      std::move(true_mutations), std::move(false_mutations));
}

StatusOr<std::vector<google::cloud::bigtable::RowKeySample>>
SessionsDataConnection::SampleRows(std::string const& table_name) {
  return classic_connection_->SampleRows(table_name);
}

future<StatusOr<std::vector<google::cloud::bigtable::RowKeySample>>>
SessionsDataConnection::AsyncSampleRows(std::string const& table_name) {
  return classic_connection_->AsyncSampleRows(table_name);
}

StatusOr<google::cloud::bigtable::Row>
SessionsDataConnection::ReadModifyWriteRow(
    google::bigtable::v2::ReadModifyWriteRowRequest request) {
  return classic_connection_->ReadModifyWriteRow(std::move(request));
}

future<StatusOr<google::cloud::bigtable::Row>>
SessionsDataConnection::AsyncReadModifyWriteRow(
    google::bigtable::v2::ReadModifyWriteRowRequest request) {
  return classic_connection_->AsyncReadModifyWriteRow(std::move(request));
}

void SessionsDataConnection::AsyncReadRows(
    std::string const& table_name,
    std::function<future<bool>(google::cloud::bigtable::Row)> on_row,
    std::function<void(Status)> on_finish,
    google::cloud::bigtable::RowSet row_set, std::int64_t rows_limit,
    google::cloud::bigtable::Filter filter) {
  classic_connection_->AsyncReadRows(table_name, std::move(on_row),
                                     std::move(on_finish), std::move(row_set),
                                     rows_limit, std::move(filter));
}

StatusOr<google::cloud::bigtable::PreparedQuery>
SessionsDataConnection::PrepareQuery(
    google::cloud::bigtable::PrepareQueryParams const& params) {
  return classic_connection_->PrepareQuery(params);
}

future<StatusOr<google::cloud::bigtable::PreparedQuery>>
SessionsDataConnection::AsyncPrepareQuery(
    google::cloud::bigtable::PrepareQueryParams const& params) {
  return classic_connection_->AsyncPrepareQuery(params);
}

google::cloud::bigtable::RowStream SessionsDataConnection::ExecuteQuery(
    google::cloud::bigtable::ExecuteQueryParams params) {
  return classic_connection_->ExecuteQuery(std::move(params));
}

std::shared_ptr<google::cloud::bigtable::DataConnection>
MakeSessionsDataConnection(
    std::shared_ptr<google::cloud::bigtable::DataConnection> classic_connection,
    std::shared_ptr<OperationContextFactory> operation_context_factory,
    std::vector<google::cloud::bigtable::InstanceResource> const& instances,
    Options options) {
  return SessionsDataConnection::Create(std::move(classic_connection),
                                        std::move(operation_context_factory),
                                        instances, std::move(options));
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

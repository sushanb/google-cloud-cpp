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

#include "google/cloud/bigtable/internal/sessions/client/client_configuration_manager.h"
#include "google/cloud/bigtable/internal/sessions/logging_helper.h"
#include "google/cloud/internal/url_encode.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include <google/bigtable/v2/feature_flags.pb.h>
#include <google/protobuf/duration.pb.h>
#include <grpcpp/client_context.h>
#include <algorithm>
#include <chrono>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace client {

DiversionLoads ResolveDiversionLoads(
    google::bigtable::v2::SessionClientConfiguration const& session_cfg) {
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
  double const legacy = static_cast<double>(session_cfg.session_load());
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
  DiversionLoads loads{legacy, legacy};
  if (session_cfg.has_session_diversion_configuration()) {
    auto const& div = session_cfg.session_diversion_configuration();
    switch (div.diversion_strategy_case()) {
      case google::bigtable::v2::SessionDiversionConfiguration::
          kGlobalDiversion: {
        double const g =
            static_cast<double>(div.global_diversion().session_load());
        loads.read_row = g;
        loads.mutate_row = g;
        break;
      }
      case google::bigtable::v2::SessionDiversionConfiguration::
          kPerScopeDiversion: {
        auto const& scopes = div.per_scope_diversion().scope_diversions();
        auto const r_it = scopes.find("method:Bigtable.ReadRow");
        loads.read_row = (r_it != scopes.end())
                             ? static_cast<double>(r_it->second.session_load())
                             : 0.0;
        auto const w_it = scopes.find("method:Bigtable.MutateRow");
        loads.mutate_row =
            (w_it != scopes.end())
                ? static_cast<double>(w_it->second.session_load())
                : 0.0;
        break;
      }
      case google::bigtable::v2::SessionDiversionConfiguration::
          DIVERSION_STRATEGY_NOT_SET:
        break;
    }
  }
  return loads;
}

std::shared_ptr<ClientConfigurationManager> ClientConfigurationManager::Create(
    std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
    std::string instance_name, std::string app_profile_id, ScheduleFn scheduler,
    SessionLoadCallback on_session_load, PoolConfigCallback on_pool_config,
    DiversionLoadsCallback on_diversion_loads, ContextSetupFn context_setup) {
  return std::shared_ptr<ClientConfigurationManager>(
      new ClientConfigurationManager(
          std::move(stubs), nullptr, std::move(instance_name),
          std::move(app_profile_id), std::move(scheduler),
          std::move(on_session_load), std::move(on_pool_config),
          std::move(on_diversion_loads), std::move(context_setup)));
}

std::shared_ptr<ClientConfigurationManager>
ClientConfigurationManager::CreateForTesting(
    FetchFn fetch_fn, std::string instance_name, std::string app_profile_id,
    ScheduleFn scheduler, SessionLoadCallback on_session_load,
    PoolConfigCallback on_pool_config,
    DiversionLoadsCallback on_diversion_loads) {
  return std::shared_ptr<ClientConfigurationManager>(
      new ClientConfigurationManager(
          {}, std::move(fetch_fn), std::move(instance_name),
          std::move(app_profile_id), std::move(scheduler),
          std::move(on_session_load), std::move(on_pool_config),
          std::move(on_diversion_loads), nullptr));
}

ClientConfigurationManager::ClientConfigurationManager(
    std::vector<google::bigtable::v2::Bigtable::Stub*> stubs, FetchFn fetch_fn,
    std::string instance_name, std::string app_profile_id, ScheduleFn scheduler,
    SessionLoadCallback on_session_load, PoolConfigCallback on_pool_config,
    DiversionLoadsCallback on_diversion_loads, ContextSetupFn context_setup)
    : stubs_(std::move(stubs)),
      fetch_fn_(std::move(fetch_fn)),
      instance_name_(std::move(instance_name)),
      app_profile_id_(std::move(app_profile_id)),
      scheduler_(std::move(scheduler)),
      on_session_load_(std::move(on_session_load)),
      on_pool_config_(std::move(on_pool_config)),
      on_diversion_loads_(std::move(on_diversion_loads)),
      context_setup_(std::move(context_setup)) {}

void ClientConfigurationManager::Start() {
  if (started_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  if ((stubs_.empty() && fetch_fn_ == nullptr) || scheduler_ == nullptr) {
    return;
  }
  ScheduleNextPoll(std::chrono::nanoseconds::zero());
}

void ClientConfigurationManager::Stop() {
  stopped_.store(true, std::memory_order_release);
  {
    std::unique_lock<std::mutex> l_fetch(fetch_mu_);
    for (auto* ctx : active_contexts_) {
      if (ctx != nullptr) {
        ctx->TryCancel();
      }
    }
    fetch_cv_.wait(l_fetch, [this] { return in_flight_fetches_ == 0; });
  }
  // Wait for any in-flight callback delivery to finish.
  std::lock_guard<std::mutex> l(callback_mu_);
}

bool ClientConfigurationManager::FetchAndApplyOnce() {
  if (stopped_.load(std::memory_order_acquire) ||
      (stubs_.empty() && fetch_fn_ == nullptr)) {
    return false;
  }
  {
    std::lock_guard<std::mutex> l_fetch(fetch_mu_);
    if (stopped_.load(std::memory_order_acquire)) {
      return false;
    }
    ++in_flight_fetches_;
  }
  struct FetchGuard {
    ClientConfigurationManager* self;
    ~FetchGuard() {
      std::lock_guard<std::mutex> l_fetch(self->fetch_mu_);
      if (--self->in_flight_fetches_ == 0) {
        self->fetch_cv_.notify_all();
      }
    }
  } fetch_guard{this};

  std::uint64_t const fetch_seq =
      next_fetch_seq_.fetch_add(1, std::memory_order_relaxed);
  int max_retries = 3;
  {
    std::lock_guard<std::mutex> l(callback_mu_);
    max_retries = max_rpc_retry_count_;
  }

  google::bigtable::v2::FeatureFlags feature_flags;
  feature_flags.set_sessions_compatible(true);
  feature_flags.set_sessions_required(true);
  feature_flags.set_routing_cookie(true);
  feature_flags.set_retry_info(true);
  feature_flags.set_peer_info(true);
  std::string const encoded_flags =
      absl::WebSafeBase64Escape(feature_flags.SerializeAsString());
  std::string const request_params = absl::StrCat(
      "instance_name=", google::cloud::internal::UrlEncode(instance_name_),
      "&app_profile_id=", google::cloud::internal::UrlEncode(app_profile_id_));

  google::bigtable::v2::GetClientConfigurationRequest req;
  req.set_instance_name(instance_name_);
  req.set_app_profile_id(app_profile_id_);

  google::bigtable::v2::ClientConfiguration resp;
  grpc::Status status(grpc::StatusCode::UNAVAILABLE, "No stub available");
  for (int attempt = 0; attempt <= max_retries; ++attempt) {
    if (stopped_.load(std::memory_order_acquire)) {
      return false;
    }
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(5));
    ctx.AddMetadata("bigtable-features", encoded_flags);
    ctx.AddMetadata("x-goog-request-params", request_params);
    if (context_setup_) {
      context_setup_(ctx);
    }

    {
      std::lock_guard<std::mutex> l_fetch(fetch_mu_);
      if (stopped_.load(std::memory_order_acquire)) {
        return false;
      }
      active_contexts_.insert(&ctx);
    }
    resp.Clear();
    if (fetch_fn_ != nullptr) {
      status = fetch_fn_(ctx, req, &resp);
    } else {
      auto* stub =
          stubs_[next_stub_idx_.fetch_add(1, std::memory_order_relaxed) %
                 stubs_.size()];
      if (stub == nullptr) {
        std::lock_guard<std::mutex> l_fetch(fetch_mu_);
        active_contexts_.erase(&ctx);
        return false;
      }
      status = stub->GetClientConfiguration(&ctx, req, &resp);
    }
    {
      std::lock_guard<std::mutex> l_fetch(fetch_mu_);
      active_contexts_.erase(&ctx);
    }
    if (status.ok()) {
      break;
    }
    if (stopped_.load(std::memory_order_acquire) ||
        status.error_code() == grpc::StatusCode::UNIMPLEMENTED ||
        status.error_code() == grpc::StatusCode::PERMISSION_DENIED ||
        status.error_code() == grpc::StatusCode::UNAUTHENTICATED ||
        status.error_code() == grpc::StatusCode::INVALID_ARGUMENT) {
      break;
    }
  }

  if (!status.ok()) {
    ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
        2, "ClientConfigurationManager",
        absl::StrCat("GetClientConfiguration failed: ",
                     status.error_message()));
    SessionLoadCallback load_cb;
    DiversionLoadsCallback div_cb;
    {
      std::lock_guard<std::mutex> l(callback_mu_);
      if (!stopped_.load(std::memory_order_acquire) &&
          Clock::now() >= config_valid_until_) {
        config_valid_until_ = (TimePoint::max)();
        load_cb = on_session_load_;
        div_cb = on_diversion_loads_;
      }
    }
    if (div_cb) {
      div_cb(DiversionLoads{1.0, 1.0});
    }
    if (load_cb) {
      load_cb(1.0);
    }
    return false;
  }

  fetch_success_count_.fetch_add(1, std::memory_order_relaxed);
  SessionLoadCallback load_cb;
  DiversionLoadsCallback div_cb;
  DiversionLoads loads{0.0, 0.0};
  PoolConfigCallback pool_cb;
  google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
      pool_cfg;
  {
    std::lock_guard<std::mutex> l(callback_mu_);
    if (stopped_.load(std::memory_order_acquire)) return true;
    if (fetch_seq < latest_applied_seq_) return true;
    latest_applied_seq_ = fetch_seq;

    if (resp.stop_polling()) {
      stopped_.store(true, std::memory_order_release);
    } else if (resp.has_polling_configuration()) {
      auto const& pc = resp.polling_configuration();
      if (pc.has_polling_interval()) {
        auto const d = std::chrono::seconds(pc.polling_interval().seconds()) +
                       std::chrono::nanoseconds(pc.polling_interval().nanos());
        if (d > std::chrono::nanoseconds::zero()) {
          next_poll_interval_ =
              std::max(d, std::chrono::nanoseconds(std::chrono::minutes(1)));
        }
      }
      if (pc.has_validity_duration()) {
        auto const v = std::chrono::seconds(pc.validity_duration().seconds()) +
                       std::chrono::nanoseconds(pc.validity_duration().nanos());
        if (v > std::chrono::nanoseconds::zero()) {
          config_valid_until_ = Clock::now() + v;
        }
      }
      if (pc.max_rpc_retry_count() >= 0) {
        max_rpc_retry_count_ = pc.max_rpc_retry_count();
      }
    } else {
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
      if (resp.has_polling_interval()) {
        auto const d =
            std::chrono::seconds(resp.polling_interval().seconds()) +
            std::chrono::nanoseconds(resp.polling_interval().nanos());
        if (d > std::chrono::nanoseconds::zero()) {
          next_poll_interval_ =
              std::max(d, std::chrono::nanoseconds(std::chrono::minutes(1)));
        }
      }
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
    }

    if (resp.has_session_configuration()) {
      auto const& session_cfg = resp.session_configuration();
      loads = ResolveDiversionLoads(session_cfg);
      load_cb = on_session_load_;
      div_cb = on_diversion_loads_;
      if (session_cfg.has_session_pool_configuration() && on_pool_config_) {
        pool_cb = on_pool_config_;
        pool_cfg = session_cfg.session_pool_configuration();
      }
    }
  }
  if (div_cb) {
    div_cb(loads);
  }
  if (load_cb) {
    load_cb(loads.read_row);
  }
  if (pool_cb) {
    pool_cb(pool_cfg);
  }
  return true;
}

void ClientConfigurationManager::ScheduleNextPoll(
    std::chrono::nanoseconds delay) {
  if (stopped_.load(std::memory_order_acquire) || scheduler_ == nullptr) {
    return;
  }
  std::weak_ptr<ClientConfigurationManager> weak_self = shared_from_this();
  scheduler_(delay, [weak_self]() {
    if (auto self = weak_self.lock()) {
      if (self->stopped_.load(std::memory_order_acquire)) return;
      (void)self->FetchAndApplyOnce();
      std::chrono::nanoseconds next_delay = std::chrono::minutes(5);
      {
        std::lock_guard<std::mutex> l(self->callback_mu_);
        next_delay = self->next_poll_interval_;
      }
      self->ScheduleNextPoll(next_delay);
    }
  });
}

}  // namespace client
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

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
#include "google/cloud/bigtable/internal/sessions/latch.h"
#include <gmock/gmock.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>
#include <atomic>
#include <chrono>
#include <optional>
#include <thread>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace client {
namespace {

using ::google::bigtable::v2::ClientConfiguration;
using ::google::bigtable::v2::GetClientConfigurationRequest;
using ::google::bigtable::v2::SessionClientConfiguration;

TEST(ClientConfigurationManagerTest, ResolvesGlobalAndPerScopeDiversionLoads) {
  {
    SessionClientConfiguration cfg;
    cfg.mutable_session_diversion_configuration()
        ->mutable_global_diversion()
        ->set_session_load(0.75f);
    auto loads = ResolveDiversionLoads(cfg);
    EXPECT_NEAR(loads.read_row, 0.75, 1e-5);
    EXPECT_NEAR(loads.mutate_row, 0.75, 1e-5);
  }
  {
    SessionClientConfiguration cfg;
    auto* scopes = cfg.mutable_session_diversion_configuration()
                       ->mutable_per_scope_diversion()
                       ->mutable_scope_diversions();
    (*scopes)["method:Bigtable.ReadRow"].set_session_load(0.8f);
    (*scopes)["method:Bigtable.MutateRow"].set_session_load(0.25f);
    auto loads = ResolveDiversionLoads(cfg);
    EXPECT_NEAR(loads.read_row, 0.8, 1e-5);
    EXPECT_NEAR(loads.mutate_row, 0.25, 1e-5);
  }
  {
    // Unlisted method scope under PerScopeDiversion defaults to 0.0.
    SessionClientConfiguration cfg;
    auto* scopes = cfg.mutable_session_diversion_configuration()
                       ->mutable_per_scope_diversion()
                       ->mutable_scope_diversions();
    (*scopes)["method:Bigtable.ReadRow"].set_session_load(1.0f);
    auto loads = ResolveDiversionLoads(cfg);
    EXPECT_NEAR(loads.read_row, 1.0, 1e-5);
    EXPECT_NEAR(loads.mutate_row, 0.0, 1e-5);
  }
  {
    SessionClientConfiguration cfg;
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
    cfg.set_session_load(0.5f);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
    auto loads = ResolveDiversionLoads(cfg);
    EXPECT_NEAR(loads.read_row, 0.5, 1e-5);
    EXPECT_NEAR(loads.mutate_row, 0.5, 1e-5);
  }
}

TEST(ClientConfigurationManagerTest,
     ClampsPollingIntervalToOneMinuteAndDeliversPoolConfig) {
  std::optional<DiversionLoads> delivered_loads;
  std::optional<SessionClientConfiguration::SessionPoolConfiguration>
      delivered_pool_cfg;

  auto mgr = ClientConfigurationManager::CreateForTesting(
      [](grpc::ClientContext&, GetClientConfigurationRequest const& req,
         ClientConfiguration* resp) {
        EXPECT_EQ(req.instance_name(), "projects/p/instances/i");
        EXPECT_EQ(req.app_profile_id(), "prof");
        resp->mutable_polling_configuration()
            ->mutable_polling_interval()
            ->set_seconds(10);  // Below 60s minimum -> clamped to 60s.
        auto* sc = resp->mutable_session_configuration();
        sc->mutable_session_diversion_configuration()
            ->mutable_global_diversion()
            ->set_session_load(0.6f);
        sc->mutable_session_pool_configuration()->set_min_session_count(4);
        sc->mutable_session_pool_configuration()->set_max_session_count(32);
        return grpc::Status::OK;
      },
      "projects/p/instances/i", "prof", nullptr, nullptr,
      [&](auto const& pc) { delivered_pool_cfg = pc; },
      [&](DiversionLoads const& l) { delivered_loads = l; });

  EXPECT_TRUE(mgr->FetchAndApplyOnce());
  EXPECT_EQ(mgr->fetch_success_count(), 1u);
  EXPECT_EQ(mgr->next_poll_interval(), std::chrono::minutes(1));
  ASSERT_TRUE(delivered_loads.has_value());
  EXPECT_NEAR(delivered_loads->read_row, 0.6, 1e-5);
  EXPECT_NEAR(delivered_loads->mutate_row, 0.6, 1e-5);
  ASSERT_TRUE(delivered_pool_cfg.has_value());
  EXPECT_EQ(delivered_pool_cfg->min_session_count(), 4);
  EXPECT_EQ(delivered_pool_cfg->max_session_count(), 32);
}

TEST(ClientConfigurationManagerTest,
     ExpiredValidityDurationFallsBackToFullSessionLoadOnFailure) {
  int call = 0;
  DiversionLoads latest_loads{0.0, 0.0};
  auto mgr = ClientConfigurationManager::CreateForTesting(
      [&](grpc::ClientContext&, GetClientConfigurationRequest const&,
          ClientConfiguration* resp) {
        if (++call == 1) {
          resp->mutable_polling_configuration()
              ->mutable_validity_duration()
              ->set_nanos(5000000);  // 5ms TTL
          resp->mutable_polling_configuration()->set_max_rpc_retry_count(0);
          resp->mutable_session_configuration()
              ->mutable_session_diversion_configuration()
              ->mutable_global_diversion()
              ->set_session_load(0.25f);
          return grpc::Status::OK;
        }
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "outage");
      },
      "projects/p/instances/i", "default", nullptr, nullptr, nullptr,
      [&](DiversionLoads const& l) { latest_loads = l; });

  ASSERT_TRUE(mgr->FetchAndApplyOnce());
  EXPECT_NEAR(latest_loads.read_row, 0.25, 1e-5);

  std::this_thread::sleep_for(std::chrono::milliseconds(15));
  EXPECT_FALSE(mgr->FetchAndApplyOnce());
  EXPECT_NEAR(latest_loads.read_row, 1.0, 1e-5);
  EXPECT_NEAR(latest_loads.mutate_row, 1.0, 1e-5);
}

class HangingConfigService final
    : public google::bigtable::v2::Bigtable::Service {
 public:
  grpc::Status GetClientConfiguration(
      grpc::ServerContext* context,
      GetClientConfigurationRequest const* /*request*/,
      ClientConfiguration* /*response*/) override {
    rpc_started.Notify();
    while (!context->IsCancelled()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    rpc_cancelled.Notify();
    return grpc::Status(grpc::StatusCode::CANCELLED, "cancelled");
  }

  Latch rpc_started;
  Latch rpc_cancelled;
};

TEST(ClientConfigurationManagerTest, StopCancelsInFlightRpcAndWaitsForDrain) {
  HangingConfigService service;
  grpc::ServerBuilder builder;
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);
  auto channel = server->InProcessChannel(grpc::ChannelArguments{});
  auto stub = google::bigtable::v2::Bigtable::NewStub(channel);

  auto mgr =
      ClientConfigurationManager::Create({stub.get()}, "projects/p/instances/i",
                                         "default", nullptr, nullptr, nullptr);

  std::atomic<bool> fetch_returned{false};
  std::thread worker([&] {
    EXPECT_FALSE(mgr->FetchAndApplyOnce());
    fetch_returned.store(true, std::memory_order_release);
  });
  service.rpc_started.Wait();
  mgr->Stop();
  EXPECT_TRUE(fetch_returned.load(std::memory_order_acquire));
  EXPECT_TRUE(service.rpc_cancelled.WaitFor(std::chrono::seconds(5)));
  worker.join();
  server->Shutdown();
}

}  // namespace
}  // namespace client
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "google/cloud/internal/disable_deprecation_warnings.inc"
#include "google/cloud/bigtable/data_connection.h"
#include "google/cloud/bigtable/internal/bigtable_stub_factory.h"
#include "google/cloud/bigtable/internal/data_connection_impl.h"
#include "google/cloud/bigtable/options.h"
#include "google/cloud/common_options.h"
#include "google/cloud/credentials.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/internal/background_threads_impl.h"
#include "google/cloud/testing_util/opentelemetry_matchers.h"
#include "google/cloud/testing_util/scoped_environment.h"
#include "rpc_retry_policy.h"
#include <gmock/gmock.h>
#include <chrono>
#include <future>
#include <memory>
#include <optional>

namespace google {
namespace cloud {
namespace bigtable {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {

using ms = std::chrono::milliseconds;
using ::google::cloud::testing_util::DisableTracing;
using ::google::cloud::testing_util::EnableTracing;
using ::google::cloud::testing_util::SpanNamed;
using ::testing::Contains;
using ::testing::Eq;
using ::testing::IsEmpty;

Options TestOptions() {
  return Options{}
      // Stay on this machine. Don't make a connection to the actual service.
      .set<EndpointOption>("localhost:1")
      .set<UnifiedCredentialsOption>(MakeInsecureCredentials())
      // Disable channel refreshing, which is not under test.
      .set<MaxConnectionRefreshOption>(ms(0))
      // Create the minimum number of stubs.
      .set<GrpcNumChannelsOption>(1)
      // Disable retries
      .set<DataRetryPolicyOption>(
          std::make_shared<DataLimitedErrorCountRetryPolicy>(0));
}

TEST(MakeDataConnection, DefaultsOptions) {
  auto conn = MakeDataConnection(
      TestOptions().set<AppProfileIdOption>("user-supplied"));
  auto options = conn->options();
  EXPECT_TRUE(options.has<DataBackoffPolicyOption>())
      << "Options are not defaulted in MakeDataConnection()";
  EXPECT_EQ(options.get<AppProfileIdOption>(), "user-supplied")
      << "User supplied Options are overridden in MakeDataConnection()";
}

TEST(MakeDataConnection, TracingEnabled) {
  auto span_catcher = testing_util::InstallSpanCatcher();

  auto conn = MakeDataConnection(EnableTracing(TestOptions()));
  google::cloud::internal::OptionsSpan o(conn->options());
  (void)conn->Apply("table-name", SingleRowMutation("row"));

  EXPECT_THAT(span_catcher->GetSpans(),
              Contains(SpanNamed("bigtable::Table::Apply")));
}

TEST(MakeDataConnection, TracingDisabled) {
  auto span_catcher = testing_util::InstallSpanCatcher();

  auto conn = MakeDataConnection(DisableTracing(TestOptions()));
  google::cloud::internal::OptionsSpan o(conn->options());
  (void)conn->Apply("table-name", SingleRowMutation("row"));

  EXPECT_THAT(span_catcher->GetSpans(), IsEmpty());
}

TEST(MakeDataConnection, WithInstances) {
  InstanceResource instance_a{Project("my-project"), "instance-a"};
  InstanceResource instance_b{Project("my-project"), "instance-b"};
  auto conn = MakeDataConnection(
      {instance_a, instance_b},
      TestOptions().set<AppProfileIdOption>("user-supplied"));
  auto options = conn->options();
  EXPECT_TRUE(options.has<DataBackoffPolicyOption>())
      << "Options are not defaulted in MakeDataConnection()";
  EXPECT_EQ(options.get<AppProfileIdOption>(), "user-supplied")
      << "User supplied Options are overridden in MakeDataConnection()";
  ASSERT_TRUE(options.has<bigtable_internal::InstanceChannelAffinityOption>());
  EXPECT_THAT(
      options.get<bigtable_internal::InstanceChannelAffinityOption>().size(),
      Eq(2));
}

#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
TEST(MakeDataConnection, TestingOtelCollectorEnvVar) {
  testing_util::ScopedEnvironment env("GOOGLE_CLOUD_CPP_TESTING_OTEL_COLLECTOR",
                                      "1");
  auto conn = MakeDataConnection(TestOptions().set<EnableMetricsOption>(true));
  EXPECT_NE(conn, nullptr);
}

TEST(MakeDataConnection, DirectPathMetricsModeOptionDisabled) {
  testing_util::ScopedEnvironment env("GOOGLE_CLOUD_CPP_TESTING_OTEL_COLLECTOR",
                                      "1");
  InstanceResource instance_a{Project("my-project"), "instance-a"};
  auto conn = MakeDataConnection(
      {instance_a}, TestOptions()
                        .set<EnableMetricsOption>(true)
                        .set<experimental::DirectPathMetricsModeOption>(
                            experimental::DirectPathMetricsMode::kDisabled));
  EXPECT_NE(conn, nullptr);
}

TEST(MakeDataConnection, DirectPathMetricsModeOptionEnabled) {
  testing_util::ScopedEnvironment env("GOOGLE_CLOUD_CPP_TESTING_OTEL_COLLECTOR",
                                      "1");
  InstanceResource instance_a{Project("my-project"), "instance-a"};
  auto conn = MakeDataConnection(
      {instance_a}, TestOptions()
                        .set<EnableMetricsOption>(true)
                        .set<experimental::DirectPathModeOption>(
                            experimental::DirectPathMode::kEnabled)
                        .set<experimental::DirectPathMetricsModeOption>(
                            experimental::DirectPathMetricsMode::kEnabled));
  EXPECT_NE(conn, nullptr);
}

TEST(MakeDataConnection, DirectPathModeOptionDisabledNoGrpcMetrics) {
  testing_util::ScopedEnvironment env("GOOGLE_CLOUD_CPP_TESTING_OTEL_COLLECTOR",
                                      "1");
  InstanceResource instance_a{Project("my-project"), "instance-a"};
  auto conn = MakeDataConnection(
      {instance_a}, TestOptions()
                        .set<EnableMetricsOption>(true)
                        .set<experimental::DirectPathModeOption>(
                            experimental::DirectPathMode::kDisabled)
                        .set<experimental::DirectPathMetricsModeOption>(
                            experimental::DirectPathMetricsMode::kEnabled));
  EXPECT_NE(conn, nullptr);
}

TEST(MakeDataConnection, DirectPathInitializationModeBlocking) {
  InstanceResource instance_a{Project("my-project"), "instance-a"};
  auto conn = MakeDataConnection(
      {instance_a},
      TestOptions()
          .set<experimental::DirectPathModeOption>(
              experimental::DirectPathMode::kEnabled)
          .set<experimental::DirectPathInitializationModeOption>(
              experimental::DirectPathInitializationMode::kBlocking));
  EXPECT_NE(conn, nullptr);
}

TEST(MakeDataConnection, DirectPathInitializationModeSpeculative) {
  InstanceResource instance_a{Project("my-project"), "instance-a"};
  auto conn = MakeDataConnection(
      {instance_a}, TestOptions()
                        .set<experimental::DirectPathModeOption>(
                            experimental::DirectPathMode::kEnabled)
                        .set<experimental::DirectPathInitializationModeOption>(
                            experimental::DirectPathInitializationMode::
                                kAsynchronousSpeculative));
  EXPECT_NE(conn, nullptr);
}

void VerifyDirectPathFallbackDoesNotBlockCqOrTeardown(
    experimental::DirectPathInitializationMode mode) {
  testing_util::ScopedEnvironment collector_env(
      "GOOGLE_CLOUD_CPP_TESTING_OTEL_COLLECTOR", "1");
  // Point GCE_METADATA_HOST at a non-routable RFC 5737 TEST-NET-1 endpoint so
  // that on GCE hosts where IsGoogleCloudBios() is true, fallback diagnostics
  // exercises an unreachable metadata TCP connect attempt.
  testing_util::ScopedEnvironment metadata_env("GCE_METADATA_HOST",
                                               "192.0.2.1:80");

  auto captured_cq = std::make_shared<std::optional<CompletionQueue>>();
  BackgroundThreadsFactory single_thread_factory =
      [captured_cq]() -> std::unique_ptr<BackgroundThreads> {
    auto bg = std::make_unique<
        google::cloud::internal::AutomaticallyCreatedBackgroundThreads>(1);
    if (!captured_cq->has_value()) {
      *captured_cq = bg->cq();
    }
    return bg;
  };

  InstanceResource instance_a{Project("my-project"), "instance-a"};
  auto conn = MakeDataConnection(
      {instance_a},
      TestOptions()
          .set<EnableMetricsOption>(true)
          .set<GrpcBackgroundThreadsFactoryOption>(
              std::move(single_thread_factory))
          .set<experimental::DirectPathModeOption>(
              experimental::DirectPathMode::kEnabled)
          .set<experimental::DirectPathInitializationModeOption>(mode)
          .set<experimental::DirectPathProbeTimeoutOption>(ms(50))
          .set<experimental::DirectPathDiagnosticsTimeoutOption>(
              std::chrono::seconds(30)));
  ASSERT_NE(conn, nullptr);
  ASSERT_TRUE(captured_cq->has_value());

  // Verify the single-thread background CompletionQueue remains responsive to
  // async callbacks and timers immediately after fallback.
  std::promise<void> cq_work_promise;
  auto cq_work_future = cq_work_promise.get_future();
  captured_cq->value().RunAsync(
      [&cq_work_promise] { cq_work_promise.set_value(); });
  auto timer_future = captured_cq->value().MakeRelativeTimer(ms(5));

  EXPECT_EQ(cq_work_future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_EQ(timer_future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);

  captured_cq->reset();
  auto const teardown_start = std::chrono::steady_clock::now();
  conn.reset();
  auto const teardown_elapsed =
      std::chrono::steady_clock::now() - teardown_start;
  EXPECT_LT(teardown_elapsed, std::chrono::seconds(5));
}

TEST(MakeDataConnection,
     DirectPathFallbackDoesNotBlockSingleThreadCqOrTeardownBlocking) {
  VerifyDirectPathFallbackDoesNotBlockCqOrTeardown(
      experimental::DirectPathInitializationMode::kBlocking);
}

TEST(MakeDataConnection,
     DirectPathFallbackDoesNotBlockSingleThreadCqOrTeardownSpeculative) {
  VerifyDirectPathFallbackDoesNotBlockCqOrTeardown(
      experimental::DirectPathInitializationMode::kAsynchronousSpeculative);
}
#endif

}  // namespace
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

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
#include "google/cloud/testing_util/opentelemetry_matchers.h"
#include "google/cloud/testing_util/scoped_environment.h"
#include "rpc_retry_policy.h"
#include <gmock/gmock.h>
#include <chrono>

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
using ::testing::ElementsAre;
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

bool HasGrpcStatsPluginArg(Options const& options) {
  if (!options.has<GrpcChannelArgumentsNativeOption>()) return false;
  auto const c_args =
      options.get<GrpcChannelArgumentsNativeOption>().c_channel_args();
  for (std::size_t i = 0; i < c_args.num_args; ++i) {
    if (std::string_view{c_args.args[i].key} ==
        "grpc.internal.stats_plugin_list") {
      return true;
    }
  }
  return false;
}

TEST(MakeDataConnection,
     DirectPathBlockingFallbackPreservesMultiConnectionOptions) {
  testing_util::ScopedEnvironment env("GOOGLE_CLOUD_CPP_TESTING_OTEL_COLLECTOR",
                                      "1");
  InstanceResource instance_1a{Project("project-1"), "instance-1a"};
  InstanceResource instance_1b{Project("project-1b"), "instance-1b"};
  InstanceResource instance_2{Project("project-2"), "instance-2"};

  auto base_options =
      TestOptions()
          .set<EnableMetricsOption>(true)
          .set<experimental::DirectPathModeOption>(
              experimental::DirectPathMode::kEnabled)
          .set<experimental::DirectPathMetricsModeOption>(
              experimental::DirectPathMetricsMode::kEnabled)
          .set<experimental::DirectPathInitializationModeOption>(
              experimental::DirectPathInitializationMode::kBlocking)
          .set<experimental::DirectPathProbeTimeoutOption>(ms(50))
          .set<experimental::DirectPathDiagnosticsTimeoutOption>(ms(50));

  auto conn_1 = MakeDataConnection(
      {instance_1a, instance_1b},
      Options{base_options}.set<AppProfileIdOption>("profile-1"));
  auto conn_2 = MakeDataConnection(
      {instance_2}, Options{base_options}.set<AppProfileIdOption>("profile-2"));
  auto cloudpath_conn = MakeDataConnection(
      {instance_2},
      TestOptions().set<EnableMetricsOption>(true).set<AppProfileIdOption>(
          "profile-cloud"));

  ASSERT_NE(conn_1, nullptr);
  ASSERT_NE(conn_2, nullptr);
  ASSERT_NE(cloudpath_conn, nullptr);

  // With insecure credentials, DirectPath probing fails and falls back to
  // CloudPath without attaching DirectPath stats plugins to CloudPath options.
  EXPECT_EQ(conn_1->options().get<experimental::DirectPathModeOption>(),
            experimental::DirectPathMode::kDisabled);
  EXPECT_EQ(conn_1->options().get<AuthorityOption>(),
            "bigtable.googleapis.com");
  EXPECT_FALSE(HasGrpcStatsPluginArg(conn_1->options()));
  EXPECT_EQ(conn_1->options().get<AppProfileIdOption>(), "profile-1");
  EXPECT_THAT(
      conn_1->options().get<bigtable_internal::InstanceChannelAffinityOption>(),
      ElementsAre(instance_1a, instance_1b));

  EXPECT_EQ(conn_2->options().get<experimental::DirectPathModeOption>(),
            experimental::DirectPathMode::kDisabled);
  EXPECT_FALSE(HasGrpcStatsPluginArg(conn_2->options()));
  EXPECT_EQ(conn_2->options().get<AppProfileIdOption>(), "profile-2");
  EXPECT_THAT(
      conn_2->options().get<bigtable_internal::InstanceChannelAffinityOption>(),
      ElementsAre(instance_2));

  EXPECT_FALSE(HasGrpcStatsPluginArg(cloudpath_conn->options()));
  EXPECT_EQ(cloudpath_conn->options().get<AppProfileIdOption>(),
            "profile-cloud");
}

TEST(MakeDataConnection,
     DirectPathSpeculativeFallbackPreservesMultiConnectionOptions) {
  testing_util::ScopedEnvironment env("GOOGLE_CLOUD_CPP_TESTING_OTEL_COLLECTOR",
                                      "1");
  InstanceResource instance_1{Project("project-1"), "instance-1"};
  InstanceResource instance_2{Project("project-2"), "instance-2"};

  auto base_options =
      TestOptions()
          .set<EnableMetricsOption>(true)
          .set<experimental::DirectPathModeOption>(
              experimental::DirectPathMode::kEnabled)
          .set<experimental::DirectPathMetricsModeOption>(
              experimental::DirectPathMetricsMode::kEnabled)
          .set<experimental::DirectPathInitializationModeOption>(
              experimental::DirectPathInitializationMode::
                  kAsynchronousSpeculative)
          .set<experimental::DirectPathProbeTimeoutOption>(ms(50))
          .set<experimental::DirectPathDiagnosticsTimeoutOption>(ms(50));

  auto conn_1 = MakeDataConnection(
      {instance_1}, Options{base_options}.set<AppProfileIdOption>("profile-1"));
  auto conn_2 = MakeDataConnection(
      {instance_2}, Options{base_options}.set<AppProfileIdOption>("profile-2"));

  ASSERT_NE(conn_1, nullptr);
  ASSERT_NE(conn_2, nullptr);

  EXPECT_EQ(conn_1->options().get<experimental::DirectPathModeOption>(),
            experimental::DirectPathMode::kDisabled);
  EXPECT_FALSE(HasGrpcStatsPluginArg(conn_1->options()));
  EXPECT_EQ(conn_1->options().get<AppProfileIdOption>(), "profile-1");
  EXPECT_THAT(
      conn_1->options().get<bigtable_internal::InstanceChannelAffinityOption>(),
      ElementsAre(instance_1));

  EXPECT_EQ(conn_2->options().get<experimental::DirectPathModeOption>(),
            experimental::DirectPathMode::kDisabled);
  EXPECT_FALSE(HasGrpcStatsPluginArg(conn_2->options()));
  EXPECT_EQ(conn_2->options().get<AppProfileIdOption>(), "profile-2");
  EXPECT_THAT(
      conn_2->options().get<bigtable_internal::InstanceChannelAffinityOption>(),
      ElementsAre(instance_2));
}
#endif

}  // namespace
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

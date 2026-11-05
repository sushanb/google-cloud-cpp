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

#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_GRPC_OTEL_METRICS

#include "google/cloud/bigtable/internal/grpc_metrics_exporter.h"
#include "google/cloud/bigtable/instance_resource.h"
#include "google/cloud/bigtable/internal/data_connection_impl.h"
#include "google/cloud/bigtable/options.h"
#include "google/cloud/bigtable/version.h"
#include "google/cloud/common_options.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/options.h"
#include "google/cloud/project.h"
#include "absl/strings/str_cat.h"
#include "google/bigtable/v2/bigtable.grpc.pb.h"
#include <gmock/gmock.h>
#include <grpcpp/grpcpp.h>
#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/meter_provider.h>
#include <opentelemetry/metrics/sync_instruments.h>
#include <opentelemetry/sdk/metrics/data/metric_data.h>
#include <opentelemetry/sdk/metrics/data/point_data.h>
#include <opentelemetry/sdk/metrics/export/metric_producer.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/metrics/push_metric_exporter.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/semconv/incubating/cloud_attributes.h>
#include <opentelemetry/semconv/incubating/host_attributes.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {

using ::testing::AllOf;
using ::testing::AtLeast;
using ::testing::Contains;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::Ge;
using ::testing::IsEmpty;
using ::testing::Le;
using ::testing::Not;
using ::testing::NotNull;
using ::testing::ResultOf;
using ::testing::Return;
using ::testing::SizeIs;
using ::testing::VariantWith;

class MockPushMetricExporter
    : public opentelemetry::sdk::metrics::PushMetricExporter {
 public:
  // NOLINTBEGIN(bugprone-exception-escape)
  MOCK_METHOD(opentelemetry::sdk::common::ExportResult, Export,
              (opentelemetry::sdk::metrics::ResourceMetrics const&),
              (noexcept, override));

  MOCK_METHOD(opentelemetry::sdk::metrics::AggregationTemporality,
              GetAggregationTemporality,
              (opentelemetry::sdk::metrics::InstrumentType),
              (const, noexcept, override));

  MOCK_METHOD(bool, ForceFlush, (std::chrono::microseconds),
              (noexcept, override));
  MOCK_METHOD(bool, Shutdown, (std::chrono::microseconds),
              (noexcept, override));
  // NOLINTEND(bugprone-exception-escape)
};

auto constexpr kDurationMetric = "grpc.client.attempt.duration";

auto MatchesLatencyBoundaries() {
  return ResultOf(
      "boundaries are latency boundaries",
      [](opentelemetry::sdk::metrics::HistogramPointData const& hpd) {
        return hpd.boundaries_;
      },
      ElementsAreArray(MakeLatencyHistogramBoundaries()));
}

auto ExpectedLatencyHistogram() {
  return ResultOf(
      "data is histogram with right boundaries",
      [](opentelemetry::sdk::metrics::PointDataAttributes const& pda) {
        return pda.point_data;
      },
      VariantWith<opentelemetry::sdk::metrics::HistogramPointData>(
          MatchesLatencyBoundaries()));
}

template <typename Matcher>
auto WithPointData(Matcher&& matcher) {
  return ResultOf(
      "instrument descriptor name",
      [](opentelemetry::sdk::metrics::MetricData const& md) {
        return md.point_data_attr_;
      },
      std::forward<Matcher>(matcher));
}

template <typename Matcher>
auto WithMetricsData(Matcher&& matcher) {
  return ResultOf(
      "metric_data_",
      [](opentelemetry::sdk::metrics::ScopeMetrics const& sm) {
        return sm.metric_data_;
      },
      std::forward<Matcher>(matcher));
}

auto MatchesInstrumentName(std::string name) {
  return ResultOf(
      "instrument descriptor name",
      [](opentelemetry::sdk::metrics::MetricData const& md) {
        return md.instrument_descriptor.name_;
      },
      std::move(name));
}

auto MetricDataEmpty() {
  return ResultOf(
      "scope_metric_data_",
      [](opentelemetry::sdk::metrics::ResourceMetrics const& data) {
        return data.scope_metric_data_;
      },
      IsEmpty());
}

auto MetricDataNotEmpty() {
  return ResultOf(
      "scope_metric_data_",
      [](opentelemetry::sdk::metrics::ResourceMetrics const& data) {
        return data.scope_metric_data_;
      },
      Not(IsEmpty()));
}

auto constexpr kExportInterval = std::chrono::milliseconds(50);
auto constexpr kExportTimeout = std::chrono::milliseconds(25);

auto TestReaderOptions() {
  opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions
      reader_options;
  reader_options.export_interval_millis = kExportInterval;
  reader_options.export_timeout_millis = kExportTimeout;
  return reader_options;
}

class DummyMetricServiceConnection
    : public monitoring_v3::MetricServiceConnection {
 public:
  ~DummyMetricServiceConnection() override = default;
};

class MockMetricServiceConnection
    : public monitoring_v3::MetricServiceConnection {
 public:
  ~MockMetricServiceConnection() override = default;
  MOCK_METHOD(Status, CreateServiceTimeSeries,
              (google::monitoring::v3::CreateTimeSeriesRequest const&),
              (override));
  MOCK_METHOD(Status, CreateTimeSeries,
              (google::monitoring::v3::CreateTimeSeriesRequest const&),
              (override));
};

class TestBigtableService final
    : public google::bigtable::v2::Bigtable::Service {
 public:
  grpc::Status PingAndWarm(
      grpc::ServerContext* /*context*/,
      google::bigtable::v2::PingAndWarmRequest const* /*request*/,
      google::bigtable::v2::PingAndWarmResponse* /*response*/) override {
    return grpc::Status::OK;
  }

  grpc::Status MutateRow(
      grpc::ServerContext* /*context*/,
      google::bigtable::v2::MutateRowRequest const* /*request*/,
      google::bigtable::v2::MutateRowResponse* /*response*/) override {
    return grpc::Status::OK;
  }
};

class LocalBigtableServer {
 public:
  LocalBigtableServer() {
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
    address_ = absl::StrCat("localhost:", port);
  }

  LocalBigtableServer(LocalBigtableServer const&) = delete;
  LocalBigtableServer& operator=(LocalBigtableServer const&) = delete;

  ~LocalBigtableServer() {
    if (server_ != nullptr) {
      server_->Shutdown();
    }
  }

  grpc::Server* server() const { return server_.get(); }
  std::string const& address() const { return address_; }

 private:
  TestBigtableService service_;
  std::unique_ptr<grpc::Server> server_;
  std::string address_;
};

TEST(GrpcMetricsExporterTest,
     MakeMonitoredResourceFromInstanceChannelAffinity) {
  Options options;
  options.set<InstanceChannelAffinityOption>({bigtable::InstanceResource(
      Project("affinity-project"), "affinity-instance")});
  options.set<bigtable::AppProfileIdOption>("affinity-app-profile");
  std::string const client_uid = "affinity-client-uid";

  // Built-in gRPC OpenTelemetry metrics (such as grpc.client.attempt.duration)
  // do not populate "project_id" or "instance" point attributes.
  opentelemetry::sdk::metrics::PointDataAttributes pda;
  pda.attributes = opentelemetry::sdk::metrics::PointAttributes({
      {"grpc.method", "google.bigtable.v2.Bigtable/MutateRow"},
      {"grpc.status", "OK"},
  });

  auto result = MakeMonitoredResource(pda, options, client_uid);
  EXPECT_THAT(result.project_id, Eq("affinity-project"));

  auto const& labels = result.resource.labels();
  EXPECT_THAT(labels.at("project_id"), Eq("affinity-project"));
  EXPECT_THAT(labels.at("client_project"), Eq("affinity-project"));
  EXPECT_THAT(labels.at("instance"), Eq("affinity-instance"));
  EXPECT_THAT(labels.at("app_profile"), Eq("affinity-app-profile"));
  EXPECT_THAT(labels.at("uuid"), Eq("affinity-client-uid"));
}

TEST(GrpcMetricsExporterTest, MakeMonitoredResource) {
  Options options;
  options.set<bigtable::AppProfileIdOption>("test-app-profile");
  std::string const client_uid = "test-client-uid";

  opentelemetry::sdk::metrics::PointDataAttributes pda;
  pda.attributes = opentelemetry::sdk::metrics::PointAttributes({
      {"project_id", "test-project"},
      {"instance", "test-instance"},
  });

  auto result = MakeMonitoredResource(pda, options, client_uid);
  EXPECT_THAT(result.project_id, Eq("test-project"));

  auto const& resource = result.resource;
  EXPECT_THAT(resource.type(), Eq("bigtable_client"));

  auto const& labels = resource.labels();
  EXPECT_THAT(labels.at("project_id"), Eq("test-project"));
  EXPECT_THAT(labels.at("client_project"), Eq("test-project"));
  EXPECT_THAT(labels.at("instance"), Eq("test-instance"));
  EXPECT_THAT(labels.at("app_profile"), Eq("test-app-profile"));
  EXPECT_THAT(labels.at("client_name"),
              Eq("cpp.Bigtable/" + bigtable::version_string()));
  EXPECT_THAT(labels.at("uuid"), Eq("test-client-uid"));
  EXPECT_THAT(labels.at("region"), Eq("global"));
  EXPECT_THAT(labels.at("cloud_platform"), Eq("unknown"));
  EXPECT_THAT(labels.at("host_id"), Eq("unknown"));
}

TEST(GrpcMetricsExporterTest, MakeMonitoredResourceMissingAttributes) {
  Options options;
  std::string const client_uid = "test-client-uid";

  opentelemetry::sdk::metrics::PointDataAttributes pda;
  pda.attributes = opentelemetry::sdk::metrics::PointAttributes({});

  auto result = MakeMonitoredResource(pda, options, client_uid);
  EXPECT_THAT(result.project_id, Eq(""));

  auto const& resource = result.resource;
  EXPECT_THAT(resource.type(), Eq("bigtable_client"));

  auto const& labels = resource.labels();
  EXPECT_THAT(labels.at("project_id"), Eq(""));
  EXPECT_THAT(labels.at("instance"), Eq(""));
  EXPECT_THAT(labels.at("app_profile"), Eq(""));
  EXPECT_THAT(labels.at("client_name"),
              Eq("cpp.Bigtable/" + bigtable::version_string()));
  EXPECT_THAT(labels.at("uuid"), Eq("test-client-uid"));
  EXPECT_THAT(labels.at("region"), Eq("global"));
  EXPECT_THAT(labels.at("cloud_platform"), Eq("unknown"));
  EXPECT_THAT(labels.at("host_id"), Eq("unknown"));
}

TEST(GrpcMetricsExporterTest, MakeMonitoredResourcePartialAttributes) {
  Options options;
  std::string const client_uid = "test-client-uid";

  opentelemetry::sdk::metrics::PointDataAttributes pda;
  pda.attributes = opentelemetry::sdk::metrics::PointAttributes({
      {"project_id", "test-project"},
  });

  auto result = MakeMonitoredResource(pda, options, client_uid);
  EXPECT_THAT(result.project_id, Eq("test-project"));

  auto const& resource = result.resource;
  EXPECT_THAT(resource.type(), Eq("bigtable_client"));

  auto const& labels = resource.labels();
  EXPECT_THAT(labels.at("project_id"), Eq("test-project"));
  EXPECT_THAT(labels.at("client_project"), Eq("test-project"));
  EXPECT_THAT(labels.at("instance"), Eq(""));
  EXPECT_THAT(labels.at("app_profile"), Eq(""));
  EXPECT_THAT(labels.at("client_name"),
              Eq("cpp.Bigtable/" + bigtable::version_string()));
  EXPECT_THAT(labels.at("uuid"), Eq("test-client-uid"));
  EXPECT_THAT(labels.at("region"), Eq("global"));
  EXPECT_THAT(labels.at("cloud_platform"), Eq("unknown"));
  EXPECT_THAT(labels.at("host_id"), Eq("unknown"));
}

TEST(GrpcMetricsExporterTest, MakeMonitoredResourceExtraAttributes) {
  Options options;
  std::string const client_uid = "test-client-uid";

  opentelemetry::sdk::metrics::PointDataAttributes pda;
  pda.attributes = opentelemetry::sdk::metrics::PointAttributes({
      {"project_id", "test-project"},
      {"instance", "test-instance"},
      {"grpc.method", "google.bigtable.v2.Bigtable/ReadRows"},
      {"grpc.status", "OK"},
  });

  auto result = MakeMonitoredResource(pda, options, client_uid);
  EXPECT_THAT(result.project_id, Eq("test-project"));

  auto const& resource = result.resource;
  EXPECT_THAT(resource.type(), Eq("bigtable_client"));

  auto const& labels = resource.labels();
  EXPECT_THAT(labels.size(), Eq(9));
  EXPECT_THAT(labels.at("project_id"), Eq("test-project"));
  EXPECT_THAT(labels.at("client_project"), Eq("test-project"));
  EXPECT_THAT(labels.at("instance"), Eq("test-instance"));
  EXPECT_THAT(labels.at("app_profile"), Eq(""));
  EXPECT_THAT(labels.at("client_name"),
              Eq("cpp.Bigtable/" + bigtable::version_string()));
  EXPECT_THAT(labels.at("uuid"), Eq("test-client-uid"));
  EXPECT_THAT(labels.at("region"), Eq("global"));
  EXPECT_THAT(labels.at("cloud_platform"), Eq("unknown"));
  EXPECT_THAT(labels.at("host_id"), Eq("unknown"));
  EXPECT_THAT(labels.find("grpc.method"), Eq(labels.end()));
  EXPECT_THAT(labels.find("grpc.status"), Eq(labels.end()));
}

TEST(GrpcMetricsExporterTest, MakeMonitoredResourceWithDetectedResource) {
  namespace sc = ::opentelemetry::semconv;
  Options options;
  options.set<bigtable::AppProfileIdOption>("test-app-profile");
  std::string const client_uid = "test-client-uid";

  opentelemetry::sdk::metrics::PointDataAttributes pda;
  pda.attributes = opentelemetry::sdk::metrics::PointAttributes({
      {"project_id", "test-project"},
      {"instance", "test-instance"},
  });

  auto detected_resource = opentelemetry::sdk::resource::Resource::Create({
      {sc::cloud::kCloudAccountId, "detected-vm-project"},
      {sc::cloud::kCloudPlatform, "gcp_compute_engine"},
      {sc::cloud::kCloudAvailabilityZone, "us-central1-a"},
      {sc::host::kHostId, "123456789"},
      {sc::host::kHostName, "test-vm-host"},
  });

  auto result =
      MakeMonitoredResource(pda, detected_resource, options, client_uid);
  EXPECT_THAT(result.project_id, Eq("test-project"));

  auto const& resource = result.resource;
  EXPECT_THAT(resource.type(), Eq("bigtable_client"));

  auto const& labels = resource.labels();
  EXPECT_THAT(labels.at("project_id"), Eq("test-project"));
  EXPECT_THAT(labels.at("client_project"), Eq("detected-vm-project"));
  EXPECT_THAT(labels.at("instance"), Eq("test-instance"));
  EXPECT_THAT(labels.at("app_profile"), Eq("test-app-profile"));
  EXPECT_THAT(labels.at("client_name"),
              Eq("cpp.Bigtable/" + bigtable::version_string()));
  EXPECT_THAT(labels.at("uuid"), Eq("test-client-uid"));
  EXPECT_THAT(labels.at("region"), Eq("us-central1"));
  EXPECT_THAT(labels.at("cloud_platform"), Eq("gcp_compute_engine"));
  EXPECT_THAT(labels.at("host_id"), Eq("123456789"));
  EXPECT_THAT(labels.at("host_name"), Eq("test-vm-host"));
}

TEST(GrpcMetricsExporterTest, MakeLatencyHistogramBoundaries) {
  auto const& boundaries = MakeLatencyHistogramBoundaries();
  ASSERT_THAT(boundaries, Not(IsEmpty()));
  ASSERT_THAT(boundaries, SizeIs(Le(200U)));
  auto sorted = boundaries;
  std::sort(sorted.begin(), sorted.end());
  EXPECT_THAT(boundaries, ElementsAreArray(sorted.begin(), sorted.end()));
  std::vector<double> diff;
  std::adjacent_difference(boundaries.begin(), boundaries.end(),
                           std::back_inserter(diff));
  ASSERT_THAT(diff, Not(IsEmpty()));
  EXPECT_THAT(*std::min_element(std::next(diff.begin()), diff.end()),
              Ge(0.001));
  ASSERT_THAT(boundaries.back(), Le(300));
}

TEST(GrpcMetricsExporterTest, ValidateGrpcClientAttemptDuration) {
  std::atomic<int> export_count{0};
  auto mock = std::make_unique<MockPushMetricExporter>();
  EXPECT_CALL(*mock, Shutdown).WillOnce(Return(true));
  EXPECT_CALL(*mock, GetAggregationTemporality)
      .WillRepeatedly(Return(
          opentelemetry::sdk::metrics::AggregationTemporality::kCumulative));
  EXPECT_CALL(*mock, Export(MetricDataEmpty()))
      .WillRepeatedly(
          Return(opentelemetry::sdk::common::ExportResult::kSuccess));
  EXPECT_CALL(*mock, Export(MetricDataNotEmpty()))
      .Times(AtLeast(1))
      .WillRepeatedly(
          [&export_count](
              opentelemetry::sdk::metrics::ResourceMetrics const& data) {
            EXPECT_THAT(
                data.scope_metric_data_,
                Contains(WithMetricsData(Contains(AllOf(
                    MatchesInstrumentName(kDurationMetric),
                    WithPointData(ElementsAre(ExpectedLatencyHistogram())))))));
            ++export_count;
            return opentelemetry::sdk::common::ExportResult::kSuccess;
          });

  {
    auto provider = MakeGrpcMeterProvider(std::move(mock), TestReaderOptions());
    auto meter = provider->GetMeter("grpc-c++", grpc::Version());
    auto histogram = meter->CreateDoubleHistogram(kDurationMetric,
                                                  "test-only-description", "s");
    for (int i = 0; i != 50 && export_count.load() == 0; ++i) {
      histogram->Record(1.0, opentelemetry::context::Context{});
      std::this_thread::sleep_for(kExportInterval);
    }
  }
}

TEST(GrpcMetricsExporterTest, ValidatePluginConfigArguments) {
  Options options;
  options.set<AuthorityOption>("custom-bigtable-authority.googleapis.com");
  options.set<bigtable::AppProfileIdOption>("test-app-profile");
  options.set<bigtable::MetricsPeriodOption>(std::chrono::seconds(60));
  auto conn = std::make_shared<DummyMetricServiceConnection>();
  std::string const client_uid = "test-client-uid";

  auto const detected_resource =
      opentelemetry::sdk::resource::Resource::Create({});
  auto config =
      MakeGrpcMetricsPluginConfig(detected_resource, conn, options, client_uid);

  EXPECT_THAT(config.meter_provider, NotNull());

  EXPECT_THAT(
      config.enabled_metrics,
      ElementsAre("grpc.client.attempt.duration",
                  "grpc.lb.rls.default_target_picks",
                  "grpc.lb.rls.target_picks", "grpc.lb.rls.failed_picks",
                  "grpc.xds_client.server_failure",
                  "grpc.xds_client.resource_updates_invalid",
                  "grpc.subchannel.disconnections",
                  "grpc.subchannel.connection_attempts_succeeded",
                  "grpc.subchannel.connection_attempts_failed",
                  "grpc.subchannel.open_connections"));

  EXPECT_THAT(
      config.disabled_metrics,
      ElementsAre("grpc.client.attempt.started",
                  "grpc.client.attempt.sent_total_compressed_message_size",
                  "grpc.client.attempt.rcvd_total_compressed_message_size"));

  ASSERT_TRUE(config.generic_method_filter);
  EXPECT_TRUE(
      config.generic_method_filter("google.bigtable.v2.Bigtable/ReadRows"));
  EXPECT_TRUE(
      config.generic_method_filter("google.bigtable.v2.Bigtable/MutateRow"));
  EXPECT_FALSE(
      config.generic_method_filter("google.storage.v2.Storage/ReadObject"));
  EXPECT_FALSE(
      config.generic_method_filter("google.spanner.v1.Spanner/ExecuteSql"));
  EXPECT_FALSE(config.generic_method_filter(""));
}

TEST(GrpcMetricsExporterTest,
     WithGrpcMetricsPluginPreservesExistingChannelArguments) {
  grpc::ChannelArguments native_args;
  native_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 12345);

  Options options;
  options.set<GrpcChannelArgumentsNativeOption>(native_args);
  options.set<GrpcChannelArgumentsOption>(
      {{"custom.string.arg", "custom-val"}});
  options.set<bigtable::MetricsPeriodOption>(std::chrono::seconds(60));

  auto const detected_resource =
      opentelemetry::sdk::resource::Resource::Create({});
  auto conn = std::make_shared<DummyMetricServiceConnection>();
  auto plugin =
      MakeGrpcMetricsPlugin(detected_resource, conn, options, "test-uid");
  ASSERT_THAT(plugin, NotNull());

  auto with_null = WithGrpcMetricsPlugin(options, nullptr);
  auto null_args = internal::MakeChannelArguments(with_null);
  EXPECT_THAT(
      internal::GetIntChannelArgument(null_args, GRPC_ARG_KEEPALIVE_TIME_MS),
      Eq(12345));

  auto instrumented_options = WithGrpcMetricsPlugin(std::move(options), plugin);
  auto args = internal::MakeChannelArguments(instrumented_options);
  EXPECT_THAT(internal::GetIntChannelArgument(args, GRPC_ARG_KEEPALIVE_TIME_MS),
              Eq(12345));
  EXPECT_THAT(internal::GetStringChannelArgument(args, "custom.string.arg"),
              Eq("custom-val"));
}

TEST(GrpcMetricsExporterTest,
     PerChannelPluginIsolatesInstrumentedAndUninstrumentedChannels) {
  LocalBigtableServer server;
  ASSERT_THAT(server.server(), NotNull());

  std::atomic<int> export_count{0};
  auto mock = std::make_unique<MockPushMetricExporter>();
  EXPECT_CALL(*mock, Shutdown).WillRepeatedly(Return(true));
  EXPECT_CALL(*mock, GetAggregationTemporality)
      .WillRepeatedly(Return(
          opentelemetry::sdk::metrics::AggregationTemporality::kCumulative));
  EXPECT_CALL(*mock, Export)
      .WillRepeatedly(
          [&export_count](
              opentelemetry::sdk::metrics::ResourceMetrics const& data) {
            for (auto const& scope_metrics : data.scope_metric_data_) {
              for (auto const& metric_data : scope_metrics.metric_data_) {
                if (metric_data.instrument_descriptor.name_ ==
                        kDurationMetric &&
                    !metric_data.point_data_attr_.empty()) {
                  EXPECT_THAT(metric_data.point_data_attr_,
                              Contains(ExpectedLatencyHistogram()));
                  ++export_count;
                }
              }
            }
            return opentelemetry::sdk::common::ExportResult::kSuccess;
          });

  auto provider = MakeGrpcMeterProvider(std::move(mock), TestReaderOptions());
  {
    GrpcMetricsPluginConfig config{
        provider,
        {kDurationMetric},
        {"grpc.client.attempt.started",
         "grpc.client.attempt.sent_total_compressed_message_size",
         "grpc.client.attempt.rcvd_total_compressed_message_size"},
        [](std::string_view target) {
          return target == "google.bigtable.v2.Bigtable/MutateRow";
        },
    };
    auto plugin = MakeGrpcMetricsPlugin(std::move(config));
    ASSERT_THAT(plugin, NotNull());

    // Both CloudPath and DirectPath channels share default authority
    // "bigtable.googleapis.com". Uninstrumented CloudPath channels must NOT
    // emit DirectPath gRPC metrics.
    Options cloudpath_options;
    cloudpath_options.set<AuthorityOption>("bigtable.googleapis.com");
    auto cloudpath_channel = grpc::CreateCustomChannel(
        server.address(), grpc::InsecureChannelCredentials(),
        internal::MakeChannelArguments(cloudpath_options));
    auto cloudpath_stub =
        google::bigtable::v2::Bigtable::NewStub(cloudpath_channel);
    for (int i = 0; i < 5; ++i) {
      grpc::ClientContext ctx;
      google::bigtable::v2::MutateRowRequest req;
      google::bigtable::v2::MutateRowResponse resp;
      ASSERT_TRUE(cloudpath_stub->MutateRow(&ctx, req, &resp).ok());
    }
    std::this_thread::sleep_for(kExportInterval * 4);
    EXPECT_THAT(export_count.load(), Eq(0));

    // Only the channel with the per-channel plugin attached records metrics.
    auto directpath_options = WithGrpcMetricsPlugin(cloudpath_options, plugin);
    auto directpath_channel = grpc::CreateCustomChannel(
        server.address(), grpc::InsecureChannelCredentials(),
        internal::MakeChannelArguments(directpath_options));
    auto directpath_stub =
        google::bigtable::v2::Bigtable::NewStub(directpath_channel);
    for (int i = 0; i < 50 && export_count.load() == 0; ++i) {
      grpc::ClientContext ctx;
      google::bigtable::v2::MutateRowRequest req;
      google::bigtable::v2::MutateRowResponse resp;
      ASSERT_TRUE(directpath_stub->MutateRow(&ctx, req, &resp).ok());
      std::this_thread::sleep_for(kExportInterval);
    }
    EXPECT_THAT(export_count.load(), Ge(1));
  }
  server.server()->Shutdown();
  static_cast<opentelemetry::sdk::metrics::MeterProvider&>(*provider)
      .ForceFlush();
}

TEST(GrpcMetricsExporterTest,
     EnableGrpcMetricsForInstanceIsolatesMultiInstanceAndMultiConnection) {
  LocalBigtableServer server;
  ASSERT_THAT(server.server(), NotNull());

  auto const detected_resource = opentelemetry::sdk::resource::Resource::Create(
      {{opentelemetry::semconv::cloud::kCloudAccountId, "vm-project"}});

  auto conn_1 = std::make_shared<MockMetricServiceConnection>();
  auto conn_2 = std::make_shared<MockMetricServiceConnection>();

  std::mutex mu;
  std::vector<google::monitoring::v3::CreateTimeSeriesRequest> conn_1_requests;
  std::vector<google::monitoring::v3::CreateTimeSeriesRequest> conn_2_requests;

  EXPECT_CALL(*conn_1, CreateServiceTimeSeries)
      .WillRepeatedly(
          [&](google::monitoring::v3::CreateTimeSeriesRequest const& req) {
            std::lock_guard<std::mutex> lk(mu);
            conn_1_requests.push_back(req);
            return Status{};
          });
  EXPECT_CALL(*conn_2, CreateServiceTimeSeries)
      .WillRepeatedly(
          [&](google::monitoring::v3::CreateTimeSeriesRequest const& req) {
            std::lock_guard<std::mutex> lk(mu);
            conn_2_requests.push_back(req);
            return Status{};
          });

  auto has_instance_metric =
      [](std::vector<google::monitoring::v3::CreateTimeSeriesRequest> const&
             reqs,
         std::string const& expected_project,
         std::string const& expected_instance,
         std::string const& expected_profile, std::string const& expected_uid) {
        for (auto const& req : reqs) {
          if (req.name() != absl::StrCat("projects/", expected_project)) {
            continue;
          }
          for (auto const& ts : req.time_series()) {
            auto const& labels = ts.resource().labels();
            auto const proj_it = labels.find("project_id");
            auto const inst_it = labels.find("instance");
            auto const prof_it = labels.find("app_profile");
            auto const uid_it = labels.find("uuid");
            if (ts.metric().type() ==
                    "bigtable.googleapis.com/internal/client/grpc/client/"
                    "attempt/duration" &&
                ts.resource().type() == "bigtable_client" &&
                proj_it != labels.end() &&
                proj_it->second == expected_project &&
                inst_it != labels.end() &&
                inst_it->second == expected_instance &&
                prof_it != labels.end() &&
                prof_it->second == expected_profile && uid_it != labels.end() &&
                uid_it->second == expected_uid) {
              return true;
            }
          }
        }
        return false;
      };

  {
    // Connection 1 has two instances across two projects;
    // InstanceChannelAffinity starts with both so we verify instances[1] does
    // not latch instances[0].
    Options base_opts_1;
    base_opts_1.set<AuthorityOption>("bigtable.googleapis.com");
    base_opts_1.set<bigtable::AppProfileIdOption>("profile-1");
    base_opts_1.set<bigtable::MetricsPeriodOption>(std::chrono::seconds(1));
    base_opts_1.set<InstanceChannelAffinityOption>(
        {bigtable::InstanceResource(Project("proj-1a"), "inst-1a"),
         bigtable::InstanceResource(Project("proj-1b"), "inst-1b")});

    auto opts_1a = EnableGrpcMetricsForInstance(
        detected_resource, conn_1, base_opts_1,
        "projects/proj-1a/instances/inst-1a", "uid-1");
    auto opts_1b = EnableGrpcMetricsForInstance(
        detected_resource, conn_1, base_opts_1,
        "projects/proj-1b/instances/inst-1b", "uid-1");

    // Connection 2 uses the same authority "bigtable.googleapis.com" with a
    // distinct project, instance, app profile, client UID, and Monitoring
    // connection.
    Options base_opts_2;
    base_opts_2.set<AuthorityOption>("bigtable.googleapis.com");
    base_opts_2.set<bigtable::AppProfileIdOption>("profile-2");
    base_opts_2.set<bigtable::MetricsPeriodOption>(std::chrono::seconds(1));
    base_opts_2.set<InstanceChannelAffinityOption>(
        {bigtable::InstanceResource(Project("proj-2"), "inst-2")});

    auto opts_2 = EnableGrpcMetricsForInstance(
        detected_resource, conn_2, base_opts_2,
        "projects/proj-2/instances/inst-2", "uid-2");

    auto stub_1a =
        google::bigtable::v2::Bigtable::NewStub(grpc::CreateCustomChannel(
            server.address(), grpc::InsecureChannelCredentials(),
            internal::MakeChannelArguments(opts_1a)));
    auto stub_1b =
        google::bigtable::v2::Bigtable::NewStub(grpc::CreateCustomChannel(
            server.address(), grpc::InsecureChannelCredentials(),
            internal::MakeChannelArguments(opts_1b)));
    auto stub_2 =
        google::bigtable::v2::Bigtable::NewStub(grpc::CreateCustomChannel(
            server.address(), grpc::InsecureChannelCredentials(),
            internal::MakeChannelArguments(opts_2)));

    for (auto* stub : {stub_1a.get(), stub_1b.get(), stub_2.get()}) {
      grpc::ClientContext ctx;
      google::bigtable::v2::MutateRowRequest req;
      google::bigtable::v2::MutateRowResponse resp;
      ASSERT_TRUE(stub->MutateRow(&ctx, req, &resp).ok());
    }

    bool all_received = false;
    for (int i = 0; i < 60 && !all_received; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      std::lock_guard<std::mutex> lk(mu);
      all_received = has_instance_metric(conn_1_requests, "proj-1a", "inst-1a",
                                         "profile-1", "uid-1") &&
                     has_instance_metric(conn_1_requests, "proj-1b", "inst-1b",
                                         "profile-1", "uid-1") &&
                     has_instance_metric(conn_2_requests, "proj-2", "inst-2",
                                         "profile-2", "uid-2");
    }
    EXPECT_TRUE(all_received);
  }

  std::lock_guard<std::mutex> lk(mu);
  // Verify strict credential/connection isolation: conn_1 never receives
  // proj-2 metrics, and conn_2 never receives proj-1a/proj-1b metrics.
  for (auto const& req : conn_1_requests) {
    EXPECT_THAT(req.name(), Not(Eq("projects/proj-2")));
  }
  for (auto const& req : conn_2_requests) {
    EXPECT_THAT(req.name(), Eq("projects/proj-2"));
  }
}

TEST(GrpcMetricsExporterTest, EnableGrpcMetricsForInstanceRespectsExportGate) {
  LocalBigtableServer server;
  ASSERT_THAT(server.server(), NotNull());

  auto const detected_resource =
      opentelemetry::sdk::resource::Resource::Create({});
  auto conn = std::make_shared<MockMetricServiceConnection>();
  std::atomic<int> export_calls{0};
  EXPECT_CALL(*conn, CreateServiceTimeSeries)
      .WillRepeatedly(
          [&](google::monitoring::v3::CreateTimeSeriesRequest const&) {
            ++export_calls;
            return Status{};
          });

  auto export_enabled = std::make_shared<std::atomic<bool>>(false);
  {
    Options base_opts;
    base_opts.set<AuthorityOption>("bigtable.googleapis.com");
    base_opts.set<bigtable::MetricsPeriodOption>(std::chrono::seconds(1));

    auto opts = EnableGrpcMetricsForInstance(detected_resource, conn, base_opts,
                                             "projects/proj-1/instances/inst-1",
                                             "uid-gated", export_enabled);
    auto stub =
        google::bigtable::v2::Bigtable::NewStub(grpc::CreateCustomChannel(
            server.address(), grpc::InsecureChannelCredentials(),
            internal::MakeChannelArguments(opts)));

    grpc::ClientContext ctx;
    google::bigtable::v2::PingAndWarmRequest req;
    google::bigtable::v2::PingAndWarmResponse resp;
    ASSERT_TRUE(stub->PingAndWarm(&ctx, req, &resp).ok());

    // Wait past a full 1-second export period while gated off.
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    EXPECT_THAT(export_calls.load(), Eq(0));

    // Enable exports (simulating DirectPath probe success) and record an RPC.
    export_enabled->store(true, std::memory_order_release);
    grpc::ClientContext ctx2;
    ASSERT_TRUE(stub->PingAndWarm(&ctx2, req, &resp).ok());

    for (int i = 0; i < 40 && export_calls.load() == 0; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_THAT(export_calls.load(), Ge(1));
  }
}

}  // namespace
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_BIGTABLE_WITH_GRPC_OTEL_METRICS

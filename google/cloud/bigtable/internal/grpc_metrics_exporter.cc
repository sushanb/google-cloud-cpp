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

#include "google/cloud/bigtable/internal/grpc_metrics_exporter.h"
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_GRPC_OTEL_METRICS
#include "google/cloud/bigtable/instance_resource.h"
#include "google/cloud/bigtable/internal/data_connection_impl.h"
#include "google/cloud/bigtable/internal/defaults.h"
#include "google/cloud/bigtable/options.h"
#include "google/cloud/bigtable/version.h"
#include "google/cloud/opentelemetry/internal/monitoring_exporter.h"
#include "google/cloud/opentelemetry/resource_detector.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/internal/algorithm.h"
#include "google/cloud/log.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include <grpcpp/ext/otel_plugin.h>
#include <grpcpp/grpcpp.h>
#include <opentelemetry/semconv/incubating/cloud_attributes.h>
#include <opentelemetry/semconv/incubating/faas_attributes.h>
#include <opentelemetry/semconv/incubating/host_attributes.h>
#include <opentelemetry/version.h>
#if OPENTELEMETRY_VERSION_MAJOR > 1 || \
    (OPENTELEMETRY_VERSION_MAJOR == 1 && OPENTELEMETRY_VERSION_MINOR >= 10)
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/meter_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/view_factory.h>
#endif  // OPENTELEMETRY_VERSION_MAJOR > 1 || (OPENTELEMETRY_VERSION_MAJOR == 1
        // && OPENTELEMETRY_VERSION_MINOR >= 10)
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector.h>
#include <opentelemetry/sdk/metrics/view/meter_selector.h>
#include <opentelemetry/sdk/metrics/view/view.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#endif  // GOOGLE_CLOUD_CPP_BIGTABLE_WITH_GRPC_OTEL_METRICS

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN

#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_GRPC_OTEL_METRICS

std::vector<double> const& MakeLatencyHistogramBoundaries() {
  static auto const kBoundaries = [] {
    using dseconds = std::chrono::duration<double, std::ratio<1>>;
    std::vector<double> boundaries;
    auto boundary = std::chrono::milliseconds(0);
    auto increment = std::chrono::milliseconds(2);
    for (int i = 0; i != 50; ++i) {
      boundaries.push_back(
          std::chrono::duration_cast<dseconds>(boundary).count());
      boundary += increment;
    }
    increment = std::chrono::milliseconds(10);
    for (int i = 0; i != 150 && boundary <= std::chrono::minutes(5); ++i) {
      boundaries.push_back(
          std::chrono::duration_cast<dseconds>(boundary).count());
      if (i != 0 && i % 10 == 0) increment *= 2;
      boundary += increment;
    }
    return boundaries;
  }();
  return kBoundaries;
}

namespace {

void AddHistogramView(opentelemetry::sdk::metrics::MeterProvider& provider,
                      std::vector<double> boundaries, std::string const& name,
                      std::string const& unit) {
  auto constexpr kGrpcMeterName = "grpc-c++";
  auto constexpr kGrpcSchema = "";

  auto histogram_aggregation_config = std::make_unique<
      opentelemetry::sdk::metrics::HistogramAggregationConfig>();
  histogram_aggregation_config->boundaries_ = std::move(boundaries);
  auto aggregation_config =
      std::shared_ptr<opentelemetry::sdk::metrics::AggregationConfig>(
          std::move(histogram_aggregation_config));

  auto description = absl::StrCat("A view of ", name,
                                  " with histogram boundaries more appropriate "
                                  "for Google Cloud Bigtable RPCs");

#if OPENTELEMETRY_VERSION_MAJOR > 1 || \
    (OPENTELEMETRY_VERSION_MAJOR == 1 && OPENTELEMETRY_VERSION_MINOR >= 23)
  (void)unit;
  provider.AddView(
      opentelemetry::sdk::metrics::InstrumentSelectorFactory::Create(
          opentelemetry::sdk::metrics::InstrumentType::kHistogram, name, unit),
      opentelemetry::sdk::metrics::MeterSelectorFactory::Create(
          kGrpcMeterName, grpc::Version(), kGrpcSchema),
      opentelemetry::sdk::metrics::ViewFactory::Create(
          name, std::move(description),
          opentelemetry::sdk::metrics::AggregationType::kHistogram,
          std::move(aggregation_config)));
#elif OPENTELEMETRY_VERSION_MAJOR > 1 || \
    (OPENTELEMETRY_VERSION_MAJOR == 1 && OPENTELEMETRY_VERSION_MINOR >= 10)
  provider.AddView(
      opentelemetry::sdk::metrics::InstrumentSelectorFactory::Create(
          opentelemetry::sdk::metrics::InstrumentType::kHistogram, name, unit),
      opentelemetry::sdk::metrics::MeterSelectorFactory::Create(
          kGrpcMeterName, grpc::Version(), kGrpcSchema),
      opentelemetry::sdk::metrics::ViewFactory::Create(
          name, std::move(description), unit,
          opentelemetry::sdk::metrics::AggregationType::kHistogram,
          std::move(aggregation_config)));
#else   // OPENTELEMETRY_VERSION_MAJOR > 1 || (OPENTELEMETRY_VERSION_MAJOR == 1
        // && OPENTELEMETRY_VERSION_MINOR >= 10)
  (void)unit;
  provider.AddView(
      std::make_unique<opentelemetry::sdk::metrics::InstrumentSelector>(
          opentelemetry::sdk::metrics::InstrumentType::kHistogram, name),
      std::make_unique<opentelemetry::sdk::metrics::MeterSelector>(
          kGrpcMeterName, grpc::Version(), kGrpcSchema),
      std::make_unique<opentelemetry::sdk::metrics::View>(
          name, std::move(description),
          opentelemetry::sdk::metrics::AggregationType::kHistogram,
          std::move(aggregation_config)));
#endif  // OPENTELEMETRY_VERSION_MAJOR > 1 || (OPENTELEMETRY_VERSION_MAJOR == 1
        // && OPENTELEMETRY_VERSION_MINOR >= 23)
}

class GatedPushMetricExporter final
    : public opentelemetry::sdk::metrics::PushMetricExporter {
 public:
  GatedPushMetricExporter(
      std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> exporter,
      std::shared_ptr<std::atomic<bool>> export_enabled)
      : exporter_(std::move(exporter)),
        export_enabled_(std::move(export_enabled)) {}

  opentelemetry::sdk::metrics::AggregationTemporality GetAggregationTemporality(
      opentelemetry::sdk::metrics::InstrumentType instrument_type)
      const noexcept override {
    return exporter_->GetAggregationTemporality(instrument_type);
  }

  opentelemetry::sdk::common::ExportResult Export(
      opentelemetry::sdk::metrics::ResourceMetrics const& data) noexcept
      override {
    if (export_enabled_ != nullptr &&
        !export_enabled_->load(std::memory_order_acquire)) {
      return opentelemetry::sdk::common::ExportResult::kSuccess;
    }
    return exporter_->Export(data);
  }

  bool ForceFlush(std::chrono::microseconds timeout) noexcept override {
    if (export_enabled_ != nullptr &&
        !export_enabled_->load(std::memory_order_acquire)) {
      return true;
    }
    return exporter_->ForceFlush(timeout);
  }

  bool Shutdown(std::chrono::microseconds timeout) noexcept override {
    return exporter_->Shutdown(timeout);
  }

 private:
  std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> exporter_;
  std::shared_ptr<std::atomic<bool>> export_enabled_;
};

}  // namespace

std::shared_ptr<opentelemetry::metrics::MeterProvider> MakeGrpcMeterProvider(
    std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> exporter,
    opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions
        reader_options) {
#if OPENTELEMETRY_VERSION_MAJOR > 1 || \
    (OPENTELEMETRY_VERSION_MAJOR == 1 && OPENTELEMETRY_VERSION_MINOR >= 10)
  auto provider = opentelemetry::sdk::metrics::MeterProviderFactory::Create(
      std::make_unique<opentelemetry::sdk::metrics::ViewRegistry>(),
      opentelemetry::sdk::resource::Resource::Create({}));
#else   // OPENTELEMETRY_VERSION_MAJOR > 1 || (OPENTELEMETRY_VERSION_MAJOR == 1
        // && OPENTELEMETRY_VERSION_MINOR >= 10)
  auto provider = std::make_unique<opentelemetry::sdk::metrics::MeterProvider>(
      std::make_unique<opentelemetry::sdk::metrics::ViewRegistry>(),
      opentelemetry::sdk::resource::Resource::Create({}));
#endif  // OPENTELEMETRY_VERSION_MAJOR > 1 || (OPENTELEMETRY_VERSION_MAJOR == 1
        // && OPENTELEMETRY_VERSION_MINOR >= 10)
  auto* p =
      static_cast<opentelemetry::sdk::metrics::MeterProvider*>(provider.get());
  AddHistogramView(*p, MakeLatencyHistogramBoundaries(),
                   "grpc.client.attempt.duration", "s");

#if OPENTELEMETRY_VERSION_MAJOR > 1 || OPENTELEMETRY_VERSION_MINOR >= 10
  p->AddMetricReader(
      opentelemetry::sdk::metrics::PeriodicExportingMetricReaderFactory::Create(
          std::move(exporter), std::move(reader_options)));
#else   // OPENTELEMETRY_VERSION_MAJOR > 1 || OPENTELEMETRY_VERSION_MINOR >= 10
  p->AddMetricReader(
      std::make_unique<
          opentelemetry::sdk::metrics::PeriodicExportingMetricReader>(
          std::move(exporter), std::move(reader_options)));
#endif  // OPENTELEMETRY_VERSION_MAJOR > 1 || OPENTELEMETRY_VERSION_MINOR >= 10

  return std::shared_ptr<opentelemetry::metrics::MeterProvider>(
      std::move(provider));
}

MonitoredResourceResult MakeMonitoredResource(
    opentelemetry::sdk::metrics::PointDataAttributes const& pda,
    opentelemetry::sdk::resource::Resource const& detected_resource,
    Options const& options, std::string const& client_uid) {
  namespace sc = ::opentelemetry::semconv;
  google::api::MonitoredResource resource;
  resource.set_type("bigtable_client");
  auto& labels = *resource.mutable_labels();
  auto const& attributes = pda.attributes.GetAttributes();
  auto get_attr = [&](std::string const& key) {
    auto it = attributes.find(key);
    if (it == attributes.end() ||
        !opentelemetry::nostd::holds_alternative<std::string>(it->second)) {
      return std::string{};
    }
    return opentelemetry::nostd::get<std::string>(it->second);
  };
  auto const& detected_attributes = detected_resource.GetAttributes();
  auto by_name = [&](std::string const& name, std::string default_value = {}) {
    auto const l = detected_attributes.find(name);
    if (l == detected_attributes.end() ||
        !opentelemetry::nostd::holds_alternative<std::string>(l->second)) {
      return default_value;
    }
    return opentelemetry::nostd::get<std::string>(l->second);
  };

  auto project_id = get_attr("project_id");
  if (project_id.empty() &&
      options.has<bigtable_internal::InstanceChannelAffinityOption>()) {
    auto const& instances =
        options.get<bigtable_internal::InstanceChannelAffinityOption>();
    if (!instances.empty()) {
      project_id = instances[0].project_id();
    }
  }
  if (project_id.empty()) {
    project_id = by_name(sc::cloud::kCloudAccountId);
  }

  auto instance = get_attr("instance");
  if (instance.empty() &&
      options.has<bigtable_internal::InstanceChannelAffinityOption>()) {
    auto const& instances =
        options.get<bigtable_internal::InstanceChannelAffinityOption>();
    if (!instances.empty()) {
      instance = instances[0].instance_id();
    }
  }

  labels["project_id"] = project_id;
  labels["instance"] = std::move(instance);
  labels["app_profile"] = options.get<bigtable::AppProfileIdOption>();
  labels["client_name"] = "cpp.Bigtable/" + bigtable::version_string();
  labels["uuid"] = client_uid;

  auto client_project = by_name(sc::cloud::kCloudAccountId);
  if (client_project.empty()) {
    client_project = project_id;
  }
  if (!client_project.empty()) {
    labels["client_project"] = client_project;
  }

  auto region = by_name(sc::cloud::kCloudRegion);
  if (region.empty()) {
    auto zone = by_name(sc::cloud::kCloudAvailabilityZone);
    std::vector<std::string_view> parts = absl::StrSplit(zone, '-');
    if (parts.size() >= 3 && parts.back().size() == 1) {
      parts.pop_back();
      region = absl::StrJoin(parts, "-");
    } else {
      region = std::move(zone);
    }
  }
  if (region.empty()) {
    region = "global";
  }
  labels["region"] = std::move(region);
  labels["cloud_platform"] = by_name(sc::cloud::kCloudPlatform, "unknown");
  labels["host_id"] = by_name("faas.id", by_name(sc::host::kHostId, "unknown"));
  auto hostname = by_name(sc::host::kHostName);
  if (!hostname.empty()) {
    labels["host_name"] = hostname;
  }

  return MonitoredResourceResult{std::move(project_id), std::move(resource)};
}

GrpcMetricsPluginConfig MakeGrpcMetricsPluginConfig(
    opentelemetry::sdk::resource::Resource const& detected_resource,
    std::shared_ptr<monitoring_v3::MetricServiceConnection> const& conn,
    Options const& options, std::string const& client_uid,
    std::shared_ptr<std::atomic<bool>> export_enabled) {
  auto resource_options = options;
  resource_options.unset<GrpcChannelArgumentsNativeOption>();
  auto dynamic_resource_fn =
      [options = std::move(resource_options), client_uid, detected_resource](
          opentelemetry::sdk::metrics::PointDataAttributes const& pda) {
        auto res =
            MakeMonitoredResource(pda, detected_resource, options, client_uid);
        return std::make_pair(std::move(res.project_id),
                              std::move(res.resource));
      };

  std::set<std::string> excluded_labels{
      "project_id",
      "instance",
      "app_profile",
      "client_project",
      "cloud_platform",
      "region",
      "client_region",
      "host_id",
      "host_name",
      "client_name",
      "uuid",
      "service_name",
      "service_namespace",
      "service_instance_id",
      "service.name",
      "service.namespace",
      "service.instance.id",
  };
  auto resource_filter_fn =
      [excluded_labels = std::move(excluded_labels)](
          std::string const& key,
          opentelemetry::sdk::metrics::PointAttributes const&) {
        return internal::Contains(excluded_labels, key);
      };

  auto constexpr kBigtableMetricNamePath =
      "bigtable.googleapis.com/internal/client/";
  auto exporter_options = options;
  exporter_options.unset<GrpcChannelArgumentsNativeOption>();
  // Internal metrics must use ServiceTimeSeries to avoid permission issues.
  exporter_options.set<otel::ServiceTimeSeriesOption>(true);
  exporter_options.set<otel::MetricNameFormatterOption>([=](auto const& name) {
    return absl::StrCat(kBigtableMetricNamePath,
                        absl::StrReplaceAll(name, {{".", "/"}}));
  });

  auto metric_conn =
      conn != nullptr
          ? conn
          : monitoring_v3::MakeMetricServiceConnection(
                bigtable::internal::MetricsExporterConnectionOptions(
                    exporter_options));
  auto exporter = otel_internal::MakeMonitoringExporter(
      std::move(dynamic_resource_fn), std::move(resource_filter_fn),
      std::move(metric_conn), std::move(exporter_options));
  if (export_enabled != nullptr) {
    exporter = std::make_unique<GatedPushMetricExporter>(
        std::move(exporter), std::move(export_enabled));
  }

  auto reader_options =
      opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions{};
  // otel::PeriodicExportingMetricReader enforces that export_timeout_millis <
  // export_interval_millis.
  auto const period = options.has<bigtable::MetricsPeriodOption>() &&
                              options.get<bigtable::MetricsPeriodOption>() >
                                  std::chrono::seconds::zero()
                          ? options.get<bigtable::MetricsPeriodOption>()
                          : std::chrono::seconds(60);
  reader_options.export_interval_millis = period;
  reader_options.export_timeout_millis =
      (std::min)(std::chrono::milliseconds(std::chrono::seconds(30)),
                 reader_options.export_interval_millis / 2);

  auto provider =
      MakeGrpcMeterProvider(std::move(exporter), std::move(reader_options));

  auto metrics = std::vector<std::string_view>{
      std::string_view{"grpc.client.attempt.duration"},
      std::string_view{"grpc.lb.rls.default_target_picks"},
      std::string_view{"grpc.lb.rls.target_picks"},
      std::string_view{"grpc.lb.rls.failed_picks"},
      std::string_view{"grpc.xds_client.server_failure"},
      std::string_view{"grpc.xds_client.resource_updates_invalid"},
      std::string_view{"grpc.subchannel.disconnections"},
      std::string_view{"grpc.subchannel.connection_attempts_succeeded"},
      std::string_view{"grpc.subchannel.connection_attempts_failed"},
      std::string_view{"grpc.subchannel.open_connections"},
  };

  auto disable_metrics = std::vector<std::string_view>{
      std::string_view{"grpc.client.attempt.started"},
      std::string_view{
          "grpc.client.attempt.sent_total_compressed_message_size"},
      std::string_view{
          "grpc.client.attempt.rcvd_total_compressed_message_size"},
  };

  auto generic_method_filter = [](std::string_view target) {
    return absl::StartsWith(target, "google.bigtable.v2");
  };

  return GrpcMetricsPluginConfig{
      std::move(provider),
      std::move(metrics),
      std::move(disable_metrics),
      std::move(generic_method_filter),
  };
}

GrpcMetricsPluginConfig MakeGrpcMetricsPluginConfig(
    std::shared_ptr<monitoring_v3::MetricServiceConnection> const& conn,
    Options const& options, std::string const& client_uid,
    std::shared_ptr<std::atomic<bool>> export_enabled) {
  auto detector = otel::MakeResourceDetector();
  return MakeGrpcMetricsPluginConfig(detector->Detect(), conn, options,
                                     client_uid, std::move(export_enabled));
}

std::shared_ptr<grpc::experimental::OpenTelemetryPlugin> MakeGrpcMetricsPlugin(
    GrpcMetricsPluginConfig config) {
  auto plugin = grpc::OpenTelemetryPluginBuilder()
                    .SetMeterProvider(std::move(config.meter_provider))
                    .EnableMetrics(config.enabled_metrics)
                    .DisableMetrics(config.disabled_metrics)
                    .SetGenericMethodAttributeFilter(
                        std::move(config.generic_method_filter))
                    .Build();
  if (!plugin.ok()) {
    GCP_LOG(ERROR) << "Cannot build gRPC OpenTelemetry plugin: status="
                   << plugin.status().ToString();
    return nullptr;
  }
  return *std::move(plugin);
}

std::shared_ptr<grpc::experimental::OpenTelemetryPlugin> MakeGrpcMetricsPlugin(
    opentelemetry::sdk::resource::Resource const& detected_resource,
    std::shared_ptr<monitoring_v3::MetricServiceConnection> const& conn,
    Options const& options, std::string const& client_uid,
    std::shared_ptr<std::atomic<bool>> export_enabled) {
  return MakeGrpcMetricsPlugin(MakeGrpcMetricsPluginConfig(
      detected_resource, conn, options, client_uid, std::move(export_enabled)));
}

std::shared_ptr<grpc::experimental::OpenTelemetryPlugin> MakeGrpcMetricsPlugin(
    std::shared_ptr<monitoring_v3::MetricServiceConnection> const& conn,
    Options const& options, std::string const& client_uid,
    std::shared_ptr<std::atomic<bool>> export_enabled) {
  return MakeGrpcMetricsPlugin(MakeGrpcMetricsPluginConfig(
      conn, options, client_uid, std::move(export_enabled)));
}

Options WithGrpcMetricsPlugin(
    Options options,
    std::shared_ptr<grpc::experimental::OpenTelemetryPlugin> const& plugin) {
  if (plugin != nullptr) {
    plugin->AddToChannelArguments(
        &options.lookup<GrpcChannelArgumentsNativeOption>());
  }
  return options;
}

Options EnableGrpcMetricsForInstance(
    opentelemetry::sdk::resource::Resource const& detected_resource,
    std::shared_ptr<monitoring_v3::MetricServiceConnection> const& conn,
    Options options, std::string_view instance_name,
    std::string const& client_uid,
    std::shared_ptr<std::atomic<bool>> export_enabled) {
  auto instance = bigtable::MakeInstanceResource(std::string{instance_name});
  if (instance.ok()) {
    options.set<bigtable_internal::InstanceChannelAffinityOption>(
        {*std::move(instance)});
  }
  auto plugin = MakeGrpcMetricsPlugin(detected_resource, conn, options,
                                      client_uid, std::move(export_enabled));
  return WithGrpcMetricsPlugin(std::move(options), plugin);
}

#endif  // GOOGLE_CLOUD_CPP_BIGTABLE_WITH_GRPC_OTEL_METRICS

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

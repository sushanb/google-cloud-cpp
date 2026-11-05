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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_GRPC_METRICS_EXPORTER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_GRPC_METRICS_EXPORTER_H

#include "google/cloud/options.h"
#include "google/cloud/version.h"

#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_GRPC_OTEL_METRICS
#include "google/cloud/monitoring/v3/metric_connection.h"
#include "google/api/monitored_resource.pb.h"
#include <grpcpp/ext/otel_plugin.h>
#include <opentelemetry/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/data/metric_data.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/metrics/push_metric_exporter.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#endif  // GOOGLE_CLOUD_CPP_BIGTABLE_WITH_GRPC_OTEL_METRICS

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN

#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_GRPC_OTEL_METRICS
struct MonitoredResourceResult {
  std::string project_id;
  google::api::MonitoredResource resource;
};

MonitoredResourceResult MakeMonitoredResource(
    opentelemetry::sdk::metrics::PointDataAttributes const& pda,
    opentelemetry::sdk::resource::Resource const& detected_resource,
    Options const& options, std::string const& client_uid);

inline MonitoredResourceResult MakeMonitoredResource(
    opentelemetry::sdk::metrics::PointDataAttributes const& pda,
    Options const& options, std::string const& client_uid) {
  return MakeMonitoredResource(
      pda, opentelemetry::sdk::resource::Resource::Create({}), options,
      client_uid);
}

std::vector<double> const& MakeLatencyHistogramBoundaries();

std::shared_ptr<opentelemetry::metrics::MeterProvider> MakeGrpcMeterProvider(
    std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> exporter,
    opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions
        reader_options);

struct GrpcMetricsPluginConfig {
  std::shared_ptr<opentelemetry::metrics::MeterProvider> meter_provider;
  std::vector<std::string_view> enabled_metrics;
  std::vector<std::string_view> disabled_metrics;
  std::function<bool(std::string_view)> generic_method_filter;
};

GrpcMetricsPluginConfig MakeGrpcMetricsPluginConfig(
    opentelemetry::sdk::resource::Resource const& detected_resource,
    std::shared_ptr<monitoring_v3::MetricServiceConnection> const& conn,
    Options const& options, std::string const& client_uid,
    std::shared_ptr<std::atomic<bool>> export_enabled = nullptr);

GrpcMetricsPluginConfig MakeGrpcMetricsPluginConfig(
    std::shared_ptr<monitoring_v3::MetricServiceConnection> const& conn,
    Options const& options, std::string const& client_uid,
    std::shared_ptr<std::atomic<bool>> export_enabled = nullptr);

std::shared_ptr<grpc::experimental::OpenTelemetryPlugin> MakeGrpcMetricsPlugin(
    GrpcMetricsPluginConfig config);

std::shared_ptr<grpc::experimental::OpenTelemetryPlugin> MakeGrpcMetricsPlugin(
    opentelemetry::sdk::resource::Resource const& detected_resource,
    std::shared_ptr<monitoring_v3::MetricServiceConnection> const& conn,
    Options const& options, std::string const& client_uid,
    std::shared_ptr<std::atomic<bool>> export_enabled = nullptr);

std::shared_ptr<grpc::experimental::OpenTelemetryPlugin> MakeGrpcMetricsPlugin(
    std::shared_ptr<monitoring_v3::MetricServiceConnection> const& conn,
    Options const& options, std::string const& client_uid,
    std::shared_ptr<std::atomic<bool>> export_enabled = nullptr);

Options WithGrpcMetricsPlugin(
    Options options,
    std::shared_ptr<grpc::experimental::OpenTelemetryPlugin> const& plugin);

Options EnableGrpcMetricsForInstance(
    opentelemetry::sdk::resource::Resource const& detected_resource,
    std::shared_ptr<monitoring_v3::MetricServiceConnection> const& conn,
    Options options, std::string_view instance_name,
    std::string const& client_uid,
    std::shared_ptr<std::atomic<bool>> export_enabled = nullptr);
#endif  // GOOGLE_CLOUD_CPP_BIGTABLE_WITH_GRPC_OTEL_METRICS

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_GRPC_METRICS_EXPORTER_H

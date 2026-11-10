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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TELEMETRY_SESSION_TRACER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TELEMETRY_SESSION_TRACER_H

#include "google/cloud/bigtable/internal/sessions/telemetry/debug_snapshot.h"
#include "google/cloud/options.h"
#include "google/cloud/version.h"
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
#include <opentelemetry/common/key_value_iterable_view.h>
#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/meter_provider.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/metrics/sync_instruments.h>
#include <opentelemetry/nostd/shared_ptr.h>
#endif
#include "absl/container/flat_hash_map.h"
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace telemetry {

/// Configuration for `SessionTracer` passed from `SessionsDataConnection`
/// (`MetricsOperationContextFactory`) to `SessionPoolImpl`.
struct SessionTracerConfig {
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
  opentelemetry::nostd::shared_ptr<opentelemetry::metrics::MeterProvider>
      meter_provider;
#endif
  std::string project_id;
  std::string instance_id;
  std::string app_profile_id = "default";
  std::string client_name = "cbt-c++/";
  std::string client_uid;
  std::string client_project;
  std::string region;
  std::string cloud_platform;
  std::string host_id;
  std::string host_name;
  std::string session_type = "SESSION_TYPE_TABLE";
  std::string session_name = "default";
  bool use_short_instrument_names = true;
};

struct SessionsTracerConfigOption {
  using Type = SessionTracerConfig;
};

struct OpenLatencyAttributes {
  std::string transport_type;
  std::string status = "OK";
  std::string session_type;
  std::string afe_location;
  std::string session_name;
};

struct DurationAttributes {
  std::string transport_type;
  std::string status = "OK";
  std::string session_type;
  std::string afe_location;
  std::string session_name;
  std::string closing_reason = "USER";
  std::string vrpcs = "none";
  std::string ready = "false";
};

struct UptimeAttributes {
  std::string transport_type;
  std::string session_type;
  std::string ready = "true";
  std::string afe_location;
  std::string session_name;
};

struct TransportLatencyAttributes {
  std::string transport_type;
  std::string session_type;
  std::string afe_location;
  std::string session_name;
  std::string method = "Bigtable.ReadRow";
};

/// Emits session-scoped and transport-scoped histograms via `opentelemetry-cpp`
/// when `GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS` is enabled, and maintains
/// lock-free atomic telemetry counters for diagnostic snapshots.
class SessionTracer {
 public:
  SessionTracer() = default;

  explicit SessionTracer(SessionTracerConfig config) {
    Configure(std::move(config));
  }

  void Configure(SessionTracerConfig config) {
    std::lock_guard<std::mutex> lock(mu_);
    config_ = std::move(config);
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
    open_latencies_histogram_.reset();
    durations_histogram_.reset();
    uptime_histogram_.reset();
    transport_latencies_histogram_.reset();
    if (config_.meter_provider) {
      auto meter = config_.meter_provider->GetMeter(
          "google-cloud-cpp-bigtable-sessions", "1.0.0");
      if (meter) {
        std::string const prefix =
            config_.use_short_instrument_names
                ? ""
                : "bigtable.googleapis.com/internal/client/";
        open_latencies_histogram_ = meter->CreateDoubleHistogram(
            prefix + "session/open_latencies",
            "Time taken for a session to transition from Starting to Ready",
            "ms");
        durations_histogram_ = meter->CreateDoubleHistogram(
            prefix + "session/durations",
            "Total lifetime duration of closed sessions", "ms");
        uptime_histogram_ = meter->CreateDoubleHistogram(
            prefix + "session/uptime", "Active uptime of sessions", "ms");
        transport_latencies_histogram_ = meter->CreateDoubleHistogram(
            prefix + "transport_latencies",
            "Wire transport latency (E2E wire minus server backend latency)",
            "ms");
      }
    }
#endif
  }

  void RecordSessionOpenLatencyMs(double ms) {
    RecordSessionOpenLatencyMs(ms, OpenLatencyAttributes{});
  }

  void RecordSessionOpenLatencyMs(double ms,
                                  OpenLatencyAttributes const& attrs) {
    open_count_.fetch_add(1, std::memory_order_relaxed);
    last_open_latency_ms_.store(ms, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(mu_);
    last_open_attributes_ = attrs;
    if (last_open_attributes_.session_type.empty()) {
      last_open_attributes_.session_type = config_.session_type;
    }
    if (last_open_attributes_.session_name.empty()) {
      last_open_attributes_.session_name = config_.session_name;
    }
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
    if (open_latencies_histogram_) {
      auto map = MakeBaseAttributesLocked();
      map["transport_type"] = last_open_attributes_.transport_type;
      map["status"] = last_open_attributes_.status;
      map["session_type"] = last_open_attributes_.session_type;
      map["afe_location"] = last_open_attributes_.afe_location;
      map["session_name"] = last_open_attributes_.session_name;
      open_latencies_histogram_->Record(
          ms, opentelemetry::common::KeyValueIterableView<decltype(map)>(map),
          opentelemetry::context::RuntimeContext::GetCurrent());
    }
#endif
  }

  void RecordSessionDurationMs(double duration_ms) {
    RecordSessionDurationMs(duration_ms, DurationAttributes{});
  }

  void RecordSessionDurationMs(double duration_ms,
                               DurationAttributes const& attrs) {
    close_count_.fetch_add(1, std::memory_order_relaxed);
    last_duration_ms_.store(duration_ms, std::memory_order_relaxed);
    last_duration_sec_.store(duration_ms / 1000.0, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(mu_);
    last_duration_attributes_ = attrs;
    if (last_duration_attributes_.session_type.empty()) {
      last_duration_attributes_.session_type = config_.session_type;
    }
    if (last_duration_attributes_.session_name.empty()) {
      last_duration_attributes_.session_name = config_.session_name;
    }
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
    if (durations_histogram_) {
      auto map = MakeBaseAttributesLocked();
      map["transport_type"] = last_duration_attributes_.transport_type;
      map["status"] = last_duration_attributes_.status;
      map["session_type"] = last_duration_attributes_.session_type;
      map["afe_location"] = last_duration_attributes_.afe_location;
      map["session_name"] = last_duration_attributes_.session_name;
      map["closing_reason"] = last_duration_attributes_.closing_reason;
      map["vrpcs"] = last_duration_attributes_.vrpcs;
      map["ready"] = last_duration_attributes_.ready;
      durations_histogram_->Record(
          duration_ms,
          opentelemetry::common::KeyValueIterableView<decltype(map)>(map),
          opentelemetry::context::RuntimeContext::GetCurrent());
    }
#endif
  }

  void RecordSessionDurationSec(double duration_sec, double active_uptime_sec) {
    (void)active_uptime_sec;
    RecordSessionDurationMs(duration_sec * 1000.0);
  }

  void RecordSessionUptimeMs(double uptime_ms) {
    RecordSessionUptimeMs(uptime_ms, UptimeAttributes{});
  }

  void RecordSessionUptimeMs(double uptime_ms, UptimeAttributes const& attrs) {
    uptime_sample_count_.fetch_add(1, std::memory_order_relaxed);
    last_uptime_ms_.store(uptime_ms, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(mu_);
    last_uptime_attributes_ = attrs;
    if (last_uptime_attributes_.session_type.empty()) {
      last_uptime_attributes_.session_type = config_.session_type;
    }
    if (last_uptime_attributes_.session_name.empty()) {
      last_uptime_attributes_.session_name = config_.session_name;
    }
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
    if (uptime_histogram_) {
      auto map = MakeBaseAttributesLocked();
      map["transport_type"] = last_uptime_attributes_.transport_type;
      map["session_type"] = last_uptime_attributes_.session_type;
      map["ready"] = last_uptime_attributes_.ready;
      map["afe_location"] = last_uptime_attributes_.afe_location;
      map["session_name"] = last_uptime_attributes_.session_name;
      uptime_histogram_->Record(
          uptime_ms,
          opentelemetry::common::KeyValueIterableView<decltype(map)>(map),
          opentelemetry::context::RuntimeContext::GetCurrent());
    }
#endif
  }

  void RecordTransportLatencyMs(double transport_latency_ms) {
    RecordTransportLatencyMs(transport_latency_ms,
                             TransportLatencyAttributes{});
  }

  void RecordTransportLatencyMs(double transport_latency_ms,
                                TransportLatencyAttributes const& attrs) {
    vrpc_count_.fetch_add(1, std::memory_order_relaxed);
    last_transport_latency_ms_.store(transport_latency_ms,
                                     std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(mu_);
    last_transport_attributes_ = attrs;
    if (last_transport_attributes_.session_type.empty()) {
      last_transport_attributes_.session_type = config_.session_type;
    }
    if (last_transport_attributes_.session_name.empty()) {
      last_transport_attributes_.session_name = config_.session_name;
    }
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
    if (transport_latencies_histogram_) {
      auto map = MakeBaseAttributesLocked();
      map["transport_type"] = last_transport_attributes_.transport_type;
      map["session_type"] = last_transport_attributes_.session_type;
      map["afe_location"] = last_transport_attributes_.afe_location;
      map["session_name"] = last_transport_attributes_.session_name;
      map["method"] = last_transport_attributes_.method;
      transport_latencies_histogram_->Record(
          transport_latency_ms,
          opentelemetry::common::KeyValueIterableView<decltype(map)>(map),
          opentelemetry::context::RuntimeContext::GetCurrent());
    }
#endif
  }

  void RecordVRpcLatenciesMs(double client_blocking_ms,
                             double transport_latency_ms) {
    last_client_blocking_ms_.store(client_blocking_ms,
                                   std::memory_order_relaxed);
    RecordTransportLatencyMs(transport_latency_ms);
  }

  std::uint64_t open_count() const {
    return open_count_.load(std::memory_order_relaxed);
  }
  std::uint64_t close_count() const {
    return close_count_.load(std::memory_order_relaxed);
  }
  std::uint64_t uptime_sample_count() const {
    return uptime_sample_count_.load(std::memory_order_relaxed);
  }
  std::uint64_t vrpc_count() const {
    return vrpc_count_.load(std::memory_order_relaxed);
  }
  double last_open_latency_ms() const {
    return last_open_latency_ms_.load(std::memory_order_relaxed);
  }
  double last_duration_ms() const {
    return last_duration_ms_.load(std::memory_order_relaxed);
  }
  double last_uptime_ms() const {
    return last_uptime_ms_.load(std::memory_order_relaxed);
  }
  double last_transport_latency_ms() const {
    return last_transport_latency_ms_.load(std::memory_order_relaxed);
  }
  double last_client_blocking_ms() const {
    return last_client_blocking_ms_.load(std::memory_order_relaxed);
  }

  OpenLatencyAttributes last_open_attributes() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_open_attributes_;
  }
  DurationAttributes last_duration_attributes() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_duration_attributes_;
  }
  UptimeAttributes last_uptime_attributes() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_uptime_attributes_;
  }
  TransportLatencyAttributes last_transport_attributes() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_transport_attributes_;
  }
  SessionTracerConfig config() const {
    std::lock_guard<std::mutex> lock(mu_);
    return config_;
  }

 private:
  absl::flat_hash_map<std::string, std::string> MakeBaseAttributesLocked()
      const {
    absl::flat_hash_map<std::string, std::string> map;
    map.reserve(16);
    map["project_id"] = config_.project_id;
    map["instance"] = config_.instance_id;
    map["app_profile"] = config_.app_profile_id;
    map["client_name"] = config_.client_name;
    map["client_uid"] = config_.client_uid;
    if (!config_.client_project.empty()) {
      map["client_project"] = config_.client_project;
    }
    map["region"] = config_.region;
    map["cloud_platform"] = config_.cloud_platform;
    map["host_id"] = config_.host_id;
    if (!config_.host_name.empty()) {
      map["host_name"] = config_.host_name;
    }
    return map;
  }

  mutable std::mutex mu_;
  SessionTracerConfig config_;
  OpenLatencyAttributes last_open_attributes_;
  DurationAttributes last_duration_attributes_;
  UptimeAttributes last_uptime_attributes_;
  TransportLatencyAttributes last_transport_attributes_;

  std::atomic<std::uint64_t> open_count_{0};
  std::atomic<std::uint64_t> close_count_{0};
  std::atomic<std::uint64_t> uptime_sample_count_{0};
  std::atomic<std::uint64_t> vrpc_count_{0};
  std::atomic<double> last_open_latency_ms_{0.0};
  std::atomic<double> last_duration_ms_{0.0};
  std::atomic<double> last_duration_sec_{0.0};
  std::atomic<double> last_uptime_ms_{0.0};
  std::atomic<double> last_client_blocking_ms_{0.0};
  std::atomic<double> last_transport_latency_ms_{0.0};

#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Histogram<double>>
      open_latencies_histogram_;
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Histogram<double>>
      durations_histogram_;
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Histogram<double>>
      uptime_histogram_;
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Histogram<double>>
      transport_latencies_histogram_;
#endif
};

}  // namespace telemetry
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TELEMETRY_SESSION_TRACER_H

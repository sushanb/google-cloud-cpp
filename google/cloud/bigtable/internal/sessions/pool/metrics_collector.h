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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_METRICS_COLLECTOR_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_METRICS_COLLECTOR_H

#include "google/cloud/bigtable/internal/sessions/transport/peak_ewma_tracker.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/version.h"
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

struct LatencyPercentiles {
  std::chrono::nanoseconds p50{0};
  std::chrono::nanoseconds p90{0};
  std::chrono::nanoseconds p99{0};
  std::chrono::nanoseconds p999{0};
};

struct MetricsSnapshot {
  int active_sessions = 0;
  int pending_queue_size = 0;
  int active_calls = 0;
  std::chrono::nanoseconds peak_ewma_latency{0};
  std::map<google::cloud::StatusCode, std::int64_t> error_counts;
  std::int64_t success_count = 0;
  std::int64_t server_cancelled_count = 0;

  LatencyPercentiles queue_latency;
  LatencyPercentiles stream_latency;
};

class MetricsExporter {
 public:
  virtual ~MetricsExporter() = default;
  virtual void Export(MetricsSnapshot const& snapshot) = 0;
};

class LoggingMetricsExporter : public MetricsExporter {
 public:
  void Export(MetricsSnapshot const& snapshot) override;
};

class MetricsCollector : public std::enable_shared_from_this<MetricsCollector> {
 public:
  class ReservoirSampler {
   public:
    explicit ReservoirSampler(std::size_t max_samples = 10000);
    void Add(std::chrono::nanoseconds sample);
    std::vector<std::chrono::nanoseconds> GetSortedSamples() const;
    void Reset();

   private:
    std::size_t const max_samples_;
    std::unique_ptr<std::atomic<std::int64_t>[]> samples_ns_;
    std::atomic<std::size_t> total_samples_{0};
    std::atomic<std::uint32_t> rand_state_{123456789};
  };

  using ScheduleFn =
      std::function<void(std::chrono::nanoseconds, std::function<void()>)>;

  explicit MetricsCollector(
      ScheduleFn scheduler,
      std::chrono::nanoseconds export_interval = std::chrono::seconds(10));
  ~MetricsCollector() = default;

  void AddExporter(std::unique_ptr<MetricsExporter> exporter);

  // Event callbacks
  void OnRequestStart();
  void OnRequestComplete(std::chrono::nanoseconds latency,
                         google::cloud::Status const& status,
                         bool is_server_cancelled = false);
  void OnRequestComplete(std::chrono::nanoseconds queue_latency,
                         std::chrono::nanoseconds stream_latency,
                         google::cloud::Status const& status,
                         bool is_server_cancelled = false);
  void OnSessionCreated();
  void OnSessionClosed();

  // Setters for gauge metrics
  void SetPendingQueueSize(int size);
  void SetActiveSessions(int count);
  void SetActiveCalls(int count);

  void Start();
  void Stop();

  LatencyPercentiles GetQueuePercentiles() const;
  LatencyPercentiles GetStreamPercentiles() const;

 private:
  void ExportMetrics();
  void ScheduleNextExport();
  static LatencyPercentiles CalculatePercentiles(
      std::vector<std::chrono::nanoseconds> const& sorted);

  ScheduleFn scheduler_;
  std::chrono::nanoseconds export_interval_;

  std::mutex mutex_;
  bool running_ = false;

  std::vector<std::unique_ptr<MetricsExporter>> exporters_;

  // Metrics data (lock-free gauges)
  std::atomic<int> active_sessions_{0};
  std::atomic<int> pending_queue_size_{0};
  std::atomic<int> active_calls_{0};

  PeakEwmaTracker global_latency_tracker_;
  ReservoirSampler queue_latency_sampler_;
  ReservoirSampler stream_latency_sampler_;

  static constexpr std::size_t kNumStatusCodes = 17;
  std::array<std::atomic<std::int64_t>, kNumStatusCodes> error_counts_{};
  std::atomic<std::int64_t> success_count_{0};
  std::atomic<std::int64_t> server_cancelled_count_{0};
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_METRICS_COLLECTOR_H

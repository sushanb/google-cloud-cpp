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

#include "google/cloud/bigtable/internal/sessions/pool/metrics_collector.h"
#include "google/cloud/bigtable/internal/sessions/logging_helper.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "absl/strings/str_cat.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {
double ToMs(std::chrono::nanoseconds d) {
  return std::chrono::duration<double, std::milli>(d).count();
}
}  // namespace

void LoggingMetricsExporter::Export(MetricsSnapshot const& snapshot) {
  std::string err_str;
  for (auto const& [code, count] : snapshot.error_counts) {
    absl::StrAppend(&err_str, " ", google::cloud::StatusCodeToString(code), ":",
                    count);
  }
  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      1, "MetricsExporter",
      absl::StrCat("Pool Metrics: active_sessions=", snapshot.active_sessions,
                   " pending_queue=", snapshot.pending_queue_size,
                   " active_calls=", snapshot.active_calls,
                   " peak_ewma_latency_ms=", ToMs(snapshot.peak_ewma_latency),
                   " success=", snapshot.success_count,
                   " server_cancelled=", snapshot.server_cancelled_count,
                   " queue_p50_ms=", ToMs(snapshot.queue_latency.p50),
                   " queue_p99_ms=", ToMs(snapshot.queue_latency.p99),
                   " stream_p50_ms=", ToMs(snapshot.stream_latency.p50),
                   " stream_p99_ms=", ToMs(snapshot.stream_latency.p99),
                   " errors=[", err_str, " ]"));
}

// === ReservoirSampler ===

MetricsCollector::ReservoirSampler::ReservoirSampler(std::size_t max_samples)
    : max_samples_(max_samples),
      samples_ns_(std::make_unique<std::atomic<std::int64_t>[]>(max_samples_)) {
  Reset();
}

void MetricsCollector::ReservoirSampler::Add(std::chrono::nanoseconds sample) {
  if (max_samples_ == 0) return;
  std::int64_t const ns = sample.count();
  std::size_t const n =
      total_samples_.fetch_add(1, std::memory_order_relaxed) + 1;
  if (n <= max_samples_) {
    samples_ns_[n - 1].store(ns, std::memory_order_relaxed);
  } else {
    std::uint32_t const r = rand_state_.load(std::memory_order_relaxed);
    std::uint32_t const next_r = r * 1103515245U + 12345U;
    rand_state_.store(next_r, std::memory_order_relaxed);
    std::size_t const idx = static_cast<std::size_t>(next_r) % n;
    if (idx < max_samples_) {
      samples_ns_[idx].store(ns, std::memory_order_relaxed);
    }
  }
}

std::vector<std::chrono::nanoseconds>
MetricsCollector::ReservoirSampler::GetSortedSamples() const {
  std::size_t const count =
      (std::min)(max_samples_, total_samples_.load(std::memory_order_relaxed));
  std::vector<std::chrono::nanoseconds> sorted;
  sorted.reserve(count);
  for (std::size_t i = 0; i < count; ++i) {
    sorted.emplace_back(samples_ns_[i].load(std::memory_order_relaxed));
  }
  std::sort(sorted.begin(), sorted.end());
  return sorted;
}

void MetricsCollector::ReservoirSampler::Reset() {
  total_samples_.store(0, std::memory_order_relaxed);
  for (std::size_t i = 0; i < max_samples_; ++i) {
    samples_ns_[i].store(0, std::memory_order_relaxed);
  }
}

// === MetricsCollector ===

MetricsCollector::MetricsCollector(ScheduleFn scheduler,
                                   std::chrono::nanoseconds export_interval)
    : scheduler_(std::move(scheduler)),
      export_interval_(export_interval),
      queue_latency_sampler_(50000),
      stream_latency_sampler_(50000) {}

void MetricsCollector::AddExporter(std::unique_ptr<MetricsExporter> exporter) {
  std::lock_guard<std::mutex> l(mutex_);
  exporters_.push_back(std::move(exporter));
}

void MetricsCollector::OnRequestStart() {
  global_latency_tracker_.OnRequestStart();
}

void MetricsCollector::OnRequestComplete(std::chrono::nanoseconds latency,
                                         google::cloud::Status const& status,
                                         bool is_server_cancelled) {
  OnRequestComplete(std::chrono::nanoseconds(0), latency, status,
                    is_server_cancelled);
}

void MetricsCollector::OnRequestComplete(
    std::chrono::nanoseconds queue_latency,
    std::chrono::nanoseconds stream_latency,
    google::cloud::Status const& status, bool is_server_cancelled) {
  if (status.ok()) {
    global_latency_tracker_.OnRequestComplete(stream_latency);
    queue_latency_sampler_.Add(queue_latency);
    stream_latency_sampler_.Add(stream_latency);
    success_count_.fetch_add(1, std::memory_order_relaxed);
  } else {
    global_latency_tracker_.OnRequestFailed();
    auto const code_idx = static_cast<std::size_t>(status.code());
    if (code_idx < kNumStatusCodes) {
      error_counts_[code_idx].fetch_add(1, std::memory_order_relaxed);
    }
  }

  if (is_server_cancelled) {
    server_cancelled_count_.fetch_add(1, std::memory_order_relaxed);
  }
}

void MetricsCollector::OnSessionCreated() {
  active_sessions_.fetch_add(1, std::memory_order_relaxed);
}

void MetricsCollector::OnSessionClosed() {
  active_sessions_.fetch_sub(1, std::memory_order_relaxed);
}

void MetricsCollector::SetPendingQueueSize(int size) {
  pending_queue_size_.store(size, std::memory_order_relaxed);
}

void MetricsCollector::SetActiveSessions(int count) {
  active_sessions_.store(count, std::memory_order_relaxed);
}

void MetricsCollector::SetActiveCalls(int count) {
  active_calls_.store(count, std::memory_order_relaxed);
}

void MetricsCollector::Start() {
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (running_) return;
    running_ = true;
  }
  ScheduleNextExport();
}

void MetricsCollector::Stop() {
  std::lock_guard<std::mutex> l(mutex_);
  running_ = false;
}

void MetricsCollector::ScheduleNextExport() {
  if (scheduler_ == nullptr) return;
  scheduler_(export_interval_, [weak_self = weak_from_this()]() {
    if (auto self = weak_self.lock()) {
      self->ExportMetrics();
    }
  });
}

void MetricsCollector::ExportMetrics() {
  MetricsSnapshot snapshot;
  std::vector<MetricsExporter*> exporters_to_run;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (!running_) return;

    for (auto const& exporter : exporters_) {
      exporters_to_run.push_back(exporter.get());
    }
  }

  snapshot.active_sessions = active_sessions_.load(std::memory_order_relaxed);
  snapshot.pending_queue_size =
      pending_queue_size_.load(std::memory_order_relaxed);
  snapshot.active_calls = active_calls_.load(std::memory_order_relaxed);
  snapshot.peak_ewma_latency = global_latency_tracker_.GetCurrentEwma();
  snapshot.success_count = success_count_.load(std::memory_order_relaxed);
  snapshot.server_cancelled_count =
      server_cancelled_count_.load(std::memory_order_relaxed);
  for (std::size_t i = 0; i < kNumStatusCodes; ++i) {
    std::int64_t const count = error_counts_[i].load(std::memory_order_relaxed);
    if (count > 0) {
      snapshot.error_counts[static_cast<google::cloud::StatusCode>(i)] = count;
    }
  }

  snapshot.queue_latency = GetQueuePercentiles();
  snapshot.stream_latency = GetStreamPercentiles();

  for (auto* exporter : exporters_to_run) {
    exporter->Export(snapshot);
  }

  bool should_reschedule = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    should_reschedule = running_;
  }
  if (should_reschedule) {
    ScheduleNextExport();
  }
}

LatencyPercentiles MetricsCollector::GetQueuePercentiles() const {
  return CalculatePercentiles(queue_latency_sampler_.GetSortedSamples());
}

LatencyPercentiles MetricsCollector::GetStreamPercentiles() const {
  return CalculatePercentiles(stream_latency_sampler_.GetSortedSamples());
}

LatencyPercentiles MetricsCollector::CalculatePercentiles(
    std::vector<std::chrono::nanoseconds> const& sorted) {
  LatencyPercentiles p;
  if (sorted.empty()) return p;

  auto get_perc = [&](double percentile) {
    auto const idx = static_cast<std::size_t>(
        std::round(percentile * static_cast<double>(sorted.size() - 1)));
    return sorted[(std::min)(idx, sorted.size() - 1)];
  };

  p.p50 = get_perc(0.50);
  p.p90 = get_perc(0.90);
  p.p99 = get_perc(0.99);
  p.p999 = get_perc(0.999);
  return p;
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

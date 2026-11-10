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

#include "google/cloud/bigtable/internal/sessions/pool/dynamic_picker.h"
#include "google/cloud/bigtable/internal/sessions/session/session_impl.h"
#include "google/cloud/internal/random.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {
// The per-thread SWRR accumulator is pruned to the live candidate set once
// it exceeds kSwrrPruneFactor * candidates + kSwrrPruneSlack entries.
constexpr std::size_t kSwrrPruneFactor = 2;
constexpr std::size_t kSwrrPruneSlack = 16;
}  // namespace

DynamicPicker::DynamicPicker() = default;

void DynamicPicker::UpdateConfig(
    google::bigtable::v2::SessionClientConfiguration::
        SessionPoolConfiguration const& config) {
  if (config.has_load_balancing_options()) {
    auto const& opts = config.load_balancing_options();
    if (opts.has_random()) {
      strategy_.store(Strategy::kRandom, std::memory_order_relaxed);
      random_subset_size_.store(0, std::memory_order_relaxed);
    } else if (opts.has_peak_ewma()) {
      strategy_.store(Strategy::kLeastLatency, std::memory_order_relaxed);
      random_subset_size_.store(
          (std::max<std::int64_t>)(0, opts.peak_ewma().random_subset_size()),
          std::memory_order_relaxed);
    } else if (opts.has_least_in_flight()) {
      strategy_.store(Strategy::kLeastInFlight, std::memory_order_relaxed);
      std::int64_t const k = opts.least_in_flight().random_subset_size();
      random_subset_size_.store(k > 0 ? k : 2, std::memory_order_relaxed);
    } else {
      strategy_.store(Strategy::kLeastInFlight, std::memory_order_relaxed);
      random_subset_size_.store(2, std::memory_order_relaxed);
    }
  }
}

std::optional<std::shared_ptr<SessionImpl>> DynamicPicker::PickSession(
    std::vector<SessionEntry> const& sessions,
    std::unordered_set<SessionImpl*> const& ignored,
    bool is_write_request) const {
  Strategy const strat = strategy_.load(std::memory_order_relaxed);
  std::int64_t const subset_size =
      random_subset_size_.load(std::memory_order_relaxed);

  auto is_candidate =
      [&ignored, is_write_request](std::shared_ptr<SessionImpl> const& sess) {
        return sess != nullptr && sess->state() == SessionImpl::State::kReady &&
               (!is_write_request || !sess->IsWriteCongested()) &&
               sess->active_rpc_count() <
                   SessionImpl::kMaxConcurrentRpcsPerSession &&
               ignored.find(sess.get()) == ignored.end();
      };

  if (strat == Strategy::kRoundRobin) {
    if (sessions.empty()) return std::nullopt;
    thread_local std::size_t index = 0;

    std::size_t const size = sessions.size();
    for (std::size_t i = 0; i < size; ++i) {
      std::size_t const curr = (index + i) % size;
      if (is_candidate(sessions[curr].session)) {
        index = (curr + 1) % size;
        return sessions[curr].session;
      }
    }
    return std::nullopt;
  }

  if (strat == Strategy::kWeightedRoundRobin) {
    std::vector<std::shared_ptr<SessionImpl>> candidates;
    candidates.reserve(sessions.size());
    double total_weight = 0.0;
    for (auto const& entry : sessions) {
      if (is_candidate(entry.session)) {
        double const w = (std::max)(0.001, entry.session->GetPeerWeight());
        total_weight += w;
        candidates.push_back(entry.session);
      }
    }
    if (candidates.empty()) return std::nullopt;
    if (candidates.size() == 1) return candidates.front();

    // Deterministic Smooth Weighted Round-Robin (SWRR) accumulator per thread.
    // Keys are raw pointers of sessions seen by this thread. Sessions turn
    // over continuously (GoAway, refresh), so the map is pruned to the live
    // candidate set whenever it grows past a small multiple of it.
    struct SwrrState {
      std::unordered_map<SessionImpl*, double> current_weights;
    };
    thread_local SwrrState swrr;
    if (swrr.current_weights.size() >
        kSwrrPruneFactor * candidates.size() + kSwrrPruneSlack) {
      std::unordered_map<SessionImpl*, double> pruned;
      pruned.reserve(candidates.size());
      for (auto const& sess : candidates) {
        auto it = swrr.current_weights.find(sess.get());
        if (it != swrr.current_weights.end()) pruned.emplace(*it);
      }
      swrr.current_weights.swap(pruned);
    }

    SessionImpl* best_raw = nullptr;
    std::shared_ptr<SessionImpl> best_session;
    double max_current = -std::numeric_limits<double>::infinity();
    for (auto const& sess : candidates) {
      double const w = (std::max)(0.001, sess->GetPeerWeight());
      double& cw = swrr.current_weights[sess.get()];
      cw += w;
      if (cw > max_current) {
        max_current = cw;
        best_raw = sess.get();
        best_session = sess;
      }
    }
    if (best_raw != nullptr) {
      swrr.current_weights[best_raw] -= total_weight;
    }
    return best_session;
  }

  if (strat == Strategy::kRandom) {
    std::shared_ptr<SessionImpl> selected;
    std::size_t count = 0;
    thread_local google::cloud::internal::DefaultPRNG gen =
        google::cloud::internal::MakeDefaultPRNG();
    for (auto const& entry : sessions) {
      if (is_candidate(entry.session)) {
        count++;
        if (std::uniform_int_distribution<std::size_t>(0, count - 1)(gen) ==
            0) {
          selected = entry.session;
        }
      }
    }
    if (selected) return selected;
    return std::nullopt;
  }

  // Collect candidates (applying random_subset_size / Power-of-k-Choices if >
  // 0).
  std::vector<std::shared_ptr<SessionImpl>> pool_candidates;
  if (subset_size > 0 &&
      sessions.size() > static_cast<std::size_t>(subset_size)) {
    for (auto const& entry : sessions) {
      if (is_candidate(entry.session)) {
        pool_candidates.push_back(entry.session);
      }
    }
    if (pool_candidates.size() > static_cast<std::size_t>(subset_size)) {
      thread_local google::cloud::internal::DefaultPRNG gen =
          google::cloud::internal::MakeDefaultPRNG();
      for (std::size_t i = 0; i < static_cast<std::size_t>(subset_size); ++i) {
        std::size_t const j = std::uniform_int_distribution<std::size_t>(
            i, pool_candidates.size() - 1)(gen);
        std::swap(pool_candidates[i], pool_candidates[j]);
      }
      pool_candidates.resize(static_cast<std::size_t>(subset_size));
    }
  }

  if (strat == Strategy::kLeastLatency) {
    auto const now = PeakEwmaTracker::Clock::now();
    std::shared_ptr<SessionImpl> best_session;
    double best_score = (std::numeric_limits<double>::max)();
    if (!pool_candidates.empty()) {
      for (auto const& sess : pool_candidates) {
        double const score = sess->GetLatencyTracker()->GetScore(now);
        if (score < best_score) {
          best_score = score;
          best_session = sess;
        }
      }
    } else {
      for (auto const& entry : sessions) {
        if (is_candidate(entry.session)) {
          double const score =
              entry.session->GetLatencyTracker()->GetScore(now);
          if (score < best_score) {
            best_score = score;
            best_session = entry.session;
          }
        }
      }
    }
    if (best_session) return best_session;
    return std::nullopt;
  }

  // Strategy::kLeastInFlight
  std::shared_ptr<SessionImpl> best_session;
  int min_score = (std::numeric_limits<int>::max)();
  if (!pool_candidates.empty()) {
    for (auto const& sess : pool_candidates) {
      int const in_flight = sess->active_rpc_count();
      int const stub_calls = sess->GetStubActiveCalls();
      int const score = in_flight * 100 + stub_calls;
      if (score < min_score) {
        min_score = score;
        best_session = sess;
      }
    }
  } else {
    for (auto const& entry : sessions) {
      if (is_candidate(entry.session)) {
        int const in_flight = entry.session->active_rpc_count();
        int const stub_calls = entry.session->GetStubActiveCalls();
        int const score = in_flight * 100 + stub_calls;
        if (score < min_score) {
          min_score = score;
          best_session = entry.session;
        }
      }
    }
  }
  if (best_session) return best_session;
  return std::nullopt;
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

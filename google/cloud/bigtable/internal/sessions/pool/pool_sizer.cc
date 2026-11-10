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

#include "google/cloud/bigtable/internal/sessions/pool/pool_sizer.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_list.h"
#include <algorithm>
#include <cstddef>
#include <mutex>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

void PoolSizer::UpdateConfig(google::bigtable::v2::SessionClientConfiguration::
                                 SessionPoolConfiguration const& config) {
  std::lock_guard<std::mutex> l(mutex_);
  if (config.min_session_count() > 0) {
    min_sessions_ = static_cast<int>(config.min_session_count());
  }
  if (config.max_session_count() > 0) {
    max_sessions_ =
        (std::max)(min_sessions_, static_cast<int>(config.max_session_count()));
  }
  if (config.headroom() > 0.0F) {
    headroom_ = config.headroom();
  }
  if (config.new_session_queue_length() > 0) {
    new_session_queue_length_ =
        static_cast<int>(config.new_session_queue_length());
  }
}

int PoolSizer::GetScaleDelta(int ready_count, int connecting_count,
                             int in_use_count, int pending_count) const {
  int min_sess = 0;
  int max_sess = 0;
  float headrm = 0.0F;
  int queue_len = 10;

  {
    std::lock_guard<std::mutex> l(mutex_);
    min_sess = (std::max)(0, min_sessions_);
    max_sess = (std::max)(min_sess, max_sessions_);
    headrm = headroom_;
    queue_len = (std::max)(1, new_session_queue_length_);
  }

  std::size_t const desired = pool::ComputeDesiredPoolCapacity(
      static_cast<std::size_t>((std::max)(0, in_use_count)),
      static_cast<std::size_t>((std::max)(0, pending_count)),
      static_cast<std::size_t>(queue_len), static_cast<double>(headrm),
      static_cast<std::size_t>(min_sess), static_cast<std::size_t>(max_sess));
  return (std::max)(0, static_cast<int>(desired) -
                           (ready_count + connecting_count));
}

int PoolSizer::GetScaleDelta(int ready_count, int connecting_count,
                             int active_calls,
                             float /*avg_write_queue_size*/) const {
  return GetScaleDelta(ready_count, connecting_count, active_calls,
                       /*pending_count=*/0);
}

int PoolSizer::min_sessions() const {
  std::lock_guard<std::mutex> l(mutex_);
  return min_sessions_;
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

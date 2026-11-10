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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_LOGGING_HELPER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_LOGGING_HELPER_H

#include "google/cloud/internal/getenv.h"
#include "google/cloud/log.h"
#include "google/cloud/version.h"
#include "absl/strings/numbers.h"
#include <string_view>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace internal {

inline void LogVerbose(int layer, std::string_view class_name,
                       std::string_view details) {
  static int const kMaxLayer = []() {
    auto env = google::cloud::internal::GetEnv("SESSIONS_MAX_LOG_LAYER");
    int val = 0;
    return (env && absl::SimpleAtoi(*env, &val)) ? val : 0;
  }();
  if (layer <= kMaxLayer) {
    GCP_LOG(DEBUG) << "[SESSIONS] L" << layer << " - " << class_name << " - "
                   << details;
  }
}

inline void LogVerboseWarning(int layer, std::string_view class_name,
                              std::string_view details) {
  (void)layer;
  GCP_LOG(WARNING) << "[SESSIONS] WARNING - " << class_name << " - " << details;
}

}  // namespace internal
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_LOGGING_HELPER_H

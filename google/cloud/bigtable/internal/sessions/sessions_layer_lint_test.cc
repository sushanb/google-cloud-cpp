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

#include "google/cloud/version.h"
#include "absl/strings/match.h"
#include <gmock/gmock.h>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <regex>
#include <set>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

namespace fs = std::filesystem;

std::optional<int> ClassifyLayer(std::string const& rel_path) {
  if (rel_path == "latch.h" || rel_path == "logging_helper.h" ||
      rel_path == "sessions_options.h" ||
      absl::StartsWith(rel_path, "transport/")) {
    return 1;
  }
  if (absl::StartsWith(rel_path, "session/")) return 2;
  if (absl::StartsWith(rel_path, "pool/")) return 3;
  if (absl::StartsWith(rel_path, "middleware/")) return 4;
  if (rel_path == "client.h" || rel_path == "client.cc" ||
      rel_path == "client_test.cc" || absl::StartsWith(rel_path, "client/")) {
    return 5;
  }
  if (absl::StartsWith(rel_path, "sessions_data_connection") ||
      absl::StartsWith(rel_path, "shim/")) {
    return 6;
  }
  if (absl::StartsWith(rel_path, "telemetry/")) return 7;
  return std::nullopt;
}

bool IsTestSource(std::string const& rel_path) {
  return absl::EndsWith(rel_path, "_test.cc") ||
         absl::EndsWith(rel_path, "_test_helpers.h");
}

fs::path LocateSessionsRoot() {
  std::vector<fs::path> candidates = {
      fs::path("google/cloud/bigtable/internal/sessions"),
      fs::path("internal/sessions"),
  };
  if (char const* srcdir = std::getenv("TEST_SRCDIR"); srcdir != nullptr) {
    char const* ws = std::getenv("TEST_WORKSPACE");
    std::string const ws_name = (ws != nullptr && ws[0] != '\0') ? ws : "_main";
    candidates.push_back(fs::path(srcdir) / ws_name /
                         "google/cloud/bigtable/internal/sessions");
    candidates.push_back(
        fs::path(srcdir) /
        "google_cloud_cpp/google/cloud/bigtable/internal/sessions");
  }
  for (auto const& candidate : candidates) {
    if (fs::exists(candidate) && fs::is_directory(candidate)) {
      return candidate;
    }
  }
  return {};
}

TEST(SessionsLayerLintTest, B2_ExhaustiveIncludeScanOverAllSources) {
  auto const root = LocateSessionsRoot();
  if (std::getenv("TEST_SRCDIR") != nullptr) {
    ASSERT_FALSE(root.empty())
        << "Sessions source directory must be declared in BUILD.bazel data";
  } else if (root.empty()) {
    GTEST_SKIP()
        << "Sessions source directory not mounted in working directory";
  }

  std::set<std::string> const kForbiddenPublicModelHeaders = {
      "google/cloud/bigtable/row.h",
      "google/cloud/bigtable/mutations.h",
      "google/cloud/bigtable/filters.h",
      "google/cloud/bigtable/table.h",
      "google/cloud/bigtable/row_reader.h",
      "google/cloud/bigtable/read_modify_write_rule.h",
  };

  std::regex const include_re(R"re(^\s*#\s*include\s*[<"]([^>"]+)[>"])re");
  int scanned_files = 0;
  std::vector<std::string> violations;

  for (auto const& entry : fs::recursive_directory_iterator(
           root, fs::directory_options::follow_directory_symlink)) {
    if (!fs::is_regular_file(entry.path())) continue;
    auto const ext = entry.path().extension().string();
    if (ext != ".h" && ext != ".cc") continue;

    std::string const rel =
        entry.path().lexically_relative(root).generic_string();
    auto const src_layer = ClassifyLayer(rel);
    if (!src_layer.has_value()) continue;
    bool const is_test = IsTestSource(rel);
    ++scanned_files;

    std::ifstream in(entry.path());
    std::string line;
    int line_no = 0;
    while (std::getline(in, line)) {
      ++line_no;
      std::smatch m;
      if (!std::regex_search(line, m, include_re)) continue;
      std::string const inc = m[1].str();

      // Invariant B1: L1-L5 and L7 must stay proto-native and never include
      // public google::cloud::bigtable model headers.
      if (*src_layer != 6 && kForbiddenPublicModelHeaders.count(inc) > 0) {
        violations.push_back(rel + ":" + std::to_string(line_no) + " (L" +
                             std::to_string(*src_layer) +
                             ") includes public model header " + inc);
      }

      // Invariant B2: Downward 7-layer DAG (L7 telemetry is a leaf layer).
      constexpr std::string_view kPrefix =
          "google/cloud/bigtable/internal/sessions/";
      if (!is_test && absl::StartsWith(inc, kPrefix)) {
        std::string const target_rel(inc.substr(kPrefix.size()));
        auto const target_layer = ClassifyLayer(target_rel);
        if (target_layer.has_value()) {
          if (*src_layer == 7 && *target_layer != 7) {
            violations.push_back(rel + ":" + std::to_string(line_no) +
                                 " (L7 leaf) includes " + inc + " (L" +
                                 std::to_string(*target_layer) + ")");
          } else if (*target_layer != 7 && *target_layer > *src_layer) {
            violations.push_back(rel + ":" + std::to_string(line_no) + " (L" +
                                 std::to_string(*src_layer) +
                                 ") includes upward header " + inc + " (L" +
                                 std::to_string(*target_layer) + ")");
          }
        }
      }
    }
  }

  EXPECT_GT(scanned_files, 15);
  EXPECT_THAT(violations, ::testing::IsEmpty());
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

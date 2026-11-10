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
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <chrono>
#include <functional>
#include <memory>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

using ::testing::AllOf;
using ::testing::Field;
using ::testing::Key;

class MockMetricsExporter : public MetricsExporter {
 public:
  MOCK_METHOD(void, Export, (MetricsSnapshot const& snapshot), (override));
};

TEST(MetricsCollectorTest, CollectsAndExportsMetrics) {
  std::function<void()> export_task;
  auto mock_scheduler = [&](std::chrono::nanoseconds,
                            std::function<void()> task) {
    export_task = std::move(task);
  };

  auto collector = std::make_shared<MetricsCollector>(mock_scheduler,
                                                      std::chrono::seconds(5));
  auto mock_exporter = std::make_unique<MockMetricsExporter>();
  auto* mock_exporter_ptr = mock_exporter.get();
  collector->AddExporter(std::move(mock_exporter));

  collector->Start();
  ASSERT_TRUE(export_task != nullptr);

  collector->OnSessionCreated();
  collector->OnSessionCreated();
  collector->OnSessionClosed();

  collector->SetPendingQueueSize(3);
  collector->SetActiveCalls(5);

  collector->OnRequestStart();
  collector->OnRequestComplete(std::chrono::milliseconds(100),
                               google::cloud::Status());

  collector->OnRequestStart();
  collector->OnRequestComplete(
      std::chrono::milliseconds(200),
      google::cloud::internal::InvalidArgumentError("bad arg"));

  EXPECT_CALL(
      *mock_exporter_ptr,
      Export(AllOf(Field(&MetricsSnapshot::active_sessions, 1),
                   Field(&MetricsSnapshot::pending_queue_size, 3),
                   Field(&MetricsSnapshot::active_calls, 5),
                   Field(&MetricsSnapshot::success_count, 1),
                   Field(&MetricsSnapshot::server_cancelled_count, 0),
                   Field(&MetricsSnapshot::error_counts,
                         testing::Contains(Key(
                             google::cloud::StatusCode::kInvalidArgument))))));

  export_task();
}

TEST(MetricsCollectorTest, TracksServerCancelled) {
  std::function<void()> export_task;
  auto mock_scheduler = [&](std::chrono::nanoseconds,
                            std::function<void()> task) {
    export_task = std::move(task);
  };

  auto collector = std::make_shared<MetricsCollector>(mock_scheduler,
                                                      std::chrono::seconds(5));
  auto mock_exporter = std::make_unique<MockMetricsExporter>();
  auto* mock_exporter_ptr = mock_exporter.get();
  collector->AddExporter(std::move(mock_exporter));

  collector->Start();
  ASSERT_TRUE(export_task != nullptr);

  collector->OnRequestStart();
  collector->OnRequestComplete(
      std::chrono::milliseconds(100),
      google::cloud::internal::CancelledError("server cancelled"),
      /*is_server_cancelled=*/true);

  EXPECT_CALL(
      *mock_exporter_ptr,
      Export(AllOf(Field(&MetricsSnapshot::server_cancelled_count, 1),
                   Field(&MetricsSnapshot::error_counts,
                         testing::Contains(
                             Key(google::cloud::StatusCode::kCancelled))))));

  export_task();
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

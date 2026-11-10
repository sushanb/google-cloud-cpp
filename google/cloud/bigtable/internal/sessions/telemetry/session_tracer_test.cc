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

#include "google/cloud/bigtable/internal/sessions/telemetry/session_tracer.h"
#include <gtest/gtest.h>
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <memory>
#endif

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace telemetry {
namespace {

TEST(SessionTracerTest, RecordsCountersAndDefaultAttributes) {
  SessionTracerConfig cfg;
  cfg.project_id = "proj-1";
  cfg.instance_id = "inst-1";
  cfg.app_profile_id = "app-1";
  cfg.client_name = "cpp.Bigtable/2.36.0";
  cfg.client_uid = "uid-123";
  cfg.session_type = "SESSION_TYPE_TABLE";
  cfg.session_name = "projects/proj-1/instances/inst-1/tables/t1";
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
  auto sdk_provider =
      std::make_shared<opentelemetry::sdk::metrics::MeterProvider>();
  cfg.meter_provider =
      opentelemetry::nostd::shared_ptr<opentelemetry::metrics::MeterProvider>(
          sdk_provider);
#endif

  SessionTracer tracer(cfg);
  EXPECT_EQ(tracer.open_count(), 0u);
  EXPECT_EQ(tracer.close_count(), 0u);
  EXPECT_EQ(tracer.uptime_sample_count(), 0u);
  EXPECT_EQ(tracer.vrpc_count(), 0u);

  OpenLatencyAttributes open_attrs;
  open_attrs.transport_type = "transport_type_session_direct_access";
  open_attrs.status = "OK";
  open_attrs.afe_location = "us-east1-b:sub1";
  tracer.RecordSessionOpenLatencyMs(12.5, open_attrs);
  EXPECT_EQ(tracer.open_count(), 1u);
  EXPECT_DOUBLE_EQ(tracer.last_open_latency_ms(), 12.5);
  EXPECT_EQ(tracer.last_open_attributes().session_type, "SESSION_TYPE_TABLE");
  EXPECT_EQ(tracer.last_open_attributes().session_name,
            "projects/proj-1/instances/inst-1/tables/t1");
  EXPECT_EQ(tracer.last_open_attributes().afe_location, "us-east1-b:sub1");

  DurationAttributes dur_attrs;
  dur_attrs.transport_type = "transport_type_session_direct_access";
  dur_attrs.status = "OK";
  dur_attrs.closing_reason = "GOAWAY";
  dur_attrs.vrpcs = "all_ok";
  dur_attrs.ready = "true";
  tracer.RecordSessionDurationMs(60000.0, dur_attrs);
  EXPECT_EQ(tracer.close_count(), 1u);
  EXPECT_DOUBLE_EQ(tracer.last_duration_ms(), 60000.0);
  EXPECT_EQ(tracer.last_duration_attributes().closing_reason, "GOAWAY");
  EXPECT_EQ(tracer.last_duration_attributes().vrpcs, "all_ok");
  EXPECT_EQ(tracer.last_duration_attributes().ready, "true");

  UptimeAttributes up_attrs;
  up_attrs.transport_type = "transport_type_session_cloud_path";
  up_attrs.ready = "true";
  tracer.RecordSessionUptimeMs(30000.0, up_attrs);
  EXPECT_EQ(tracer.uptime_sample_count(), 1u);
  EXPECT_DOUBLE_EQ(tracer.last_uptime_ms(), 30000.0);

  TransportLatencyAttributes trans_attrs;
  trans_attrs.transport_type = "transport_type_session_direct_access";
  trans_attrs.method = "Bigtable.MutateRow";
  tracer.RecordTransportLatencyMs(0.75, trans_attrs);
  EXPECT_EQ(tracer.vrpc_count(), 1u);
  EXPECT_DOUBLE_EQ(tracer.last_transport_latency_ms(), 0.75);
  EXPECT_EQ(tracer.last_transport_attributes().method, "Bigtable.MutateRow");
}

}  // namespace
}  // namespace telemetry
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

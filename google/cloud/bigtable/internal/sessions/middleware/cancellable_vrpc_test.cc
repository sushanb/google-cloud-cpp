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

#include "google/cloud/bigtable/internal/sessions/middleware/cancellable_vrpc.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include <google/rpc/status.pb.h>
#include <gmock/gmock.h>
#include <grpcpp/grpcpp.h>
#include <chrono>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace middleware {
namespace {

TEST(CancellableVRpcTest, ProceedsDownstreamWhenContextClear) {
  grpc::ClientContext context;
  bool factory_invoked = false;

  auto factory = [&](CancellableVRpc::GenericCallback cb) {
    factory_invoked = true;
    if (cb != nullptr) {
      cb(google::bigtable::v2::VirtualRpcResponse());
    }
  };

  bool cb_invoked = false;
  CancellableVRpc::Intercept(
      &context, factory,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        cb_invoked = true;
        EXPECT_TRUE(resp.ok());
      });

  EXPECT_TRUE(factory_invoked);
  EXPECT_TRUE(cb_invoked);
}

TEST(CancellableVRpcTest, ShortCircuitsWhenDeadlineExpired) {
  grpc::ClientContext context;
  // Set deadline boundary in the past.
  context.set_deadline(std::chrono::system_clock::now() -
                       std::chrono::seconds(5));

  bool factory_invoked = false;
  auto factory = [&](auto /*cb*/) { factory_invoked = true; };

  bool cb_invoked = false;
  CancellableVRpc::Intercept(
      &context, factory,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        cb_invoked = true;
        EXPECT_EQ(resp.status().code(),
                  google::cloud::StatusCode::kDeadlineExceeded);
      });

  EXPECT_FALSE(factory_invoked);
  EXPECT_TRUE(cb_invoked);
}

TEST(CancellableVRpcTest, ProceedsDownstreamWhenContextIsNull) {
  bool factory_invoked = false;
  auto factory = [&](CancellableVRpc::GenericCallback cb) {
    factory_invoked = true;
    if (cb != nullptr) {
      cb(google::bigtable::v2::VirtualRpcResponse());
    }
  };

  bool cb_invoked = false;
  CancellableVRpc::Intercept(
      nullptr, factory,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        cb_invoked = true;
        EXPECT_TRUE(resp.ok());
      });

  EXPECT_TRUE(factory_invoked);
  EXPECT_TRUE(cb_invoked);
}

}  // namespace
}  // namespace middleware
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

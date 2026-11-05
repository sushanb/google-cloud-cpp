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

#include "google/cloud/bigtable/internal/stub_manager.h"
#include "google/cloud/bigtable/instance_resource.h"
#include "google/cloud/bigtable/internal/operation_context.h"
#include "google/cloud/bigtable/table_resource.h"
#include "google/cloud/bigtable/testing/mock_bigtable_stub.h"
#include "google/cloud/testing_util/scoped_log.h"
#include "google/cloud/testing_util/status_matchers.h"
#include <gmock/gmock.h>
#include <atomic>
#include <future>
#include <thread>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {

using ::google::cloud::bigtable::testing::MockBigtableStub;
using ::google::cloud::testing_util::IsOk;
using ::testing::Contains;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::MockFunction;
using ::testing::Not;
using ::testing::StartsWith;

TEST(StubManagerTest, NoAffinity) {
  bigtable::InstanceResource instance(Project("my-project"), "my-instance");
  bigtable::TableResource table(instance, "my-table");
  std::string const expected_table_name = table.FullName();
  auto mock = std::make_shared<MockBigtableStub>();
  EXPECT_CALL(*mock, MutateRow)
      .WillOnce([&](grpc::ClientContext&, Options const&,
                    google::bigtable::v2::MutateRowRequest const& request,
                    OperationContext&) {
        EXPECT_THAT(request.table_name(), Eq(expected_table_name));
        return google::bigtable::v2::MutateRowResponse{};
      });

  StubManager manager(mock);

  auto stub = manager.GetStub({});
  grpc::ClientContext context;
  google::bigtable::v2::MutateRowRequest request;
  request.set_table_name(expected_table_name);
  OperationContext op_ctx;
  auto result = stub->MutateRow(context, {}, request, op_ctx);
  EXPECT_THAT(result, IsOk());
}

TEST(StubManagerTest, AffinityToExistingInstance) {
  bigtable::InstanceResource instance(Project("my-project"), "a");
  bigtable::TableResource table(instance, "my-table");
  std::string const expected_table_name = table.FullName();

  auto mock_stub = std::make_shared<MockBigtableStub>();
  EXPECT_CALL(*mock_stub, MutateRow)
      .WillOnce([instance_name = instance.FullName()](
                    grpc::ClientContext&, Options const&,
                    google::bigtable::v2::MutateRowRequest const& request,
                    OperationContext&) {
        EXPECT_THAT(request.table_name(), StartsWith(instance_name));
        return google::bigtable::v2::MutateRowResponse{};
      });

  absl::flat_hash_map<std::string, std::shared_ptr<BigtableStub>>
      affinity_stubs;
  affinity_stubs[instance.FullName()] = mock_stub;

  MockFunction<std::shared_ptr<BigtableStub>(std::string_view,
                                             StubManager::Priming)>
      stub_creation_fn;
  EXPECT_CALL(stub_creation_fn, Call).Times(0);

  StubManager manager(affinity_stubs, stub_creation_fn.AsStdFunction());

  auto stub = manager.GetStub(instance.FullName());
  grpc::ClientContext context;
  google::bigtable::v2::MutateRowRequest request;
  request.set_table_name(expected_table_name);
  OperationContext op_ctx;
  auto result = stub->MutateRow(context, {}, request, op_ctx);
  EXPECT_THAT(result, IsOk());
}

TEST(StubManagerTest, AffinityToMissingInstance) {
  testing_util::ScopedLog log;
  bigtable::InstanceResource instance_a(Project("my-project"), "a");
  bigtable::TableResource table_a(instance_a, "my-table");
  auto mock_stub_a = std::make_shared<MockBigtableStub>();
  EXPECT_CALL(*mock_stub_a, MutateRow).Times(0);
  absl::flat_hash_map<std::string, std::shared_ptr<BigtableStub>>
      affinity_stubs;
  affinity_stubs[instance_a.FullName()] = mock_stub_a;

  auto stub_creation_fn = [](std::string_view instance_name,
                             StubManager::Priming) {
    auto mock_stub = std::make_shared<MockBigtableStub>();
    EXPECT_CALL(*mock_stub, MutateRow)
        .WillOnce([instance_name = std::string{instance_name}](
                      grpc::ClientContext&, Options const&,
                      google::bigtable::v2::MutateRowRequest const& request,
                      OperationContext&) {
          EXPECT_THAT(request.table_name(), StartsWith(instance_name));
          return google::bigtable::v2::MutateRowResponse{};
        });
    return mock_stub;
  };

  StubManager manager(affinity_stubs, stub_creation_fn);

  bigtable::InstanceResource instance_b(Project("my-project"), "b");
  bigtable::TableResource table_b(instance_b, "my-table");
  std::string const expected_table_name = table_b.FullName();
  auto stub = manager.GetStub(instance_b.FullName());
  grpc::ClientContext context;
  google::bigtable::v2::MutateRowRequest request;
  request.set_table_name(expected_table_name);
  OperationContext op_ctx;
  auto result = stub->MutateRow(context, {}, request, op_ctx);
  EXPECT_THAT(result, IsOk());
  EXPECT_THAT(log.ExtractLines(),
              Contains(HasSubstr(
                  "Dynamic connection resolution adding new channel pool")));
}

TEST(StubManagerTest, EmptyInstanceNameFallsBackToExistingStubWithoutCaching) {
  testing_util::ScopedLog log;
  bigtable::InstanceResource instance_a(Project("my-project"), "a");
  auto mock_stub_a = std::make_shared<MockBigtableStub>();
  absl::flat_hash_map<std::string, std::shared_ptr<BigtableStub>>
      affinity_stubs;
  affinity_stubs[instance_a.FullName()] = mock_stub_a;

  MockFunction<std::shared_ptr<BigtableStub>(std::string_view,
                                             StubManager::Priming)>
      stub_creation_fn;
  EXPECT_CALL(stub_creation_fn, Call).Times(0);

  StubManager manager(std::move(affinity_stubs),
                      stub_creation_fn.AsStdFunction());

  EXPECT_THAT(manager.GetStub(""), Eq(mock_stub_a));
  EXPECT_THAT(manager.GetStub(""), Eq(mock_stub_a));
  EXPECT_THAT(log.ExtractLines(),
              Not(Contains(HasSubstr("Dynamic connection resolution"))));
}

TEST(StubManagerTest, EmptyInstanceNameWithNoExistingStubsDoesNotCache) {
  testing_util::ScopedLog log;
  auto stub_1 = std::make_shared<MockBigtableStub>();
  auto stub_2 = std::make_shared<MockBigtableStub>();

  MockFunction<std::shared_ptr<BigtableStub>(std::string_view,
                                             StubManager::Priming)>
      stub_creation_fn;
  EXPECT_CALL(stub_creation_fn,
              Call(Eq(""), Eq(StubManager::Priming::kNoPriming)))
      .WillOnce([&](std::string_view, StubManager::Priming) { return stub_1; })
      .WillOnce([&](std::string_view, StubManager::Priming) { return stub_2; });

  StubManager manager({}, stub_creation_fn.AsStdFunction());

  EXPECT_THAT(manager.GetStub(""), Eq(stub_1));
  EXPECT_THAT(manager.GetStub(""), Eq(stub_2));
  EXPECT_THAT(log.ExtractLines(),
              Not(Contains(HasSubstr("Dynamic connection resolution"))));
}

TEST(StubManagerTest, StubCreationDoesNotBlockExistingInstanceReads) {
  bigtable::InstanceResource instance_a(Project("my-project"), "a");
  bigtable::InstanceResource instance_b(Project("my-project"), "b");
  auto mock_stub_a = std::make_shared<MockBigtableStub>();

  absl::flat_hash_map<std::string, std::shared_ptr<BigtableStub>>
      affinity_stubs;
  affinity_stubs[instance_a.FullName()] = mock_stub_a;

  std::promise<void> entered_creation;
  std::shared_future<void> entered_creation_future =
      entered_creation.get_future().share();
  std::promise<void> release_creation;
  std::shared_future<void> release_creation_future =
      release_creation.get_future().share();
  std::atomic<bool> notified{false};

  auto stub_creation_fn = [&](std::string_view instance_name,
                              StubManager::Priming priming) {
    EXPECT_THAT(instance_name, Eq(instance_b.FullName()));
    EXPECT_THAT(priming, Eq(StubManager::Priming::kNoPriming));
    if (!notified.exchange(true)) {
      entered_creation.set_value();
    }
    release_creation_future.wait();
    return std::make_shared<MockBigtableStub>();
  };

  StubManager manager(std::move(affinity_stubs), stub_creation_fn);

  std::shared_ptr<BigtableStub> stub_b_1;
  std::thread t1([&] { stub_b_1 = manager.GetStub(instance_b.FullName()); });
  entered_creation_future.wait();

  // While stub_creation_fn is in flight on t1, reading an already-cached
  // instance must succeed without blocking on mu_.
  EXPECT_THAT(manager.GetStub(instance_a.FullName()), Eq(mock_stub_a));

  std::shared_ptr<BigtableStub> stub_b_2;
  std::thread t2([&] { stub_b_2 = manager.GetStub(instance_b.FullName()); });

  release_creation.set_value();
  t1.join();
  t2.join();

  auto stub_b_cached = manager.GetStub(instance_b.FullName());
  EXPECT_NE(stub_b_cached, nullptr);
  EXPECT_THAT(stub_b_1, Eq(stub_b_cached));
  EXPECT_THAT(stub_b_2, Eq(stub_b_cached));
}

}  // namespace
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

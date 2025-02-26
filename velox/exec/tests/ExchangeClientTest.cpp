/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec {

namespace {

class ExchangeClientTest : public testing::Test,
                           public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    exec::ExchangeSource::factories().clear();
    exec::ExchangeSource::registerFactory(test::createLocalExchangeSource);
    if (!isRegisteredVectorSerde()) {
      velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }

    bufferManager_ = OutputBufferManager::getInstance().lock();
  }

  std::unique_ptr<SerializedPage> toSerializedPage(const RowVectorPtr& vector) {
    auto data = std::make_unique<VectorStreamGroup>(pool());
    auto size = vector->size();
    auto range = IndexRange{0, size};
    data->createStreamTree(asRowType(vector->type()), size);
    data->append(vector, folly::Range(&range, 1));
    auto listener = bufferManager_->newListener();
    IOBufOutputStream stream(*pool(), listener.get(), data->size());
    data->flush(&stream);
    return std::make_unique<SerializedPage>(stream.getIOBuf(), nullptr, size);
  }

  std::shared_ptr<Task> makeTask(
      const std::string& taskId,
      const core::PlanNodePtr& planNode) {
    auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
    queryCtx->testingOverrideMemoryPool(
        memory::memoryManager()->addRootPool(queryCtx->queryId()));
    return Task::create(
        taskId, core::PlanFragment{planNode}, 0, std::move(queryCtx));
  }

  int32_t enqueue(
      const std::string& taskId,
      int32_t destination,
      const RowVectorPtr& data) {
    auto page = toSerializedPage(data);
    const auto pageSize = page->size();
    ContinueFuture unused;
    auto blocked =
        bufferManager_->enqueue(taskId, destination, std::move(page), &unused);
    VELOX_CHECK(!blocked);
    return pageSize;
  }

  void fetchPages(ExchangeClient& client, int32_t numPages) {
    for (auto i = 0; i < numPages; ++i) {
      bool atEnd;
      ContinueFuture future;
      auto pages = client.next(1, &atEnd, &future);
      if (pages.empty()) {
        auto& exec = folly::QueuedImmediateExecutor::instance();
        std::move(future).via(&exec).wait();
        pages = client.next(1, &atEnd, &future);
      }
      ASSERT_EQ(1, pages.size());
    }
  }

  static void addSources(ExchangeQueue& queue, int32_t numSources) {
    {
      std::lock_guard<std::mutex> l(queue.mutex());
      for (auto i = 0; i < numSources; ++i) {
        queue.addSourceLocked();
      }
    }
    queue.noMoreSources();
  }

  static void enqueue(
      ExchangeQueue& queue,
      std::unique_ptr<SerializedPage> page) {
    std::vector<ContinuePromise> promises;
    {
      std::lock_guard<std::mutex> l(queue.mutex());
      queue.enqueueLocked(std::move(page), promises);
    }
    for (auto& promise : promises) {
      promise.setValue();
    }
  }

  static std::unique_ptr<SerializedPage> makePage(uint64_t size) {
    auto ioBuf = folly::IOBuf::create(size);
    ioBuf->append(size);
    return std::make_unique<SerializedPage>(std::move(ioBuf));
  }

  std::shared_ptr<OutputBufferManager> bufferManager_;
};

TEST_F(ExchangeClientTest, nonVeloxCreateExchangeSourceException) {
  ExchangeSource::registerFactory(
      [](const auto& taskId, auto destination, auto queue, auto pool)
          -> std::shared_ptr<ExchangeSource> {
        throw std::runtime_error("Testing error");
      });

  ExchangeClient client("t", 1, pool(), ExchangeClient::kDefaultMaxQueuedBytes);
  VELOX_ASSERT_THROW(
      client.addRemoteTaskId("task.1.2.3"),
      "Failed to create ExchangeSource: Testing error. Task ID: task.1.2.3.");

  // Test with a very long task ID. Make sure it is truncated.
  VELOX_ASSERT_THROW(
      client.addRemoteTaskId(std::string(1024, 'x')),
      "Failed to create ExchangeSource: Testing error. "
      "Task ID: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.");
}

TEST_F(ExchangeClientTest, stats) {
  auto data = {
      makeRowVector({makeFlatVector<int32_t>({1, 2, 3})}),
      makeRowVector({makeFlatVector<int32_t>({1, 2, 3, 4, 5})}),
      makeRowVector({makeFlatVector<int32_t>({1, 2})}),
  };

  auto plan = test::PlanBuilder()
                  .values(data)
                  .partitionedOutput({"c0"}, 100)
                  .planNode();
  auto taskId = "local://t1";
  auto task = makeTask(taskId, plan);

  bufferManager_->initializeTask(
      task, core::PartitionedOutputNode::Kind::kPartitioned, 100, 16);

  ExchangeClient client(
      "t", 17, pool(), ExchangeClient::kDefaultMaxQueuedBytes);
  client.addRemoteTaskId(taskId);

  // Enqueue 3 pages.
  std::vector<uint64_t> pageBytes;
  uint64_t totalBytes = 0;
  for (auto vector : data) {
    const auto pageSize = enqueue(taskId, 17, vector);
    totalBytes += pageSize;
    pageBytes.push_back(pageSize);
  }

  fetchPages(client, 3);

  auto stats = client.stats();
  EXPECT_EQ(totalBytes, stats.at("peakBytes").sum);
  EXPECT_EQ(data.size(), stats.at("numReceivedPages").sum);
  EXPECT_EQ(totalBytes / data.size(), stats.at("averageReceivedPageBytes").sum);

  task->requestCancel();
  bufferManager_->removeTask(taskId);
}

// Test scenario where fetching data from all sources at once would exceed queue
// size. Verify that ExchangeClient is fetching data only from a few sources at
// a time to avoid exceeding the limit.
TEST_F(ExchangeClientTest, flowControl) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(10'000, [](auto row) { return row; }),
  });

  auto page = toSerializedPage(data);

  // Set limit at 3.5 pages.
  ExchangeClient client("flow.control", 17, pool(), page->size() * 3.5);

  auto plan = test::PlanBuilder()
                  .values({data})
                  .partitionedOutput({"c0"}, 100)
                  .planNode();
  // Make 10 tasks.
  std::vector<std::shared_ptr<Task>> tasks;
  for (auto i = 0; i < 10; ++i) {
    auto taskId = fmt::format("local://t{}", i);
    auto task = makeTask(taskId, plan);

    bufferManager_->initializeTask(
        task, core::PartitionedOutputNode::Kind::kPartitioned, 100, 16);

    // Enqueue 3 pages.
    for (auto j = 0; j < 3; ++j) {
      enqueue(taskId, 17, data);
    }

    tasks.push_back(task);
    client.addRemoteTaskId(taskId);
  }

  fetchPages(client, 3 * tasks.size());

  auto stats = client.stats();
  EXPECT_LE(stats.at("peakBytes").sum, page->size() * 4);
  EXPECT_EQ(30, stats.at("numReceivedPages").sum);
  EXPECT_EQ(page->size(), stats.at("averageReceivedPageBytes").sum);

  for (auto& task : tasks) {
    task->requestCancel();
    bufferManager_->removeTask(task->taskId());
  }
}

TEST_F(ExchangeClientTest, multiPageFetch) {
  ExchangeClient client("test", 17, pool(), 1 << 20);

  bool atEnd;
  ContinueFuture future;
  auto pages = client.next(1, &atEnd, &future);
  ASSERT_EQ(0, pages.size());
  ASSERT_FALSE(atEnd);

  const auto& queue = client.queue();
  addSources(*queue, 1);

  for (auto i = 0; i < 10; ++i) {
    enqueue(*queue, makePage(1'000 + i));
  }

  // Fetch one page.
  pages = client.next(1, &atEnd, &future);
  ASSERT_EQ(1, pages.size());
  ASSERT_FALSE(atEnd);

  // Fetch multiple pages. Each page is slightly larger than 1K bytes, hence,
  // only 4 pages fit.
  pages = client.next(5'000, &atEnd, &future);
  ASSERT_EQ(4, pages.size());
  ASSERT_FALSE(atEnd);

  // Fetch the rest of the pages.
  pages = client.next(10'000, &atEnd, &future);
  ASSERT_EQ(5, pages.size());
  ASSERT_FALSE(atEnd);

  // Signal no-more-data.
  enqueue(*queue, nullptr);

  pages = client.next(10'000, &atEnd, &future);
  ASSERT_EQ(0, pages.size());
  ASSERT_TRUE(atEnd);
}

TEST_F(ExchangeClientTest, sourceTimeout) {
  constexpr int32_t kNumSources = 3;
  common::testutil::TestValue::enable();
  ExchangeClient client("test", 17, pool(), 1 << 20);

  bool atEnd;
  ContinueFuture future;
  auto pages = client.next(1, &atEnd, &future);
  ASSERT_EQ(0, pages.size());
  ASSERT_FALSE(atEnd);

  for (auto i = 0; i < kNumSources; ++i) {
    client.addRemoteTaskId(fmt::format("local://{}", i));
  }
  client.noMoreRemoteTasks();

  // Fetch a page. No page is found. All sources are fetching.
  pages = client.next(1, &atEnd, &future);
  EXPECT_TRUE(pages.empty());

  std::mutex mutex;
  std::unordered_set<void*> sourcesWithTimeout;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::test::LocalExchangeSource::timeout",
      std::function<void(void*)>(([&](void* source) {
        std::lock_guard<std::mutex> l(mutex);
        sourcesWithTimeout.insert(source);
      })));

#ifndef NDEBUG
  // Wait until all sources have timed out at least once.
  constexpr int32_t kMaxIters =
      3 * kNumSources * ExchangeClient::kDefaultMaxWaitSeconds;
  int32_t counter = 0;
  for (; counter < kMaxIters; ++counter) {
    {
      std::lock_guard<std::mutex> l(mutex);
      if (sourcesWithTimeout.size() == kNumSources) {
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  ASSERT_LT(counter, kMaxIters);
#endif

  const auto& queue = client.queue();
  for (auto i = 0; i < 10; ++i) {
    enqueue(*queue, makePage(1'000 + i));
  }

  // Fetch one page.
  pages = client.next(1, &atEnd, &future);
  ASSERT_EQ(1, pages.size());
  ASSERT_FALSE(atEnd);

  // Fetch multiple pages. Each page is slightly larger than 1K bytes, hence,
  // only 4 pages fit.
  pages = client.next(5'000, &atEnd, &future);
  ASSERT_EQ(4, pages.size());
  ASSERT_FALSE(atEnd);

  // Fetch the rest of the pages.
  pages = client.next(10'000, &atEnd, &future);
  ASSERT_EQ(5, pages.size());
  ASSERT_FALSE(atEnd);

  // Signal no-more-data for all sources.
  for (auto i = 0; i < kNumSources; ++i) {
    enqueue(*queue, nullptr);
  }
  pages = client.next(10'000, &atEnd, &future);
  ASSERT_EQ(0, pages.size());
  ASSERT_TRUE(atEnd);
}

TEST_F(ExchangeClientTest, timeoutDuringValueCallback) {
  common::testutil::TestValue::enable();
  auto row = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});

  auto plan = test::PlanBuilder()
                  .values({row})
                  .partitionedOutput({"c0"}, 100)
                  .planNode();
  auto taskId = "local://t1";
  auto task = makeTask(taskId, plan);

  bufferManager_->initializeTask(
      task, core::PartitionedOutputNode::Kind::kPartitioned, 100, 16);

  ExchangeClient client(
      "t", 17, pool(), ExchangeClient::kDefaultMaxQueuedBytes);
  client.addRemoteTaskId(taskId);
  int32_t numTimeouts = 0;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::test::LocalExchangeSource::timeout",
      std::function<void(void*)>(([&](void* /*ignore*/) { ++numTimeouts; })));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::test::LocalExchangeSource",
      std::function<void(void*)>(([&](void* /*pages*/) {
        std::this_thread::sleep_for(
            std::chrono::seconds(2 * ExchangeClient::kDefaultMaxWaitSeconds));
      })));

  auto thread = std::thread([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    enqueue(taskId, 17, row);
  });

  fetchPages(client, 1);
  thread.join();
  EXPECT_EQ(0, numTimeouts);

  task->requestCancel();
  bufferManager_->removeTask(taskId);
}

} // namespace
} // namespace facebook::velox::exec

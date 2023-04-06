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

#include "velox/substrait/tests/JsonToProtoConverter.h"

#include "velox/common/base/Fs.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "velox/substrait/SubstraitToVeloxPlan.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::substrait;

class Substrait2VeloxValuesNodeConversionTest : public OperatorTestBase {
 protected:
  std::shared_ptr<SubstraitVeloxPlanConverter> planConverter_ =
      std::make_shared<SubstraitVeloxPlanConverter>(pool_.get());
};

// SELECT * FROM tmp
TEST_F(Substrait2VeloxValuesNodeConversionTest, valuesNode) {
  auto planPath = getDataFilePath(
      "velox/substrait/tests", "data/substrait_virtualTable.json");

  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(planPath, substraitPlan);

  auto veloxPlan = planConverter_->toVeloxPlan(substraitPlan);

  RowVectorPtr expectedData = makeRowVector(
      {makeFlatVector<int64_t>(
           {2499109626526694126, 2342493223442167775, 4077358421272316858}),
       makeFlatVector<int32_t>({581869302, -708632711, -133711905}),
       makeFlatVector<double>(
           {0.90579193414549275, 0.96886777112423139, 0.63235925003444637}),
       makeFlatVector<bool>({true, false, false}),
       makeFlatVector<int32_t>(3, nullptr, nullEvery(1))

      });

  createDuckDbTable({expectedData});
  assertQuery(veloxPlan, "SELECT * FROM tmp");
}

TEST_F(Substrait2VeloxValuesNodeConversionTest, copy) {
  auto pool = facebook::velox::memory::getDefaultMemoryPool();

  auto a_vec = makeFlatVector<int32_t>({14, 11, 11, 10, 14, 13, 11, 12});
  auto b_vec = makeFlatVector<double_t>({0.14, 0.11, 0.11, 0.10, 0.14, 0.13, 0.11, 0.12});
  auto c_vec = makeFlatVector<StringView>({StringView("1"), StringView("2"), StringView("2"), StringView("3"), 
    StringView("1"), StringView("4"), StringView("2"), StringView("5")});

  auto row_vecs = makeRowVector({"a", "b", "c"}, {a_vec, b_vec, c_vec});

  // std::vector<int32_t> expected_values = {14, 11, 10, 13, 12};
  // auto expected_vec =
  //     makeVector<int32_t>(expected_values, facebook::velox::INTEGER(), pool.get());
  // auto expected_rowvec = makeRowVector({"a"}, {expected_vec}, pool.get());

  auto group_by_fragment = facebook::velox::exec::test::PlanBuilder()
                               .values({row_vecs})
                               .singleAggregation({"a", "c"}, {})
                               .planFragment();

std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(1));

  // testing the groupby without a consumer which doesn't need an extractor
  auto group_by_task = std::make_shared<facebook::velox::exec::Task>(
      "group_by_task", group_by_fragment, 0,
      std::make_shared<facebook::velox::core::QueryCtx>(executor.get()));

  auto result = group_by_task->next();
  while (auto tmp = group_by_task->next()) {
  }
  std::cout << "Without Consumer" << std::endl;
  std::cout << result->toString() << std::endl;
  std::cout << result->toString(0, 20) << std::endl;

  auto copy = facebook::velox::BaseVector::create<facebook::velox::RowVector>(
      result->type(), result->size(), pool_.get());
  copy->copy(result.get(), 0, 0, result->size());
  std::cout << "Without Consumer(copy)" << std::endl;
  std::cout << copy->toString() << std::endl;
  std::cout << copy->toString(0, 20) << std::endl;

}

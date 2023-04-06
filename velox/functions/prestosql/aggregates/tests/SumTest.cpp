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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/AggregationHook.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using facebook::velox::exec::test::PlanBuilder;
using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

class SumTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }

  template <
      typename InputType,
      typename ResultType,
      typename IntermediateType = ResultType>
  void testAggregateOverflow(
      bool expectOverflow = false,
      const TypePtr& type = CppToType<InputType>::create());
};

template <typename ResultType>
void verifyAggregates(
    const std::vector<std::pair<core::PlanNodePtr, ResultType>>& aggsToTest,
    bool expectOverflow) {
  for (const auto& [agg, expectedResult] : aggsToTest) {
    if (expectOverflow) {
      VELOX_ASSERT_THROW(readSingleValue(agg), "overflow");
    } else {
      auto result = readSingleValue(agg);
      if constexpr (std::is_same_v<ResultType, float>) {
        ASSERT_FLOAT_EQ(
            result.template value<TypeKind::REAL>(), expectedResult);
      } else if constexpr (std::is_same_v<ResultType, double>) {
        ASSERT_FLOAT_EQ(
            result.template value<TypeKind::DOUBLE>(), expectedResult);
      } else if constexpr (std::is_same_v<ResultType, UnscaledShortDecimal>) {
        auto output = result.template value<TypeKind::SHORT_DECIMAL>();
        ASSERT_EQ(output.value(), expectedResult);
      } else if constexpr (std::is_same_v<ResultType, UnscaledLongDecimal>) {
        auto output = result.template value<TypeKind::LONG_DECIMAL>();
        ASSERT_EQ(output.value(), expectedResult);
      } else {
        ASSERT_EQ(result, expectedResult);
      }
    }
  }
}

template <typename InputType, typename ResultType, typename IntermediateType>
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
__attribute__((no_sanitize("integer")))
#endif
#endif
void SumTest::testAggregateOverflow(
    bool expectOverflow,
    const TypePtr& type) {
  const InputType maxLimit = std::numeric_limits<InputType>::max();
  const InputType overflow = InputType(1);
  const InputType zero = InputType(0);

  // Intermediate type size is always >= result type size. Hence, use
  // intermediate type to calculate the expected output.
  IntermediateType limitResult = IntermediateType(maxLimit);
  IntermediateType overflowResult = IntermediateType(overflow);

  // Single max limit value. 0's to induce dummy calculations.
  auto limitVector =
      makeRowVector({makeFlatVector<InputType>({maxLimit, zero, zero}, type)});

  // Test code path for single values with possible overflow hit in add.
  auto overflowFlatVector =
      makeRowVector({makeFlatVector<InputType>({maxLimit, overflow}, type)});
  IntermediateType expectedFlatSum = limitResult + overflowResult;

  // Test code path for duplicate values with possible overflow hit in
  // multiply.
  auto overflowConstantVector =
      makeRowVector({makeConstant<InputType>(maxLimit / 3, 4, type)});
  IntermediateType expectedConstantSum = (limitResult / 3) * 4;

  // Test code path for duplicate values with possible overflow hit in add.
  auto overflowHybridVector = {limitVector, overflowConstantVector};
  IntermediateType expectedHybridSum = limitResult + expectedConstantSum;

  // Vector with element pairs of a partial aggregate node, expected result.
  std::vector<std::pair<core::PlanNodePtr, IntermediateType>> partialAggsToTest;
  // Partial Aggregation (raw input in - partial result out).
  partialAggsToTest.push_back(
      {PlanBuilder()
           .values({overflowFlatVector})
           .partialAggregation({}, {"sum(c0)"})
           .planNode(),
       expectedFlatSum});
  partialAggsToTest.push_back(
      {PlanBuilder()
           .values({overflowConstantVector})
           .partialAggregation({}, {"sum(c0)"})
           .planNode(),
       expectedConstantSum});
  partialAggsToTest.push_back(
      {PlanBuilder()
           .values(overflowHybridVector)
           .partialAggregation({}, {"sum(c0)"})
           .planNode(),
       expectedHybridSum});

  // Vector with element pairs of a full aggregate node, expected result.
  std::vector<std::pair<core::PlanNodePtr, ResultType>> aggsToTest;
  // Single Aggregation (raw input in - final result out).
  aggsToTest.push_back(
      {PlanBuilder()
           .values({overflowFlatVector})
           .singleAggregation({}, {"sum(c0)"})
           .planNode(),
       expectedFlatSum});
  aggsToTest.push_back(
      {PlanBuilder()
           .values({overflowConstantVector})
           .singleAggregation({}, {"sum(c0)"})
           .planNode(),
       expectedConstantSum});
  aggsToTest.push_back(
      {PlanBuilder()
           .values(overflowHybridVector)
           .singleAggregation({}, {"sum(c0)"})
           .planNode(),
       expectedHybridSum});
  // Final Aggregation (partial result in - final result out):
  // To make sure that the overflow occurs in the final aggregation step, we
  // create 2 plan fragments and plugging their partially aggregated
  // output into a final aggregate plan node. Each of those input fragments
  // only have a single input value under the max limit which when added in
  // the final step causes a potential overflow.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  aggsToTest.push_back(
      {PlanBuilder(planNodeIdGenerator)
           .localPartition(
               {},
               {PlanBuilder(planNodeIdGenerator)
                    .values({limitVector})
                    .partialAggregation({}, {"sum(c0)"})
                    .planNode(),
                PlanBuilder(planNodeIdGenerator)
                    .values({limitVector})
                    .partialAggregation({}, {"sum(c0)"})
                    .planNode()})
           .finalAggregation()
           .planNode(),
       limitResult + limitResult});

  // Verify all partial aggregates.
  verifyAggregates<IntermediateType>(partialAggsToTest, expectOverflow);
  // Verify all aggregates.
  verifyAggregates<ResultType>(aggsToTest, expectOverflow);
}

TEST_F(SumTest, sumTinyint) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), TINYINT()});
  auto vectors = makeVectors(rowType, 1000, 10);
  createDuckDbTable(vectors);

  // Global aggregation.
  testAggregations(vectors, {}, {"sum(c1)"}, "SELECT sum(c1) FROM tmp");

  // Group by aggregation.
  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).project({"c0 % 10", "c1"});
      },
      {"p0"},
      {"sum(c1)"},
      "SELECT c0 % 10, sum(c1) FROM tmp GROUP BY 1");

  // Encodings: use filter to wrap aggregation inputs in a dictionary.
  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).filter("c0 % 2 = 0").project({"c0 % 11", "c1"});
      },
      {"p0"},
      {"sum(c1)"},
      "SELECT c0 % 11, sum(c1) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1");

  testAggregations(
      [&](auto& builder) { builder.values(vectors).filter("c0 % 2 = 0"); },
      {},
      {"sum(c1)"},
      "SELECT sum(c1) FROM tmp WHERE c0 % 2 = 0");
}

TEST_F(SumTest, sumFloat) {
  auto data = makeRowVector({makeFlatVector<float>({2.00, 1.00})});
  createDuckDbTable({data});

  testAggregations(
      [&](auto& builder) { builder.values({data}); },
      {},
      {"sum(c0)"},
      "SELECT sum(c0) FROM tmp");
}

TEST_F(SumTest, sumDoubleAndFloat) {
  for (int iter = 0; iter < 3; ++iter) {
    SCOPED_TRACE(fmt::format("test iterations: {}", iter));
    auto rowType = ROW({"c0", "c1", "c2"}, {REAL(), DOUBLE(), INTEGER()});
    auto vectors = makeVectors(rowType, 1000, 10);

    createDuckDbTable(vectors);

    // With group by.
    testAggregations(
        vectors,
        {"c2"},
        {"sum(c0)", "sum(c1)"},
        "SELECT c2, sum(c0), sum(c1) FROM tmp GROUP BY c2");

    // Without group by.
    testAggregations(
        vectors,
        {},
        {"sum(c0)", "sum(c1)"},
        "SELECT sum(c0), sum(c1) FROM tmp");
  }
}

TEST_F(SumTest, sumDecimal) {
  std::vector<std::optional<int64_t>> shortDecimalRawVector;
  std::vector<std::optional<int128_t>> longDecimalRawVector;
  for (int i = 0; i < 1000; ++i) {
    shortDecimalRawVector.push_back(i * 1000);
    longDecimalRawVector.push_back(buildInt128(i * 10, i * 100));
  }
  shortDecimalRawVector.push_back(std::nullopt);
  longDecimalRawVector.push_back(std::nullopt);
  auto input = makeRowVector(
      {makeNullableShortDecimalFlatVector(
           shortDecimalRawVector, DECIMAL(10, 1)),
       makeNullableLongDecimalFlatVector(
           longDecimalRawVector, DECIMAL(23, 4))});
  createDuckDbTable({input});
  testAggregations(
      {input}, {}, {"sum(c0)", "sum(c1)"}, "SELECT sum(c0), sum(c1) FROM tmp");
}

TEST_F(SumTest, sumDecimalOverflow) {
  // Short decimals do not overflow easily.
  std::vector<int64_t> shortDecimalInput;
  for (int i = 0; i < 10'000; ++i) {
    shortDecimalInput.push_back(UnscaledShortDecimal::max().unscaledValue());
  }
  auto input = makeRowVector(
      {makeShortDecimalFlatVector(shortDecimalInput, DECIMAL(17, 5))});
  createDuckDbTable({input});
  testAggregations({input}, {}, {"sum(c0)"}, "SELECT sum(c0) FROM tmp");

  auto decimalSumOverflow = [this](
                                const std::vector<int128_t>& input,
                                const std::vector<int128_t>& output) {
    const TypePtr type = DECIMAL(38, 0);
    auto in = makeRowVector({makeLongDecimalFlatVector({input}, type)});
    auto expected = makeRowVector({makeLongDecimalFlatVector({output}, type)});
    PlanBuilder builder(pool());
    builder.values({in});
    builder.singleAggregation({}, {"sum(c0)"});
    AssertQueryBuilder queryBuilder(
        builder.planNode(), this->duckDbQueryRunner_);
    queryBuilder.assertResults({expected});
  };

  // Test Positive Overflow.
  std::vector<int128_t> longDecimalInput;
  std::vector<int128_t> longDecimalOutput;
  // Create input with 2 UnscaledLongDecimal::max().
  longDecimalInput.push_back(UnscaledLongDecimal::max().unscaledValue());
  longDecimalInput.push_back(UnscaledLongDecimal::max().unscaledValue());
  // The sum must overflow.
  VELOX_ASSERT_THROW(
      decimalSumOverflow(longDecimalInput, longDecimalOutput),
      "Decimal overflow");

  // Now add UnscaledLongDecimal::min().
  // The sum now must not overflow.
  longDecimalInput.push_back(UnscaledLongDecimal::min().unscaledValue());
  longDecimalOutput.push_back(UnscaledLongDecimal::max().unscaledValue());
  decimalSumOverflow(longDecimalInput, longDecimalOutput);

  // Test Negative Overflow.
  longDecimalInput.clear();
  longDecimalOutput.clear();

  // Create input with 2 UnscaledLongDecimal::min().
  longDecimalInput.push_back(UnscaledLongDecimal::min().unscaledValue());
  longDecimalInput.push_back(UnscaledLongDecimal::min().unscaledValue());

  // The sum must overflow.
  VELOX_ASSERT_THROW(
      decimalSumOverflow(longDecimalInput, longDecimalOutput),
      "Decimal overflow");

  // Now add UnscaledLongDecimal::max().
  // The sum now must not overflow.
  longDecimalInput.push_back(UnscaledLongDecimal::max().unscaledValue());
  longDecimalOutput.push_back(UnscaledLongDecimal::min().unscaledValue());
  decimalSumOverflow(longDecimalInput, longDecimalOutput);

  // Check value in range.
  longDecimalInput.clear();
  longDecimalInput.push_back(UnscaledLongDecimal::max().unscaledValue());
  longDecimalInput.push_back(1);
  VELOX_ASSERT_THROW(
      decimalSumOverflow(longDecimalInput, longDecimalOutput),
      "Decimal overflow");

  longDecimalInput.clear();
  longDecimalInput.push_back(UnscaledLongDecimal::min().unscaledValue());
  longDecimalInput.push_back(-1);
  VELOX_ASSERT_THROW(
      decimalSumOverflow(longDecimalInput, longDecimalOutput),
      "Decimal overflow");
}

TEST_F(SumTest, sumWithMask) {
  auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4"},
          {INTEGER(), TINYINT(), BIGINT(), BIGINT(), INTEGER()});
  auto vectors = makeVectors(rowType, 100, 10);

  core::PlanNodePtr op;
  createDuckDbTable(vectors);

  // Aggregations 0 and 1 will use the same channel, but different masks.
  // Aggregations 1 and 2 will use different channels, but the same mask.

  // Global partial+final aggregation.
  op = PlanBuilder()
           .values(vectors)
           .project({"c0", "c1", "c2 % 2 = 0 AS m0", "c3 % 3 = 0 AS m1"})
           .partialAggregation(
               {}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op,
      "SELECT sum(c0) filter (where c2 % 2 = 0), "
      "sum(c0) filter (where c3 % 3 = 0), sum(c1) filter (where c3 % 3 = 0) "
      "FROM tmp");

  // Use mask that's always false.
  op = PlanBuilder()
           .values(vectors)
           .project({"c0", "c1", "c2 % 2 > 10 AS m0", "c3 % 3 = 0 AS m1"})
           .partialAggregation(
               {}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op,
      "SELECT sum(c0) filter (where c2 % 2 > 10), "
      "sum(c0) filter (where c3 % 3 = 0), sum(c1) filter (where c3 % 3 = 0) "
      "FROM tmp");

  // Encodings: use filter to wrap aggregation inputs in a dictionary.
  // Global partial+final aggregation.
  op = PlanBuilder()
           .values(vectors)
           .filter("c3 % 2 = 0")
           .project({"c0", "c1", "c2 % 2 = 0 AS m0", "c3 % 3 = 0 AS m1"})
           .partialAggregation(
               {}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op,
      "SELECT sum(c0) filter (where c2 % 2 = 0), "
      "sum(c0) filter (where c3 % 3 = 0), sum(c1) filter (where c3 % 3 = 0) "
      "FROM tmp where c3 % 2 = 0");

  // Group by partial+final aggregation.
  op = PlanBuilder()
           .values(vectors)
           .project({"c4", "c0", "c1", "c2 % 2 = 0 AS m0", "c3 % 3 = 0 AS m1"})
           .partialAggregation(
               {"c4"}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op,
      "SELECT c4, sum(c0) filter (where c2 % 2 = 0), "
      "sum(c0) filter (where c3 % 3 = 0), sum(c1) filter (where c3 % 3 = 0) "
      "FROM tmp group by c4");

  // Encodings: use filter to wrap aggregation inputs in a dictionary.
  // Group by partial+final aggregation.
  op = PlanBuilder()
           .values(vectors)
           .filter("c3 % 2 = 0")
           .project({"c4", "c0", "c1", "c2 % 2 = 0 AS m0", "c3 % 3 = 0 AS m1"})
           .partialAggregation(
               {"c4"}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op,
      "SELECT c4, sum(c0) filter (where c2 % 2 = 0), "
      "sum(c0) filter (where c3 % 3 = 0), sum(c1) filter (where c3 % 3 = 0) "
      "FROM tmp where c3 % 2 = 0 group by c4");

  // Use mask that's always false.
  op = PlanBuilder()
           .values(vectors)
           .filter("c3 % 2 = 0")
           .project({"c4", "c0", "c1", "c2 % 2 > 10 AS m0", "c3 % 3 = 0 AS m1"})
           .partialAggregation(
               {"c4"}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
           .finalAggregation()
           .planNode();
  assertQuery(
      op,
      "SELECT c4, sum(c0) filter (where c2 % 2 > 10), "
      "sum(c0) filter (where c3 % 3 = 0), sum(c1) filter (where c3 % 3 = 0) "
      "FROM tmp where c3 % 2 = 0 group by c4");
}

// Test aggregation over boolean key
TEST_F(SumTest, boolKey) {
  vector_size_t size = 1'000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 5; ++i) {
    vectors.push_back(makeRowVector(
        {makeFlatVector<bool>(size, [](auto row) { return row % 3 == 0; }),
         makeFlatVector<int32_t>(size, [](auto row) { return row; })}));
  }
  createDuckDbTable(vectors);

  testAggregations(
      vectors, {"c0"}, {"sum(c1)"}, "SELECT c0, sum(c1) FROM tmp GROUP BY 1");
}

TEST_F(SumTest, emptyValues) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});
  auto vector = makeRowVector(
      {makeFlatVector<int32_t>(std::vector<int32_t>{}),
       makeFlatVector<int64_t>(std::vector<int64_t>{})});

  testAggregations({vector}, {"c0"}, {"sum(c1)"}, "");
}

/// Test aggregating over lots of null values.
TEST_F(SumTest, nulls) {
  vector_size_t size = 10'000;

  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 5; ++i) {
    vectors.push_back(makeRowVector(
        {makeFlatVector<int32_t>(size, [](auto row) { return row; }),
         makeFlatVector<int32_t>(
             size, [](auto row) { return row; }, nullEvery(3))}));
  }

  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {"c0"},
      {"sum(c1) AS sum_c1"},
      "SELECT c0, sum(c1) as sum_c1 FROM tmp GROUP BY 1");
}

template <typename Type>
struct SumRow {
  char nulls;
  Type sum;
};

TEST_F(SumTest, hook) {
  SumRow<int64_t> sumRow;
  sumRow.nulls = 1;
  sumRow.sum = 0;

  char* row = reinterpret_cast<char*>(&sumRow);
  uint64_t numNulls = 1;
  aggregate::SumHook<int64_t, int64_t> hook(
      offsetof(SumRow<int64_t>, sum),
      offsetof(SumRow<int64_t>, nulls),
      1,
      &row,
      &numNulls);

  int64_t value = 11;
  hook.addValue(0, &value);
  EXPECT_EQ(0, sumRow.nulls);
  EXPECT_EQ(0, numNulls);
  EXPECT_EQ(value, sumRow.sum);
}

template <typename InputType, typename ResultType>
void testHookLimits(bool expectOverflow = false) {
  // Pair of <limit, value to overflow>.
  std::vector<std::pair<InputType, InputType>> limits = {
      {std::numeric_limits<InputType>::min(), -1},
      {std::numeric_limits<InputType>::max(), 1}};

  for (const auto& [limit, overflow] : limits) {
    SumRow<ResultType> sumRow;
    sumRow.sum = 0;
    ResultType expected = 0;
    char* row = reinterpret_cast<char*>(&sumRow);
    uint64_t numNulls = 0;
    aggregate::SumHook<InputType, ResultType> hook(
        offsetof(SumRow<ResultType>, sum),
        offsetof(SumRow<ResultType>, nulls),
        0,
        &row,
        &numNulls);

    // Adding limit should not overflow.
    ASSERT_NO_THROW(hook.addValue(0, &limit));
    expected += limit;
    EXPECT_EQ(expected, sumRow.sum);
    // Adding overflow based on the ResultType should throw.
    if (expectOverflow) {
      VELOX_ASSERT_THROW(hook.addValue(0, &overflow), "overflow");
    } else {
      ASSERT_NO_THROW(hook.addValue(0, &overflow));
      expected += overflow;
      EXPECT_EQ(expected, sumRow.sum);
    }
  }
}

TEST_F(SumTest, hookLimits) {
  testHookLimits<int32_t, int64_t>();
  testHookLimits<int64_t, int64_t>(true);
  // Float and Double do not throw an overflow error.
  testHookLimits<float, double>();
  testHookLimits<double, double>();
}

TEST_F(SumTest, integerAggregateOverflow) {
  testAggregateOverflow<int8_t, int64_t>();
  testAggregateOverflow<int16_t, int64_t>();
  testAggregateOverflow<int32_t, int64_t>();
  testAggregateOverflow<int64_t, int64_t>(true);
}

TEST_F(SumTest, floatAggregateOverflow) {
  testAggregateOverflow<float, float, double>();
  testAggregateOverflow<double, double>();
}

TEST_F(SumTest, sum0NoNull) {
  auto a_vec = makeFlatVector<int32_t>({14, 11, 11, 10, 14, 13, 11, 12});
  auto b_vec = makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8});
  auto c_vec = makeFlatVector<StringView>({StringView("1"), StringView("2"), StringView("2"), StringView("3"), 
    StringView("1"), StringView("4"), StringView("2"), StringView("5")});

  auto row_vecs = makeRowVector({"c0", "c1", "c2"}, {a_vec, b_vec, c_vec});

  auto group_by_fragment = PlanBuilder()
           .values({row_vecs})
           .singleAggregation(
               {"c0"}, {"sum(c1)"})
           .planFragment();

  facebook::velox::exec::Consumer consumer =
      [&](facebook::velox::RowVectorPtr output,
          facebook::velox::ContinueFuture*) -> facebook::velox::exec::BlockingReason {
    if (output) {
      std::cout << output->toString() << std::endl;
      std::cout << output->toString(0, 10) << std::endl;
    }
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  };

  // testing the groupby value extraction with a consumer
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(1));

  auto group_by_consumer_task = std::make_shared<facebook::velox::exec::Task>(
      "group_by_consumer_task", group_by_fragment, 0,
      std::make_shared<facebook::velox::core::QueryCtx>(executor.get()), consumer);

  facebook::velox::exec::Task::start(group_by_consumer_task, 1);

  while (group_by_consumer_task->isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

TEST_F(SumTest, sum0Null) {
  auto a_vec = makeFlatVector<int32_t>({14, 11, 11, 10, 14, 13, 11, 12});
  auto b_vec = makeFlatVector<int32_t>({NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL});
  auto c_vec = makeFlatVector<StringView>({StringView("1"), StringView("2"), StringView("2"), StringView("3"), 
    StringView("1"), StringView("4"), StringView("2"), StringView("5")});

  auto row_vecs = makeRowVector({"c0", "c1", "c2"}, {a_vec, b_vec, c_vec});

  auto group_by_fragment = PlanBuilder()
           .values({row_vecs})
           .singleAggregation(
               {"c0"}, {"sum(c1)"})
           .planFragment();

  facebook::velox::exec::Consumer consumer =
      [&](facebook::velox::RowVectorPtr output,
          facebook::velox::ContinueFuture*) -> facebook::velox::exec::BlockingReason {
    if (output) {
      std::cout << output->toString() << std::endl;
      std::cout << output->toString(0, 10) << std::endl;
    }
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  };

  // testing the groupby value extraction with a consumer
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(1));

  auto group_by_consumer_task = std::make_shared<facebook::velox::exec::Task>(
      "group_by_consumer_task", group_by_fragment, 0,
      std::make_shared<facebook::velox::core::QueryCtx>(executor.get()), consumer);

  facebook::velox::exec::Task::start(group_by_consumer_task, 1);

  while (group_by_consumer_task->isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

TEST_F(SumTest, sum0Empty) { // this doesn't work
  auto a_vec = makeFlatVector<int32_t>({14, 11, 11, 10, 14, 13, 11, 12});
  auto b_vec = makeFlatVector<int32_t>({});
  auto c_vec = makeFlatVector<StringView>({StringView("1"), StringView("2"), StringView("2"), StringView("3"), 
    StringView("1"), StringView("4"), StringView("2"), StringView("5")});

  auto row_vecs = makeRowVector({"c0", "c1", "c2"}, {a_vec, b_vec, c_vec});

  auto group_by_fragment = PlanBuilder()
           .values({row_vecs})
           .singleAggregation(
               {"c0"}, {"sum(c1)"})
           .planFragment();

  facebook::velox::exec::Consumer consumer =
      [&](facebook::velox::RowVectorPtr output,
          facebook::velox::ContinueFuture*) -> facebook::velox::exec::BlockingReason {
    if (output) {
      std::cout << output->toString() << std::endl;
      std::cout << output->toString(0, 10) << std::endl;
    }
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  };

  // testing the groupby value extraction with a consumer
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(1));

  auto group_by_consumer_task = std::make_shared<facebook::velox::exec::Task>(
      "group_by_consumer_task", group_by_fragment, 0,
      std::make_shared<facebook::velox::core::QueryCtx>(executor.get()), consumer);

  facebook::velox::exec::Task::start(group_by_consumer_task, 1);

  while (group_by_consumer_task->isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

TEST_F(SumTest, sum0NullKey) {
  auto a_vec = makeFlatVector<int32_t>({NULL, 11, 11, 10, NULL, 13, 11, NULL});
  auto b_vec = makeFlatVector<int32_t>({1, NULL, 2, NULL, 2, NULL, NULL, 3});
  auto c_vec = makeFlatVector<StringView>({StringView("1"), StringView("2"), StringView("2"), StringView("3"), 
    StringView("1"), StringView("4"), StringView("2"), StringView("5")});

  auto row_vecs = makeRowVector({"c0", "c1", "c2"}, {a_vec, b_vec, c_vec});

  auto group_by_fragment = PlanBuilder()
           .values({row_vecs})
           .singleAggregation(
               {"c0"}, {"sum(c1)"})
           .planFragment();

  facebook::velox::exec::Consumer consumer =
      [&](facebook::velox::RowVectorPtr output,
          facebook::velox::ContinueFuture*) -> facebook::velox::exec::BlockingReason {
    if (output) {
      std::cout << output->toString() << std::endl;
      std::cout << output->toString(0, 10) << std::endl;
    }
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  };

  // testing the groupby value extraction with a consumer
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(1));

  auto group_by_consumer_task = std::make_shared<facebook::velox::exec::Task>(
      "group_by_consumer_task", group_by_fragment, 0,
      std::make_shared<facebook::velox::core::QueryCtx>(executor.get()), consumer);

  facebook::velox::exec::Task::start(group_by_consumer_task, 1);

  while (group_by_consumer_task->isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

TEST_F(SumTest, sum0NullIgnoreNullKeys) {
  auto a_vec = makeFlatVector<int32_t>({14, 11, 11, 10, 14, 13, 11, 12});
  auto b_vec = makeFlatVector<int32_t>({NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL});
  auto c_vec = makeFlatVector<StringView>({StringView("1"), StringView("2"), StringView("2"), StringView("3"), 
    StringView("1"), StringView("4"), StringView("2"), StringView("5")});

  auto row_vecs = makeRowVector({"c0", "c1", "c2"}, {a_vec, b_vec, c_vec});

  auto group_by_fragment = PlanBuilder()
           .values({row_vecs})
           .aggregation(
               {"c0"}, {"sum(c1)"}, {}, facebook::velox::core::AggregationNode::Step::kSingle, true)
           .planFragment();

  facebook::velox::exec::Consumer consumer =
      [&](facebook::velox::RowVectorPtr output,
          facebook::velox::ContinueFuture*) -> facebook::velox::exec::BlockingReason {
    if (output) {
      std::cout << output->toString() << std::endl;
      std::cout << output->toString(0, 10) << std::endl;
    }
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  };

  // testing the groupby value extraction with a consumer
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(1));

  auto group_by_consumer_task = std::make_shared<facebook::velox::exec::Task>(
      "group_by_consumer_task", group_by_fragment, 0,
      std::make_shared<facebook::velox::core::QueryCtx>(executor.get()), consumer);

  facebook::velox::exec::Task::start(group_by_consumer_task, 1);

  while (group_by_consumer_task->isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

TEST_F(SumTest, sum0EmptyNoGroupByIgnoreNullKeys) {
  auto a_vec = makeFlatVector<int32_t>({});
  auto b_vec = makeFlatVector<int32_t>({});
  auto c_vec = makeFlatVector<StringView>({});

  auto row_vecs = makeRowVector({"c0", "c1", "c2"}, {a_vec, b_vec, c_vec});

  auto group_by_fragment = PlanBuilder()
           .values({row_vecs})
           .aggregation(
               {}, {"sum(c1)"}, {}, facebook::velox::core::AggregationNode::Step::kSingle, true)
           .planFragment();

  facebook::velox::exec::Consumer consumer =
      [&](facebook::velox::RowVectorPtr output,
          facebook::velox::ContinueFuture*) -> facebook::velox::exec::BlockingReason {
    if (output) {
      std::cout << output->toString() << std::endl;
      std::cout << output->toString(0, 10) << std::endl;
    }
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  };

  // testing the groupby value extraction with a consumer
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(1));

  auto group_by_consumer_task = std::make_shared<facebook::velox::exec::Task>(
      "group_by_consumer_task", group_by_fragment, 0,
      std::make_shared<facebook::velox::core::QueryCtx>(executor.get()), consumer);

  facebook::velox::exec::Task::start(group_by_consumer_task, 1);

  while (group_by_consumer_task->isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

TEST_F(SumTest, sum0EmptyNoGroupBy) {
  auto a_vec = makeFlatVector<int32_t>({});
  auto b_vec = makeFlatVector<int32_t>({});
  auto c_vec = makeFlatVector<StringView>({});

  auto row_vecs = makeRowVector({"c0", "c1", "c2"}, {a_vec, b_vec, c_vec});

  auto group_by_fragment = PlanBuilder()
           .values({row_vecs})
           .aggregation(
               {}, {"sum(c1)"}, {}, facebook::velox::core::AggregationNode::Step::kSingle, false)
           .planFragment();

  facebook::velox::exec::Consumer consumer =
      [&](facebook::velox::RowVectorPtr output,
          facebook::velox::ContinueFuture*) -> facebook::velox::exec::BlockingReason {
    if (output) {
      std::cout << output->toString() << std::endl;
      std::cout << output->toString(0, 10) << std::endl;
    }
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  };

  // testing the groupby value extraction with a consumer
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(1));

  auto group_by_consumer_task = std::make_shared<facebook::velox::exec::Task>(
      "group_by_consumer_task", group_by_fragment, 0,
      std::make_shared<facebook::velox::core::QueryCtx>(executor.get()), consumer);

  facebook::velox::exec::Task::start(group_by_consumer_task, 1);

  while (group_by_consumer_task->isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

TEST_F(SumTest, sum0NullKeyIgnoreNullKeys) {
  auto a_vec = makeFlatVector<int32_t>({NULL, 11, 11, 10, NULL, 13, 11, NULL});
  auto b_vec = makeFlatVector<int32_t>({1, NULL, 2, NULL, 2, NULL, NULL, 3});
  auto c_vec = makeFlatVector<StringView>({StringView("1"), StringView("2"), StringView("2"), StringView("3"), 
    StringView("1"), StringView("4"), StringView("2"), StringView("5")});

  auto row_vecs = makeRowVector({"c0", "c1", "c2"}, {a_vec, b_vec, c_vec});

  auto group_by_fragment = PlanBuilder()
           .values({row_vecs})
           .aggregation(
               {"c0"}, {"sum(c1)"}, {}, facebook::velox::core::AggregationNode::Step::kSingle, true)
           .planFragment();

  facebook::velox::exec::Consumer consumer =
      [&](facebook::velox::RowVectorPtr output,
          facebook::velox::ContinueFuture*) -> facebook::velox::exec::BlockingReason {
    if (output) {
      std::cout << output->toString() << std::endl;
      std::cout << output->toString(0, 10) << std::endl;
    }
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  };

  // testing the groupby value extraction with a consumer
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(1));

  auto group_by_consumer_task = std::make_shared<facebook::velox::exec::Task>(
      "group_by_consumer_task", group_by_fragment, 0,
      std::make_shared<facebook::velox::core::QueryCtx>(executor.get()), consumer);

  facebook::velox::exec::Task::start(group_by_consumer_task, 1);

  while (group_by_consumer_task->isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

TEST_F(SumTest, sum0NullKeyIgnoreNullKeysWithJoin) {
  auto a_vec_l = makeFlatVector<int32_t>({NULL, 11, 11, 10, NULL, 13, 11, NULL});
  auto b_vec_l = makeFlatVector<int32_t>({1, NULL, 2, NULL, 2, NULL, NULL, 3});
  auto c_vec_l = makeFlatVector<StringView>({StringView("1"), StringView("2"), StringView("2"), StringView("3"), 
    StringView("1"), StringView("4"), StringView("2"), StringView("5")});

  auto a_vec_r = makeFlatVector<int32_t>({NULL, 11, 11, 10, NULL, 13, 11, NULL});
  auto b_vec_r = makeFlatVector<int32_t>({10, NULL, 20, NULL, 30, NULL, NULL, 40});
  auto c_vec_r = makeFlatVector<StringView>({StringView("1"), StringView("2"), StringView("2"), StringView("3"), 
    StringView("1"), StringView("4"), StringView("2"), StringView("5")});

  auto row_vecs_l = makeRowVector({"c0_l", "c1_l", "c2_l"}, {a_vec_l, b_vec_l, c_vec_l});
  auto row_vecs_r = makeRowVector({"c0_r", "c1_r", "c2_r"}, {a_vec_r, b_vec_r, c_vec_r});

  auto lg = std::make_shared<facebook::velox::core::PlanNodeIdGenerator>();
  core::PlanNodeId lId;
  core::PlanNodeId rId;
  auto group_by_fragment = PlanBuilder(lg)
           .values({row_vecs_l})
           .capturePlanNodeId(lId)
           .aggregation(
               {"c0_l"}, {"sum(c1_l)"}, {}, facebook::velox::core::AggregationNode::Step::kSingle, true)
           .hashJoin(
             {"c0_l"},
             {"c0_r"},
             PlanBuilder(lg)
                 .values({row_vecs_r})
                 .capturePlanNodeId(rId)
                 .planNode(), 
            "",
            {"c0_l", "a0", "c0_r", "c1_r", "c2_r"})
           .planFragment();

  facebook::velox::exec::Consumer consumer =
      [&](facebook::velox::RowVectorPtr output,
          facebook::velox::ContinueFuture*) -> facebook::velox::exec::BlockingReason {
    if (output) {
      std::cout << output->toString() << std::endl;
      std::cout << output->toString(0, 10) << std::endl;
    }
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  };

  // testing the groupby value extraction with a consumer
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(1));

  auto group_by_consumer_task = std::make_shared<facebook::velox::exec::Task>(
      "group_by_consumer_task", group_by_fragment, 0,
      std::make_shared<facebook::velox::core::QueryCtx>(executor.get()), consumer);

  facebook::velox::exec::Task::start(group_by_consumer_task, 1);

  while (group_by_consumer_task->isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

TEST_F(SumTest, sum0NullKeyWithJoin) {
  auto a_vec_l = makeFlatVector<int32_t>({NULL, 11, 11, 10, NULL, 13, 11, NULL});
  auto b_vec_l = makeFlatVector<int32_t>({1, NULL, 2, NULL, 2, NULL, NULL, 3});
  auto c_vec_l = makeFlatVector<StringView>({StringView("1"), StringView("2"), StringView("2"), StringView("3"), 
    StringView("1"), StringView("4"), StringView("2"), StringView("5")});

  auto a_vec_r = makeFlatVector<int32_t>({NULL, 11, 11, 10, NULL, 13, 11, NULL});
  auto b_vec_r = makeFlatVector<int32_t>({10, NULL, 20, NULL, 30, NULL, NULL, 40});
  auto c_vec_r = makeFlatVector<StringView>({StringView("1"), StringView("2"), StringView("2"), StringView("3"), 
    StringView("1"), StringView("4"), StringView("2"), StringView("5")});

  auto row_vecs_l = makeRowVector({"c0_l", "c1_l", "c2_l"}, {a_vec_l, b_vec_l, c_vec_l});
  auto row_vecs_r = makeRowVector({"c0_r", "c1_r", "c2_r"}, {a_vec_r, b_vec_r, c_vec_r});

  auto lg = std::make_shared<facebook::velox::core::PlanNodeIdGenerator>();
  core::PlanNodeId lId;
  core::PlanNodeId rId;
  auto group_by_fragment = PlanBuilder(lg)
           .values({row_vecs_l})
           .capturePlanNodeId(lId)
           .aggregation(
               {"c0_l"}, {"sum(c1_l)"}, {}, facebook::velox::core::AggregationNode::Step::kSingle, false)
           .hashJoin(
             {"c0_l"},
             {"c0_r"},
             PlanBuilder(lg)
                 .values({row_vecs_r})
                 .capturePlanNodeId(rId)
                 .planNode(), 
            "",
            {"c0_l", "a0", "c0_r", "c1_r", "c2_r"})
           .planFragment();

  facebook::velox::exec::Consumer consumer =
      [&](facebook::velox::RowVectorPtr output,
          facebook::velox::ContinueFuture*) -> facebook::velox::exec::BlockingReason {
    if (output) {
      std::cout << output->toString() << std::endl;
      std::cout << output->toString(0, 10) << std::endl;
    }
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  };

  // testing the groupby value extraction with a consumer
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(1));

  auto group_by_consumer_task = std::make_shared<facebook::velox::exec::Task>(
      "group_by_consumer_task", group_by_fragment, 0,
      std::make_shared<facebook::velox::core::QueryCtx>(executor.get()), consumer);

  facebook::velox::exec::Task::start(group_by_consumer_task, 1);

  while (group_by_consumer_task->isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

} // namespace
} // namespace facebook::velox::aggregate::test

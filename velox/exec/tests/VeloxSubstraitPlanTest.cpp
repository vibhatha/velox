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

#include <folly/Random.h>


#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/vector/tests/utils/VectorMaker.h"

#include <folly/init/Init.h>
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/core/Expressions.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::substrait;


class VeloxSubstraitDemo : public VectorTestBase {
 public:
  const std::string kTpchConnectorId = "test-tpch";

  VeloxSubstraitDemo() {
    // Register Presto scalar functions.
    functions::prestosql::registerAllScalarFunctions();

    // Register Presto aggregate functions.
    aggregate::prestosql::registerAllAggregateFunctions();

    // Register type resolver with DuckDB SQL parser.
    parse::registerTypeResolver();
  }

  /// Run the demo.
  void run();

  std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};
  std::shared_ptr<core::QueryCtx> queryCtx_{
      std::make_shared<core::QueryCtx>(executor_.get())};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
};

void VeloxSubstraitDemo::run() {
  // Let’s create two vectors of 64-bit integers and one vector of strings.
  auto a = makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6});
  auto b = makeFlatVector<int64_t>({0, 5, 10, 15, 20, 25, 30});
  auto dow = makeFlatVector<std::string>(
      {"monday",
       "tuesday",
       "wednesday",
       "thursday",
       "friday",
       "saturday",
       "sunday"});

  auto data = makeRowVector({"a", "b", "dow"}, {a, b, dow});

  // We can also filter rows that have even values of 'a'.
  auto plan = PlanBuilder().values({data}).project({"a + b"}).planNode();

  std::shared_ptr<VeloxToSubstraitPlanConvertor> veloxConvertor =
      std::make_shared<VeloxToSubstraitPlanConvertor>();

  google::protobuf::Arena arena;
  auto substraitPlan = veloxConvertor->toSubstrait(arena, plan);


  std::shared_ptr<SubstraitVeloxPlanConverter> substraitConverter =
      std::make_shared<SubstraitVeloxPlanConverter>(pool());

  auto rd_plan = substraitConverter->toVeloxPlan(substraitPlan);

  // plan output
  auto evenA = AssertQueryBuilder(plan).copyResults(pool());

  std::cout << std::endl
            << "> 1 : " << evenA->toString()
            << std::endl;
  std::cout << evenA->toString(0, evenA->size()) << std::endl;

  // rd_plan output

  auto rd_evenA = AssertQueryBuilder(rd_plan).copyResults(pool());

  std::cout << std::endl
            << "> 2 : " << rd_evenA->toString()
            << std::endl;
  std::cout << rd_evenA->toString(0, rd_evenA->size()) << std::endl;

}

int main(int argc, char** argv) {
  folly::init(&argc, &argv, false);

  VeloxSubstraitDemo demo;
  demo.run();
}

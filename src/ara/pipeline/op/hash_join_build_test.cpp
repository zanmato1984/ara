#include "hash_join_build.h"
#include "hash_join.h"

#include <ara/pipeline/pipeline_context.h>
#include <ara/pipeline/pipeline_observer.h>
#include <ara/schedule/naive_parallel_scheduler.h>
#include <ara/task/task_status.h>

#include <arrow/acero/query_context.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

using namespace ara;
using namespace ara::task;
using namespace ara::pipeline;
using namespace ara::schedule;

class HashJoinBuildTest : public testing::Test {
 protected:
  std::unique_ptr<arrow::acero::QueryContext> query_ctx_;

  void SetUp() override {
    query_ctx_ = std::make_unique<arrow::acero::QueryContext>(
        arrow::acero::QueryOptions{}, arrow::compute::ExecContext());
  }
};

TEST_F(HashJoinBuildTest, Basic) {}

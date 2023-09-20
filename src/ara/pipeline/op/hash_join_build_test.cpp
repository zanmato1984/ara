#include "hash_join_build.h"
#include "hash_join.h"

#include <ara/common/query_context.h>
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

using ara::pipeline::detail::HashJoin;

class HashJoinBuildTest : public testing::Test {
 protected:
  std::unique_ptr<arrow::acero::QueryContext> arrow_query_ctx_;
  QueryContext query_ctx_{Options{8, 8, 4}};
  PipelineContext pipeline_ctx_{&query_ctx_};
  HashJoin hash_join_;

  void SetUp() override {
    arrow_query_ctx_ = std::make_unique<arrow::acero::QueryContext>(
        arrow::acero::QueryOptions{}, arrow::compute::ExecContext());
  }

  void Init(size_t dop, arrow::acero::QueryContext* ctx,
            const arrow::acero::HashJoinNodeOptions& options,
            const arrow::Schema& left_schema, const arrow::Schema& right_schema) {
    ASSERT_OK(hash_join_.Init(pipeline_ctx_, dop, arrow_query_ctx_.get(), options,
                              left_schema, right_schema));
  }
};

TEST_F(HashJoinBuildTest, Basic) {}

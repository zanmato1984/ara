#include "single_batch_parallel_source.h"

#include <ara/common/query_context.h>
#include <ara/pipeline/pipeline_context.h>
#include <ara/pipeline/pipeline_observer.h>

#include <arrow/acero/test_util_internal.h>
#include <gtest/gtest.h>

using namespace ara;
using namespace ara::pipeline;
using namespace ara::pipeline::detail;

class SingleBatchParallelSourceTest
    : public testing::TestWithParam<std::tuple<size_t, size_t>> {
 protected:
  void SetUp() override {
    auto [dop, batch_size] = GetParam();
    ASSERT_OK(source_.Init(pipeline_ctx_, dop, nullptr));
  }

 private:
  QueryContext query_ctx_;
  PipelineContext pipeline_ctx_{&query_ctx_};
  SingleBatchParallelSource source_{"TestSource", "For test"};
};

TEST(SingleBatchParallelSourceTest, Empty) {}

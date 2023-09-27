#include "single_batch_parallel_source.h"
#include "op_output.h"

#include <ara/common/query_context.h>
#include <ara/pipeline/pipeline_context.h>
#include <ara/pipeline/pipeline_observer.h>

#include <arrow/acero/test_util_internal.h>
#include <arrow/testing/builder.h>
#include <gtest/gtest.h>

using namespace ara;
using namespace ara::pipeline;
using namespace ara::pipeline::detail;
using namespace ara::task;

class SingleBatchParallelSourceTest
    : public testing::TestWithParam<std::tuple<size_t, size_t, size_t>> {
 protected:
  void SetUp() override {
    auto [dop, batch_size, per] = GetParam();

    query_ctx_.options.source_max_batch_length = per;

    {
      std::shared_ptr<arrow::Array> array;
      std::vector<int64_t> data;
      for (int64_t i = 0; i < batch_size; ++i) {
        data.push_back(i + 42);
      }
      arrow::ArrayFromVector<arrow::Int64Type>(arrow::int64(), data, &array);
      std::vector<arrow::Datum> values{array};
      auto result = Batch::Make(values);
      ASSERT_OK(result);
      auto batch = std::make_shared<Batch>(result.MoveValueUnsafe());
      ASSERT_OK(source_.Init(pipeline_ctx_, dop, std::move(batch)));
    }
  }

  OpResult Source(ThreadId thread_id) {
    return source_.Source(pipeline_ctx_)(pipeline_ctx_, TaskContext{}, thread_id);
  }

 private:
  QueryContext query_ctx_;
  PipelineContext pipeline_ctx_{&query_ctx_};
  SingleBatchParallelSource source_{"TestSource", "For test"};
};

TEST_P(SingleBatchParallelSourceTest, Basic) {
  auto [dop, batch_size, per] = GetParam();
  std::vector<int64_t> values;
  for (size_t thread_id = 0; thread_id < dop; ++thread_id) {
    auto result = Source(thread_id);
    ASSERT_OK(result);
    while (result->IsSourcePipeHasMore()) {
      auto batch = result->GetBatch();
      ASSERT_TRUE(batch.has_value());
      ASSERT_GT(batch.value().length, 0);
      ASSERT_EQ(batch.value().values.size(), 1);
      ASSERT_TRUE(batch.value().values[0].is_array());
      auto array = batch.value().values[0].array();
      auto* data = reinterpret_cast<const int64_t*>(array->buffers[1]->data());
      for (size_t i = 0; i < batch.value().length; ++i) {
        values.push_back(data[i]);
      }
      result = Source(thread_id);
      ASSERT_OK(result);
    }
    ASSERT_TRUE(result->IsFinished());
    if (auto batch = result->GetBatch(); batch.has_value()) {
      ASSERT_GT(batch.value().length, 0);
      ASSERT_EQ(batch.value().values.size(), 1);
      ASSERT_TRUE(batch.value().values[0].is_array());
      auto array = batch.value().values[0].array();
      auto* data = reinterpret_cast<const int64_t*>(array->buffers[1]->data());
      for (size_t i = 0; i < batch.value().length; ++i) {
        values.push_back(data[i]);
      }
    }
  }
  ASSERT_EQ(values.size(), batch_size);
  for (size_t i = 0; i < batch_size; ++i) {
    ASSERT_EQ(values[i], i + 42);
  }
}

INSTANTIATE_TEST_SUITE_P(SingleBatchParallelSourceSuite, SingleBatchParallelSourceTest,
                         testing::Combine(testing::Range(1ul, 4ul),
                                          testing::Range(0ul, 32ul),
                                          testing::Range(1ul, 5ul)),
                         [](const auto& param_info) {
                           std::stringstream ss;
                           ss << param_info.index << "_dop_"
                              << std::get<0>(param_info.param) << "_batch_"
                              << std::get<1>(param_info.param) << "_per_"
                              << std::get<2>(param_info.param);
                           return ss.str();
                         });

#include <arrow/acero/query_context.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/logging.h>
#include <gtest/gtest.h>
#include <future>

#include "arrow/acero/test_util_internal.h"

#define ARRA_DCHECK ARROW_DCHECK
#define ARRA_RETURN_IF ARROW_RETURN_IF
#define ARRA_RETURN_NOT_OK ARROW_RETURN_NOT_OK
#define ARRA_ASSIGN_OR_RAISE ARROW_ASSIGN_OR_RAISE

namespace arra::sketch {

struct TaskStatus {
 private:
  enum class Code {
    CONTINUE,
    BACKPRESSURE,
    YIELD,
    FINISHED,
    CANCELLED,
  } code_;

  TaskStatus(Code code) : code_(code) {}

 public:
  bool IsContinue() { return code_ == Code::CONTINUE; }
  bool IsBackpressure() { return code_ == Code::BACKPRESSURE; }
  bool IsYield() { return code_ == Code::YIELD; }
  bool IsFinished() { return code_ == Code::FINISHED; }
  bool IsCancelled() { return code_ == Code::CANCELLED; }

 public:
  static TaskStatus Continue() { return TaskStatus(Code::CONTINUE); }
  static TaskStatus Backpressure() { return TaskStatus{Code::BACKPRESSURE}; }
  static TaskStatus Yield() { return TaskStatus{Code::YIELD}; }
  static TaskStatus Finished() { return TaskStatus{Code::FINISHED}; }
  static TaskStatus Cancelled() { return TaskStatus{Code::CANCELLED}; }
};

using TaskId = size_t;
using ThreadId = size_t;

using TaskResult = arrow::Result<TaskStatus>;
using Task = std::function<TaskResult(TaskId)>;
using TaskCont = std::function<TaskResult()>;
using TaskGroup = std::tuple<Task, size_t, std::optional<TaskCont>>;
using TaskGroups = std::vector<TaskGroup>;

using Batch = arrow::compute::ExecBatch;

struct OperatorResult {
 private:
  enum class Code {
    SOURCE_NOT_READY,
    PIPE_SINK_NEEDS_MORE,
    PIPE_EVEN,
    SOURCE_PIPE_HAS_MORE,
    SINK_BACKPRESSURE,
    PIPE_YIELD,
    FINISHED,
    CANCELLED,
  } code_;
  std::optional<Batch> output_;

  OperatorResult(Code code, std::optional<Batch> output = std::nullopt)
      : code_(code), output_(std::move(output)) {}

 public:
  bool IsSourceNotReady() { return code_ == Code::SOURCE_NOT_READY; }
  bool IsPipeSinkNeedsMore() { return code_ == Code::PIPE_SINK_NEEDS_MORE; }
  bool IsPipeEven() { return code_ == Code::PIPE_EVEN; }
  bool IsSourcePipeHasMore() { return code_ == Code::SOURCE_PIPE_HAS_MORE; }
  bool IsSinkBackpressure() { return code_ == Code::SINK_BACKPRESSURE; }
  bool IsPipeYield() { return code_ == Code::PIPE_YIELD; }
  bool IsFinished() { return code_ == Code::FINISHED; }
  bool IsCancelled() { return code_ == Code::CANCELLED; }

  std::optional<Batch>& GetOutput() {
    ARRA_DCHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return output_;
  }

 public:
  static OperatorResult SourceNotReady() {
    return OperatorResult(Code::SOURCE_NOT_READY);
  }
  static OperatorResult PipeSinkNeedsMore() {
    return OperatorResult(Code::PIPE_SINK_NEEDS_MORE);
  }
  static OperatorResult PipeEven(Batch output) {
    return OperatorResult(Code::PIPE_EVEN, std::move(output));
  }
  static OperatorResult SourcePipeHasMore(Batch output) {
    return OperatorResult{Code::SOURCE_PIPE_HAS_MORE, std::move(output)};
  }
  static OperatorResult SinkBackpressure() {
    return OperatorResult{Code::SINK_BACKPRESSURE};
  }
  static OperatorResult PipeYield() { return OperatorResult{Code::PIPE_YIELD}; }
  static OperatorResult Finished(std::optional<Batch> output) {
    return OperatorResult{Code::FINISHED, std::move(output)};
  }
  static OperatorResult Cancelled() { return OperatorResult{Code::CANCELLED}; }
};

using PipelineTaskSource = std::function<arrow::Result<OperatorResult>(ThreadId)>;
using PipelineTaskPipe =
    std::function<arrow::Result<OperatorResult>(ThreadId, std::optional<Batch>)>;
using PipelineTaskDrain = std::function<arrow::Result<OperatorResult>(ThreadId)>;
using PipelineTaskSink =
    std::function<arrow::Result<OperatorResult>(ThreadId, std::optional<Batch>)>;

class SinkOp {
 public:
  virtual ~SinkOp() = default;
  virtual PipelineTaskSink Sink() = 0;
  virtual TaskGroups Frontend() = 0;
  virtual TaskGroups Backend() = 0;
};

namespace detail {

using namespace arrow;
using namespace arrow::acero;
using namespace arrow::compute;

class ScalarAggregateSink : public SinkOp {
 public:
  Status Init(QueryContext* ctx, size_t dop, const AggregateNodeOptions& options,
              const std::shared_ptr<Schema>& input_schema) {
    ctx_ = ctx;
    dop_ = dop;
    auto exec_ctx = ctx_->exec_context();
    aggregates_ = options.aggregates;
    target_fieldsets_.resize(aggregates_.size());
    kernels_.resize(aggregates_.size());
    kernel_intypes_.resize(aggregates_.size());
    states_.resize(aggregates_.size());
    FieldVector fields(aggregates_.size());
    for (size_t i = 0; i < aggregates_.size(); ++i) {
      const auto& target_fieldset = aggregates_[i].target;
      for (const auto& target : target_fieldset) {
        ARRA_ASSIGN_OR_RAISE(auto match, FieldRef(target).FindOne(*input_schema));
        target_fieldsets_[i].push_back(match[0]);
      }

      ARRA_ASSIGN_OR_RAISE(
          auto function, exec_ctx->func_registry()->GetFunction(aggregates_[i].function));
      ARROW_RETURN_IF(function->kind() != Function::SCALAR_AGGREGATE,
                      Status::Invalid("The provided function (", aggregates_[i].function,
                                      ") is not an aggregate function"));
      std::vector<TypeHolder> in_types;
      for (const auto& target : target_fieldsets_[i]) {
        in_types.emplace_back(input_schema->field(target)->type().get());
      }
      kernel_intypes_[i] = in_types;
      ARRA_ASSIGN_OR_RAISE(const Kernel* kernel,
                           function->DispatchExact(kernel_intypes_[i]));
      kernels_[i] = static_cast<const ScalarAggregateKernel*>(kernel);

      if (aggregates_[i].options == nullptr) {
        ARRA_DCHECK(!function->doc().options_required);
        const auto* default_options = function->default_options();
        if (default_options) {
          aggregates_[i].options = default_options->Copy();
        }
      }

      KernelContext kernel_ctx{exec_ctx};
      states_[i].resize(dop_);
      RETURN_NOT_OK(Kernel::InitAll(
          &kernel_ctx,
          KernelInitArgs{kernels_[i], kernel_intypes_[i], aggregates_[i].options.get()},
          &states_[i]));

      // pick one to resolve the kernel signature
      kernel_ctx.SetState(states_[i][0].get());
      ARROW_ASSIGN_OR_RAISE(auto out_type, kernels_[i]->signature->out_type().Resolve(
                                               &kernel_ctx, kernel_intypes_[i]));

      fields[i] = field(aggregates_[i].name, out_type.GetSharedPtr());
    }
    output_schema_ = schema(std::move(fields));
    return Status::OK();
  }

  PipelineTaskSink Sink() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> batch) -> arrow::Result<OperatorResult> {
      if (batch == std::nullopt) {
        return OperatorResult::PipeSinkNeedsMore();
      }
      ExecSpan exec_span(batch.value());
      for (size_t i = 0; i < kernels_.size(); ++i) {
        KernelContext batch_ctx{ctx_->exec_context()};
        batch_ctx.SetState(states_[i][thread_id].get());

        std::vector<ExecValue> column_values;
        for (const int field : target_fieldsets_[i]) {
          column_values.push_back(exec_span.values[field]);
        }
        ExecSpan column_batch{std::move(column_values), exec_span.length};
        ARRA_RETURN_NOT_OK(kernels_[i]->consume(&batch_ctx, column_batch));
      }
      return OperatorResult::PipeSinkNeedsMore();
    };
  }

  virtual TaskGroups Frontend() override {
    auto merge_and_finalize = [&](TaskId) -> TaskResult {
      output_data_ = ExecBatch{{}, 1};
      output_data_.values.resize(kernels_.size());
      for (size_t i = 0; i < kernels_.size(); ++i) {
        KernelContext ctx{ctx_->exec_context()};
        ARROW_ASSIGN_OR_RAISE(auto merged, ScalarAggregateKernel::MergeAll(
                                               kernels_[i], &ctx, std::move(states_[i])));
        RETURN_NOT_OK(kernels_[i]->finalize(&ctx, &output_data_.values[i]));
      }
      return TaskStatus::Finished();
    };

    return {{std::move(merge_and_finalize), 1, std::nullopt}};
  }

  virtual TaskGroups Backend() override { return {}; }

  std::shared_ptr<Schema> OutputSchema() const { return output_schema_; }

  ExecBatch GetOutputData() const { return output_data_; }

 private:
  QueryContext* ctx_;
  size_t dop_;

  std::shared_ptr<Schema> output_schema_;
  std::vector<Aggregate> aggregates_;
  std::vector<std::vector<int>> target_fieldsets_;
  std::vector<const ScalarAggregateKernel*> kernels_;
  std::vector<std::vector<TypeHolder>> kernel_intypes_;
  std::vector<std::vector<std::unique_ptr<KernelState>>> states_;

  ExecBatch output_data_;
};

}  // namespace detail

using ScalarAggregateSink = detail::ScalarAggregateSink;

}  // namespace arra::sketch

using namespace arra::sketch;

void AssertBatchesEqual(const arrow::acero::BatchesWithSchema& out,
                        const arrow::acero::BatchesWithSchema& exp) {
  ASSERT_OK_AND_ASSIGN(auto out_table,
                       arrow::acero::TableFromExecBatches(out.schema, out.batches));
  ASSERT_OK_AND_ASSIGN(auto exp_table,
                       arrow::acero::TableFromExecBatches(exp.schema, exp.batches));

  std::vector<arrow::compute::SortKey> sort_keys;
  for (auto&& f : exp.schema->fields()) {
    sort_keys.emplace_back(f->name());
  }
  ASSERT_OK_AND_ASSIGN(
      auto exp_table_sort_ids,
      arrow::compute::SortIndices(exp_table, arrow::compute::SortOptions(sort_keys)));
  ASSERT_OK_AND_ASSIGN(auto exp_table_sorted,
                       arrow::compute::Take(exp_table, exp_table_sort_ids));
  ASSERT_OK_AND_ASSIGN(
      auto out_table_sort_ids,
      arrow::compute::SortIndices(out_table, arrow::compute::SortOptions(sort_keys)));
  ASSERT_OK_AND_ASSIGN(auto out_table_sorted,
                       arrow::compute::Take(out_table, out_table_sort_ids));

  AssertTablesEqual(*exp_table_sorted.table(), *out_table_sorted.table(),
                    /*same_chunk_layout=*/false, /*flatten=*/true);
}

TEST(ScalarAggregate, Basic) {
  size_t dop = 8;
  std::unique_ptr<arrow::acero::QueryContext> query_ctx =
      std::make_unique<arrow::acero::QueryContext>(arrow::acero::QueryOptions{},
                                                   arrow::compute::ExecContext());
  ASSERT_OK(query_ctx->Init(8, nullptr));

  arrow::compute::Aggregate agg("count", nullptr, std::vector<arrow::FieldRef>{{0}},
                                "count");
  arrow::acero::AggregateNodeOptions options({std::move(agg)});
  auto input_schema = arrow::schema({arrow::field("l_i32", arrow::int32())});

  ScalarAggregateSink sink_op;
  ASSERT_OK(sink_op.Init(query_ctx.get(), dop, options, input_schema));
  auto sink = sink_op.Sink();
  std::vector<std::future<arrow::Result<OperatorResult>>> handles;
  for (size_t i = 0; i < dop; ++i) {
    handles.emplace_back(std::async(std::launch::async, [&, i]() {
      auto batch = arrow::acero::ExecBatchFromJSON(
          {arrow::int32()}, "[[1], [2], [3], [4], [5], [6], [7], [8], [9], [10]]");
      return sink(i, std::move(batch));
    }));
  }
  for (auto& handle : handles) {
    auto result = handle.get();
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore());
  }
  auto fe = sink_op.Frontend();
  auto result = std::get<0>(fe[0])(0);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());

  auto act_data = sink_op.GetOutputData();
  auto act_batch =
      arrow::acero::BatchesWithSchema{{std::move(act_data)}, sink_op.OutputSchema()};

  auto exp_schema = arrow::schema({arrow::field("count", arrow::int64())});
  auto exp_data = arrow::acero::ExecBatchFromJSON({arrow::int64()}, "[[80]]");
  auto exp_batch = arrow::acero::BatchesWithSchema{{std::move(exp_data)}, exp_schema};

  AssertBatchesEqual(act_batch, exp_batch);
}

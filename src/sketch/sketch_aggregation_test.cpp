#include <arrow/acero/query_context.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/logging.h>
#include <gtest/gtest.h>
#include <future>

#include "arrow/acero/test_util_internal.h"

#define ARA_DCHECK ARROW_DCHECK
#define ARA_RETURN_IF ARROW_RETURN_IF
#define ARA_RETURN_NOT_OK ARROW_RETURN_NOT_OK
#define ARA_ASSIGN_OR_RAISE ARROW_ASSIGN_OR_RAISE

namespace ara::sketch {

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
    ARA_DCHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
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
        ARA_ASSIGN_OR_RAISE(auto match, FieldRef(target).FindOne(*input_schema));
        target_fieldsets_[i].push_back(match[0]);
      }

      ARA_ASSIGN_OR_RAISE(
          auto function, exec_ctx->func_registry()->GetFunction(aggregates_[i].function));
      ARROW_RETURN_IF(function->kind() != Function::SCALAR_AGGREGATE,
                      Status::Invalid("The provided function (", aggregates_[i].function,
                                      ") is not an aggregate function"));
      std::vector<TypeHolder> in_types;
      for (const auto& target : target_fieldsets_[i]) {
        in_types.emplace_back(input_schema->field(target)->type().get());
      }
      kernel_intypes_[i] = in_types;
      ARA_ASSIGN_OR_RAISE(const Kernel* kernel,
                           function->DispatchExact(kernel_intypes_[i]));
      kernels_[i] = static_cast<const ScalarAggregateKernel*>(kernel);

      if (aggregates_[i].options == nullptr) {
        ARA_DCHECK(!function->doc().options_required);
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
        ARA_RETURN_NOT_OK(kernels_[i]->consume(&batch_ctx, column_batch));
      }
      return OperatorResult::PipeSinkNeedsMore();
    };
  }

  TaskGroups Frontend() override {
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

  TaskGroups Backend() override { return {}; }

  std::shared_ptr<Schema> OutputSchema() const { return output_schema_; }

  ExecBatch& GetOutputData() { return output_data_; }

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

std::vector<TypeHolder> ExtendWithGroupIdType(const std::vector<TypeHolder>& in_types) {
  std::vector<TypeHolder> aggr_in_types;
  aggr_in_types.reserve(in_types.size() + 1);
  aggr_in_types = in_types;
  aggr_in_types.emplace_back(uint32());
  return aggr_in_types;
}

Result<const HashAggregateKernel*> GetKernel(ExecContext* ctx, const Aggregate& aggregate,
                                             const std::vector<TypeHolder>& in_types) {
  const auto aggr_in_types = ExtendWithGroupIdType(in_types);
  ARROW_ASSIGN_OR_RAISE(auto function,
                        ctx->func_registry()->GetFunction(aggregate.function));
  if (function->kind() != Function::HASH_AGGREGATE) {
    if (function->kind() == Function::SCALAR_AGGREGATE) {
      return Status::Invalid("The provided function (", aggregate.function,
                             ") is a scalar aggregate function.  Since there are "
                             "keys to group by, a hash aggregate function was "
                             "expected (normally these start with hash_)");
    }
    return Status::Invalid("The provided function(", aggregate.function,
                           ") is not an aggregate function");
  }
  ARROW_ASSIGN_OR_RAISE(const Kernel* kernel, function->DispatchExact(aggr_in_types));
  return static_cast<const HashAggregateKernel*>(kernel);
}

Result<std::unique_ptr<KernelState>> InitKernel(const HashAggregateKernel* kernel,
                                                ExecContext* ctx,
                                                const Aggregate& aggregate,
                                                const std::vector<TypeHolder>& in_types) {
  const auto aggr_in_types = ExtendWithGroupIdType(in_types);

  KernelContext kernel_ctx{ctx};
  const auto* options =
      arrow::internal::checked_cast<const FunctionOptions*>(aggregate.options.get());
  if (options == nullptr) {
    // use known default options for the named function if possible
    auto maybe_function = ctx->func_registry()->GetFunction(aggregate.function);
    if (maybe_function.ok()) {
      options = maybe_function.ValueOrDie()->default_options();
    }
  }

  ARROW_ASSIGN_OR_RAISE(
      auto state,
      kernel->init(&kernel_ctx, KernelInitArgs{kernel, aggr_in_types, options}));
  return std::move(state);
}

Result<std::vector<const HashAggregateKernel*>> GetKernels(
    ExecContext* ctx, const std::vector<Aggregate>& aggregates,
    const std::vector<std::vector<TypeHolder>>& in_types) {
  if (aggregates.size() != in_types.size()) {
    return Status::Invalid(aggregates.size(), " aggregate functions were specified but ",
                           in_types.size(), " arguments were provided.");
  }

  std::vector<const HashAggregateKernel*> kernels(in_types.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(kernels[i], GetKernel(ctx, aggregates[i], in_types[i]));
  }
  return kernels;
}

Result<std::vector<std::unique_ptr<KernelState>>> InitKernels(
    const std::vector<const HashAggregateKernel*>& kernels, ExecContext* ctx,
    const std::vector<Aggregate>& aggregates,
    const std::vector<std::vector<TypeHolder>>& in_types) {
  std::vector<std::unique_ptr<KernelState>> states(kernels.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(states[i],
                          InitKernel(kernels[i], ctx, aggregates[i], in_types[i]));
  }
  return std::move(states);
}

Result<FieldVector> ResolveKernels(
    const std::vector<Aggregate>& aggregates,
    const std::vector<const HashAggregateKernel*>& kernels,
    const std::vector<std::unique_ptr<KernelState>>& states, ExecContext* ctx,
    const std::vector<std::vector<TypeHolder>>& types) {
  FieldVector fields(types.size());

  for (size_t i = 0; i < kernels.size(); ++i) {
    KernelContext kernel_ctx{ctx};
    kernel_ctx.SetState(states[i].get());

    const auto aggr_in_types = ExtendWithGroupIdType(types[i]);
    ARROW_ASSIGN_OR_RAISE(
        auto type, kernels[i]->signature->out_type().Resolve(&kernel_ctx, aggr_in_types));
    fields[i] = field(aggregates[i].function, type.GetSharedPtr());
  }
  return fields;
}

class HashAggregateSink : public SinkOp {
 public:
  Status Init(QueryContext* ctx, size_t dop, const AggregateNodeOptions& options,
              const std::shared_ptr<Schema>& input_schema) {
    ctx_ = ctx;
    dop_ = dop;
    auto exec_ctx = ctx_->exec_context();
    aggs_ = options.aggregates;

    key_field_ids_.resize(aggs_.size());
    for (size_t i = 0; i < options.keys.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto match, options.keys[i].FindOne(*input_schema));
      key_field_ids_[i] = match[0];
    }

    std::vector<TypeHolder> key_types(key_field_ids_.size());
    for (size_t i = 0; i < key_field_ids_.size(); ++i) {
      auto key_field_id = key_field_ids_[i];
      key_types[i] = input_schema->field(key_field_id)->type().get();
    }

    agg_src_fieldsets_.resize(aggs_.size());
    for (size_t i = 0; i < aggs_.size(); ++i) {
      const auto& target_fieldset = aggs_[i].target;
      for (const auto& target : target_fieldset) {
        ARROW_ASSIGN_OR_RAISE(auto match, target.FindOne(*input_schema));
        agg_src_fieldsets_[i].push_back(match[0]);
      }
    }

    agg_src_types_.resize(aggs_.size());
    for (size_t i = 0; i < aggs_.size(); ++i) {
      for (const auto& agg_src_field_id : agg_src_fieldsets_[i]) {
        agg_src_types_[i].push_back(input_schema->field(agg_src_field_id)->type().get());
      }
    }

    // Construct aggregates
    ARA_ASSIGN_OR_RAISE(agg_kernels_, GetKernels(exec_ctx, aggs_, agg_src_types_));

    ARA_ASSIGN_OR_RAISE(auto agg_states,
                         InitKernels(agg_kernels_, exec_ctx, aggs_, agg_src_types_));

    ARA_ASSIGN_OR_RAISE(
        FieldVector agg_result_fields,
        ResolveKernels(aggs_, agg_kernels_, agg_states, exec_ctx, agg_src_types_));

    // Build field vector for output schema
    FieldVector output_fields{options.keys.size() + aggs_.size()};

    // First output is keys, followed by segment_keys, followed by aggregates themselves
    // This matches the behavior described by Substrait and also tends to be the behavior
    // in SQL engines
    for (size_t i = 0; i < options.keys.size(); ++i) {
      int key_field_id = key_field_ids_[i];
      output_fields[i] = input_schema->field(key_field_id);
    }
    size_t base = options.keys.size();
    for (size_t i = 0; i < aggs_.size(); ++i) {
      output_fields[base + i] = agg_result_fields[i]->WithName(aggs_[i].name);
    }

    output_schema_ = schema(std::move(output_fields));

    local_states_.resize(dop_);
    for (size_t i = 0; i < dop_; ++i) {
      ARA_ASSIGN_OR_RAISE(local_states_[i].grouper, Grouper::Make(key_types, exec_ctx));
      ARA_ASSIGN_OR_RAISE(local_states_[i].agg_states,
                           InitKernels(agg_kernels_, exec_ctx, aggs_, agg_src_types_));
    }
    return Status::OK();
  }

  PipelineTaskSink Sink() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> batch) -> arrow::Result<OperatorResult> {
      if (batch == std::nullopt) {
        return OperatorResult::PipeSinkNeedsMore();
      }
      ExecSpan exec_span(batch.value());
      ARA_RETURN_NOT_OK(Consume(thread_id, std::move(exec_span)));
      return OperatorResult::PipeSinkNeedsMore();
    };
  }

  TaskGroups Frontend() override {
    auto merge_and_finalize = [&](TaskId) -> TaskResult {
      ARA_RETURN_NOT_OK(Merge());
      ARA_ASSIGN_OR_RAISE(output_data_, Finalize());
      return TaskStatus::Finished();
    };

    return {{std::move(merge_and_finalize), 1, std::nullopt}};
  }

  TaskGroups Backend() override { return {}; }

  std::shared_ptr<Schema> OutputSchema() const { return output_schema_; }

  ExecBatch& GetOutputData() { return output_data_; }

 private:
  Status Consume(ThreadId thread_id, ExecSpan batch) {
    auto state = &local_states_[thread_id];

    // Create a batch with key columns
    std::vector<ExecValue> keys(key_field_ids_.size());
    for (size_t i = 0; i < key_field_ids_.size(); ++i) {
      keys[i] = batch[key_field_ids_[i]];
    }
    ExecSpan key_batch(std::move(keys), batch.length);

    // Create a batch with group ids
    ARROW_ASSIGN_OR_RAISE(Datum id_batch, state->grouper->Consume(key_batch));

    // Execute aggregate kernels
    for (size_t i = 0; i < agg_kernels_.size(); ++i) {
      auto ctx = ctx_->exec_context();
      KernelContext kernel_ctx{ctx};
      kernel_ctx.SetState(state->agg_states[i].get());

      std::vector<ExecValue> column_values;
      for (const int field : agg_src_fieldsets_[i]) {
        column_values.push_back(batch[field]);
      }
      column_values.emplace_back(*id_batch.array());
      ExecSpan agg_batch(std::move(column_values), batch.length);
      RETURN_NOT_OK(agg_kernels_[i]->resize(&kernel_ctx, state->grouper->num_groups()));
      RETURN_NOT_OK(agg_kernels_[i]->consume(&kernel_ctx, agg_batch));
    }

    return Status::OK();
  }

  Status Merge() {
    ThreadLocalState* state0 = &local_states_[0];
    for (size_t i = 1; i < local_states_.size(); ++i) {
      ThreadLocalState* state = &local_states_[i];

      ARROW_ASSIGN_OR_RAISE(ExecBatch other_keys, state->grouper->GetUniques());
      ARROW_ASSIGN_OR_RAISE(Datum transposition,
                            state0->grouper->Consume(ExecSpan(other_keys)));
      state->grouper.reset();

      for (size_t i = 0; i < agg_kernels_.size(); ++i) {
        auto ctx = ctx_->exec_context();
        KernelContext batch_ctx{ctx};
        DCHECK(state0->agg_states[i]);
        batch_ctx.SetState(state0->agg_states[i].get());

        RETURN_NOT_OK(agg_kernels_[i]->resize(&batch_ctx, state0->grouper->num_groups()));
        RETURN_NOT_OK(agg_kernels_[i]->merge(&batch_ctx, std::move(*state->agg_states[i]),
                                             *transposition.array()));
        state->agg_states[i].reset();
      }
    }
    return Status::OK();
  }

  Result<ExecBatch> Finalize() {
    ThreadLocalState* state = &local_states_[0];

    // Allocate a batch for output
    ExecBatch out_data{{}, state->grouper->num_groups()};
    out_data.values.resize(agg_kernels_.size() + key_field_ids_.size());

    // Keys come first
    ARROW_ASSIGN_OR_RAISE(ExecBatch out_keys, state->grouper->GetUniques());
    std::move(out_keys.values.begin(), out_keys.values.end(), out_data.values.begin());
    // And finally, the aggregates themselves
    size_t base = key_field_ids_.size();
    for (size_t i = 0; i < agg_kernels_.size(); ++i) {
      KernelContext batch_ctx{ctx_->exec_context()};
      batch_ctx.SetState(state->agg_states[i].get());
      RETURN_NOT_OK(agg_kernels_[i]->finalize(&batch_ctx, &out_data.values[i + base]));
      state->agg_states[i].reset();
    }
    state->grouper.reset();

    return out_data;
  }

 private:
  QueryContext* ctx_;
  size_t dop_;

  std::shared_ptr<Schema> output_schema_;
  std::vector<int> key_field_ids_;
  std::vector<std::vector<TypeHolder>> agg_src_types_;
  std::vector<std::vector<int>> agg_src_fieldsets_;
  std::vector<Aggregate> aggs_;
  std::vector<const HashAggregateKernel*> agg_kernels_;

  struct ThreadLocalState {
    std::unique_ptr<Grouper> grouper;
    std::vector<std::unique_ptr<KernelState>> agg_states;
  };
  std::vector<ThreadLocalState> local_states_;

  ExecBatch output_data_;
};

}  // namespace detail

using ScalarAggregateSink = detail::ScalarAggregateSink;
using HashAggregateSink = detail::HashAggregateSink;

}  // namespace ara::sketch

using namespace ara::sketch;

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
  auto input_schema = arrow::schema({arrow::field("i32", arrow::int32())});

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

TEST(HashAggregate, Basic) {
  size_t dop = 8;
  std::unique_ptr<arrow::acero::QueryContext> query_ctx =
      std::make_unique<arrow::acero::QueryContext>(arrow::acero::QueryOptions{},
                                                   arrow::compute::ExecContext());
  ASSERT_OK(query_ctx->Init(8, nullptr));

  arrow::compute::Aggregate agg("hash_count", nullptr, std::vector<arrow::FieldRef>{{1}},
                                "count");
  arrow::acero::AggregateNodeOptions options({std::move(agg)}, {{0}});
  auto input_schema = arrow::schema(
      {arrow::field("key_i32", arrow::int32()), arrow::field("agg_i32", arrow::int32())});

  HashAggregateSink sink_op;
  ASSERT_OK(sink_op.Init(query_ctx.get(), dop, options, input_schema));
  auto sink = sink_op.Sink();
  std::vector<std::future<arrow::Result<OperatorResult>>> handles;
  for (size_t i = 0; i < dop; ++i) {
    handles.emplace_back(std::async(std::launch::async, [&, i]() {
      auto batch =
          arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::int32()},
                                          "[[1, 11], [1, 12], [1, 13], [1, 14], [2, 21], "
                                          "[2, 22], [2, 23], [3, 31], [3, 32], [4, 41]]");
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

  auto exp_schema = arrow::schema(
      {arrow::field("key_i32", arrow::int32()), arrow::field("count", arrow::int64())});
  auto exp_data = arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::int64()},
                                                  "[[1, 32], [2, 24], [3, 16], [4, 8]]");
  auto exp_batch = arrow::acero::BatchesWithSchema{{std::move(exp_data)}, exp_schema};

  AssertBatchesEqual(act_batch, exp_batch);
}

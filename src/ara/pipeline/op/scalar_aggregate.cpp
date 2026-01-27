#include "scalar_aggregate.h"
#include "op_output.h"
#include "single_batch_parallel_source.h"

#include <ara/task/task_status.h>
#include <ara/util/util.h>

#include <arrow/acero/query_context.h>
#include <arrow/compute/api.h>

namespace ara::pipeline {

using namespace ara::task;
using namespace ara::util;

using namespace arrow;
using namespace arrow::acero;
using namespace arrow::compute;

Status ScalarAggregate::Init(const PipelineContext&, size_t dop,
                             arrow::acero::QueryContext* ctx,
                             const AggregateNodeOptions& options,
                             const Schema& input_schema) {
  ARA_RETURN_NOT_OK(arrow::compute::Initialize());
  dop_ = dop;
  ctx_ = ctx;
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
      ARA_ASSIGN_OR_RAISE(auto match, FieldRef(target).FindOne(input_schema));
      target_fieldsets_[i].push_back(match[0]);
    }

    ARA_ASSIGN_OR_RAISE(auto function,
                        exec_ctx->func_registry()->GetFunction(aggregates_[i].function));
    ARA_RETURN_IF(function->kind() != Function::SCALAR_AGGREGATE,
                  Status::Invalid("The provided function (", aggregates_[i].function,
                                  ") is not an aggregate function"));
    std::vector<TypeHolder> in_types;
    for (const auto& target : target_fieldsets_[i]) {
      in_types.emplace_back(input_schema.field(target)->type().get());
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
    ARA_RETURN_NOT_OK(Kernel::InitAll(
        &kernel_ctx,
        KernelInitArgs{kernels_[i], kernel_intypes_[i], aggregates_[i].options.get()},
        &states_[i]));

    // pick one to resolve the kernel signature
    kernel_ctx.SetState(states_[i][0].get());
    ARA_ASSIGN_OR_RAISE(auto out_type, kernels_[i]->signature->out_type().Resolve(
                                           &kernel_ctx, kernel_intypes_[i]));

    fields[i] = field(aggregates_[i].name, out_type.GetSharedPtr());
  }
  output_schema_ = schema(std::move(fields));
  return Status::OK();
}

PipelineSink ScalarAggregate::Sink(const PipelineContext&) {
  return [&](const PipelineContext&, const TaskContext&, ThreadId thread_id,
             std::optional<Batch> batch) -> OpResult {
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
    return OpOutput::PipeSinkNeedsMore();
  };
}

TaskGroups ScalarAggregate::Frontend(const PipelineContext&) {
  Task merge_and_finalize(
      "ScalarAggregate::MergeAndFinalize", "",
      [&](const TaskContext&, TaskId) -> TaskResult {
        output_batch_ = std::make_shared<Batch>(std::vector<Datum>{}, 1);
        output_batch_->values.resize(kernels_.size());
        for (size_t i = 0; i < kernels_.size(); ++i) {
          KernelContext ctx{ctx_->exec_context()};
          ARA_ASSIGN_OR_RAISE(auto merged, ScalarAggregateKernel::MergeAll(
                                               kernels_[i], &ctx, std::move(states_[i])));
          ARA_RETURN_NOT_OK(kernels_[i]->finalize(&ctx, &output_batch_->values[i]));
        }
        return TaskStatus::Finished();
      });

  return {{"ScalarAggregate::MergeAndFinalize", "", std::move(merge_and_finalize), 1,
           std::nullopt, nullptr}};
}

std::unique_ptr<SourceOp> ScalarAggregate::ImplicitSource(const PipelineContext& ctx) {
  auto source = std::make_unique<detail::SingleBatchParallelSource>(
      "ScalarAggregate::ImplicitSource", "For ScalarAggregate");
  ARA_CHECK_OK(source->Init(ctx, dop_, output_batch_));
  return source;
}

}  // namespace ara::pipeline

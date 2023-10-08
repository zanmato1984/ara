#include "hash_aggregate.h"
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

namespace detail {

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
  ARA_ASSIGN_OR_RAISE(auto function,
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
  ARA_ASSIGN_OR_RAISE(const Kernel* kernel, function->DispatchExact(aggr_in_types));
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

  ARA_ASSIGN_OR_RAISE(
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
    ARA_ASSIGN_OR_RAISE(kernels[i], GetKernel(ctx, aggregates[i], in_types[i]));
  }
  return kernels;
}

Result<std::vector<std::unique_ptr<KernelState>>> InitKernels(
    const std::vector<const HashAggregateKernel*>& kernels, ExecContext* ctx,
    const std::vector<Aggregate>& aggregates,
    const std::vector<std::vector<TypeHolder>>& in_types) {
  std::vector<std::unique_ptr<KernelState>> states(kernels.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARA_ASSIGN_OR_RAISE(states[i],
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
    ARA_ASSIGN_OR_RAISE(
        auto type, kernels[i]->signature->out_type().Resolve(&kernel_ctx, aggr_in_types));
    fields[i] = field(aggregates[i].function, type.GetSharedPtr());
  }
  return fields;
}

}  // namespace detail

Status HashAggregate::Init(const PipelineContext&, size_t dop,
                           arrow::acero::QueryContext* ctx,
                           const AggregateNodeOptions& options,
                           const Schema& input_schema) {
  dop_ = dop;
  ctx_ = ctx;
  auto exec_ctx = ctx_->exec_context();
  aggs_ = options.aggregates;

  key_field_ids_.resize(aggs_.size());
  for (size_t i = 0; i < options.keys.size(); ++i) {
    ARA_ASSIGN_OR_RAISE(auto match, options.keys[i].FindOne(input_schema));
    key_field_ids_[i] = match[0];
  }

  std::vector<TypeHolder> key_types(key_field_ids_.size());
  for (size_t i = 0; i < key_field_ids_.size(); ++i) {
    auto key_field_id = key_field_ids_[i];
    key_types[i] = input_schema.field(key_field_id)->type().get();
  }

  agg_src_fieldsets_.resize(aggs_.size());
  for (size_t i = 0; i < aggs_.size(); ++i) {
    const auto& target_fieldset = aggs_[i].target;
    for (const auto& target : target_fieldset) {
      ARA_ASSIGN_OR_RAISE(auto match, target.FindOne(input_schema));
      agg_src_fieldsets_[i].push_back(match[0]);
    }
  }

  agg_src_types_.resize(aggs_.size());
  for (size_t i = 0; i < aggs_.size(); ++i) {
    for (const auto& agg_src_field_id : agg_src_fieldsets_[i]) {
      agg_src_types_[i].push_back(input_schema.field(agg_src_field_id)->type().get());
    }
  }

  // Construct aggregates
  ARA_ASSIGN_OR_RAISE(agg_kernels_, detail::GetKernels(exec_ctx, aggs_, agg_src_types_));

  ARA_ASSIGN_OR_RAISE(auto agg_states,
                      detail::InitKernels(agg_kernels_, exec_ctx, aggs_, agg_src_types_));

  ARA_ASSIGN_OR_RAISE(
      FieldVector agg_result_fields,
      detail::ResolveKernels(aggs_, agg_kernels_, agg_states, exec_ctx, agg_src_types_));

  // Build field vector for output schema
  FieldVector output_fields{options.keys.size() + aggs_.size()};

  // First output is keys, followed by segment_keys, followed by aggregates themselves
  // This matches the behavior described by Substrait and also tends to be the behavior
  // in SQL engines
  for (size_t i = 0; i < options.keys.size(); ++i) {
    int key_field_id = key_field_ids_[i];
    output_fields[i] = input_schema.field(key_field_id);
  }
  size_t base = options.keys.size();
  for (size_t i = 0; i < aggs_.size(); ++i) {
    output_fields[base + i] = agg_result_fields[i]->WithName(aggs_[i].name);
  }

  output_schema_ = schema(std::move(output_fields));

  thread_locals_.resize(dop_);
  for (size_t i = 0; i < dop_; ++i) {
    ARA_ASSIGN_OR_RAISE(thread_locals_[i].grouper, Grouper::Make(key_types, exec_ctx));
    ARA_ASSIGN_OR_RAISE(
        thread_locals_[i].agg_states,
        detail::InitKernels(agg_kernels_, exec_ctx, aggs_, agg_src_types_));
  }
  return Status::OK();
}

PipelineSink HashAggregate::Sink(const PipelineContext&) {
  return [&](const PipelineContext&, const TaskContext&, ThreadId thread_id,
             std::optional<Batch> batch) -> OpResult {
    ExecSpan exec_span(batch.value());
    ARA_RETURN_NOT_OK(Consume(thread_id, std::move(exec_span)));
    return OpOutput::PipeSinkNeedsMore();
  };
}

TaskGroups HashAggregate::Frontend(const PipelineContext&) {
  Task merge_and_finalize("ScalarAggregate::MergeAndFinalize", "",
                          [&](const TaskContext&, TaskId) -> TaskResult {
                            ARA_RETURN_NOT_OK(Merge());
                            ARA_RETURN_NOT_OK(Finalize());
                            return TaskStatus::Finished();
                          });

  return {{"ScalarAggregate::MergeAndFinalize", "", std::move(merge_and_finalize), 1,
           std::nullopt, nullptr}};
}

std::unique_ptr<SourceOp> HashAggregate::ImplicitSource(const PipelineContext& ctx) {
  auto source = std::make_unique<detail::SingleBatchParallelSource>(
      "HashAggregate::ImplicitSource", "For HashAggregate");
  ARA_CHECK_OK(source->Init(ctx, dop_, output_batch_));
  return source;
}

Status HashAggregate::Consume(ThreadId thread_id, ExecSpan batch) {
  auto state = &thread_locals_[thread_id];

  // Create a batch with key columns
  std::vector<ExecValue> keys(key_field_ids_.size());
  for (size_t i = 0; i < key_field_ids_.size(); ++i) {
    keys[i] = batch[key_field_ids_[i]];
  }
  ExecSpan key_batch(std::move(keys), batch.length);

  // Create a batch with group ids
  ARA_ASSIGN_OR_RAISE(Datum id_batch, state->grouper->Consume(key_batch));

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

Status HashAggregate::Merge() {
  auto state0 = &thread_locals_[0];
  for (size_t i = 1; i < thread_locals_.size(); ++i) {
    auto state = &thread_locals_[i];

    ARA_ASSIGN_OR_RAISE(Batch other_keys, state->grouper->GetUniques());
    ARA_ASSIGN_OR_RAISE(Datum transposition,
                        state0->grouper->Consume(ExecSpan(other_keys)));
    state->grouper.reset();

    for (size_t i = 0; i < agg_kernels_.size(); ++i) {
      auto ctx = ctx_->exec_context();
      KernelContext batch_ctx{ctx};
      ARA_CHECK(state0->agg_states[i]);
      batch_ctx.SetState(state0->agg_states[i].get());

      RETURN_NOT_OK(agg_kernels_[i]->resize(&batch_ctx, state0->grouper->num_groups()));
      RETURN_NOT_OK(agg_kernels_[i]->merge(&batch_ctx, std::move(*state->agg_states[i]),
                                           *transposition.array()));
      state->agg_states[i].reset();
    }
  }
  return Status::OK();
}

Status HashAggregate::Finalize() {
  auto state = &thread_locals_[0];

  // Allocate a batch for output
  output_batch_ =
      std::make_shared<Batch>(std::vector<Datum>{}, state->grouper->num_groups());
  output_batch_->values.resize(agg_kernels_.size() + key_field_ids_.size());

  // Keys come first
  ARA_ASSIGN_OR_RAISE(Batch out_keys, state->grouper->GetUniques());
  std::move(out_keys.values.begin(), out_keys.values.end(),
            output_batch_->values.begin());
  // And finally, the aggregates themselves
  size_t base = key_field_ids_.size();
  for (size_t i = 0; i < agg_kernels_.size(); ++i) {
    KernelContext batch_ctx{ctx_->exec_context()};
    batch_ctx.SetState(state->agg_states[i].get());
    RETURN_NOT_OK(
        agg_kernels_[i]->finalize(&batch_ctx, &output_batch_->values[i + base]));
    state->agg_states[i].reset();
  }
  state->grouper.reset();

  return Status::OK();
}

}  // namespace ara::pipeline

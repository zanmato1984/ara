#include "sort.h"
#include "op_output.h"
#include "table_parallel_source.h"

#include <ara/task/task_status.h>

#include <arrow/acero/query_context.h>

namespace ara::pipeline {

using namespace ara::task;

using namespace arrow;
using namespace arrow::acero;
using namespace arrow::compute;

Status Sort::Init(const PipelineContext&, size_t dop, arrow::acero::QueryContext* ctx,
                  SortOptions options, std::shared_ptr<Schema> output_schema) {
  dop_ = dop;
  ctx_ = ctx;
  options_ = std::move(options);
  output_schema_ = std::move(output_schema);
  return Status::OK();
}

PipelineSink Sort::Sink(const PipelineContext&) {
  return [&](const PipelineContext&, const TaskContext&, ThreadId,
             std::optional<Batch> batch) -> OpResult {
    if (!batch.has_value()) {
      return OpOutput::PipeSinkNeedsMore();
    }
    ARA_ASSIGN_OR_RAISE(auto record_batch,
                        batch.value().ToRecordBatch(output_schema_, ctx_->memory_pool()));
    std::lock_guard<std::mutex> lock(mutex_);
    batches_.push_back(std::move(record_batch));
    return OpOutput::PipeSinkNeedsMore();
  };
}

TaskGroups Sort::Frontend(const PipelineContext&) {
  Task do_sort("Sort::DoSort", "",
               [&](const TaskContext& task_ctx, TaskId) -> TaskResult {
                 ARA_RETURN_NOT_OK(DoSort());
                 return TaskStatus::Finished();
               });

  return {{"Sort::DoSort", "", std::move(do_sort), 1, std::nullopt, nullptr}};
}

std::unique_ptr<SourceOp> Sort::ImplicitSource(const PipelineContext& ctx) {
  auto source =
      std::make_unique<detail::TableParallelSource>("Sort::ImplicitSource", "For Sort");
  ARA_CHECK_OK(source->Init(ctx, dop_, table_));
  return source;
}

Status Sort::DoSort() {
  ARA_ASSIGN_OR_RAISE(auto table,
                      Table::FromRecordBatches(output_schema_, std::move(batches_)));
  ARA_ASSIGN_OR_RAISE(auto indices, SortIndices(table, options_, ctx_->exec_context()));
  ARA_ASSIGN_OR_RAISE(auto sorted, Take(table, indices, TakeOptions::NoBoundsCheck(),
                                        ctx_->exec_context()));
  table_ = sorted.table();
  return Status::OK();
}

}  // namespace ara::pipeline

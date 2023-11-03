#include "table_parallel_source.h"
#include "op_output.h"

#include <ara/task/task_status.h>

#include <ara/pipeline/pipeline_context.h>
#include <ara/pipeline/pipeline_observer.h>

namespace ara::pipeline::detail {

using namespace ara;
using namespace ara::task;

using namespace arrow;

Status TableParallelSource::Init(const PipelineContext&, size_t dop,
                                 std::shared_ptr<Table> table) {
  dop_ = dop;
  table_ = std::move(table);
  thread_locals_.resize(dop_);
  return Status::OK();
}

PipelineSource TableParallelSource::Source(const PipelineContext& pipeline_ctx) {
  return [&](const PipelineContext&, const TaskContext&, ThreadId thread_id) -> OpResult {
    size_t current_batch = thread_locals_[thread_id].n++ * dop_ + thread_id;
    size_t next_batch = thread_locals_[thread_id].n * dop_ + thread_id;

    if (current_batch >= batches_.size()) {
      return OpOutput::Finished();
    }

    if (next_batch < batches_.size()) {
      return OpOutput::SourcePipeHasMore(std::move(batches_[current_batch]));
    } else {
      return OpOutput::Finished(std::move(batches_[current_batch]));
    }
  };
}

TaskGroups TableParallelSource::Frontend(const PipelineContext&) {
  Task read_table(
      "TableParallelSource::ReadTableTask", "",
      [&](const TaskContext& task_ctx, TaskId) -> TaskResult {
        TableBatchReader reader(table_);
        reader.set_chunksize(task_ctx.query_ctx->options.source_max_batch_length);
        int batch_index = 0;
        while (true) {
          ARA_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> next, reader.Next());
          if (!next) {
            return TaskStatus::Finished();
          }
          int index = batch_index++;
          batches_.push_back(Batch(*next));
        }
        return TaskStatus::Finished();
      });

  return {{"TableParallelSource::ReadTable", "", std::move(read_table), 1, std::nullopt,
           nullptr}};
}

}  // namespace ara::pipeline::detail

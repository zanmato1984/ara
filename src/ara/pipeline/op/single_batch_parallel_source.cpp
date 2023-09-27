#include "single_batch_parallel_source.h"
#include "op_output.h"

#include <ara/pipeline/pipeline_context.h>
#include <ara/pipeline/pipeline_observer.h>

#include <arrow/util/bit_util.h>

namespace ara::pipeline::detail {

using namespace ara;
using namespace ara::task;

using namespace arrow::bit_util;

Status SingleBatchParallelSource::Init(const PipelineContext&, size_t dop,
                                       std::shared_ptr<Batch> batch) {
  dop_ = dop;
  batch_ = std::move(batch);
  return Status::OK();
}

PipelineSource SingleBatchParallelSource::Source(const PipelineContext& pipeline_ctx) {
  // TODO: Consider an alternative using atomic offset across all threads.
  return [&](const PipelineContext& pipeline_context, const TaskContext& task_context,
             ThreadId thread_id) -> OpResult {
    size_t source_batch_size = pipeline_ctx.query_ctx->options.source_max_batch_length;
    size_t total_rows_per_thread = CeilDiv(batch_->length, dop_);
    size_t start_this_thread = total_rows_per_thread * thread_id;
    size_t end_this_thread =
        std::min(total_rows_per_thread * (thread_id + 1), size_t(batch_->length));
    size_t start_this_run =
        start_this_thread + thread_locals_[thread_id].n++ * source_batch_size;
    size_t length_this_run =
        std::min(source_batch_size, end_this_thread - start_this_run);
    if (length_this_run == 0) {
      return OpOutput::Finished();
    }
    auto batch_this_run = batch_->Slice(start_this_run, length_this_run);
    if (start_this_run + length_this_run == end_this_thread) {
      return OpOutput::Finished(std::move(batch_this_run));
    } else {
      return OpOutput::SourcePipeHasMore(std::move(batch_this_run));
    }
  };
}

}  // namespace ara::pipeline::detail

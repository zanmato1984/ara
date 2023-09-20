#include "hash_join_build.h"
#include "op_output.h"

#include <ara/task/task_status.h>

#include <arrow/acero/query_context.h>

namespace ara::pipeline {

using namespace ara::task;

using namespace arrow;
using namespace arrow::util;
using namespace arrow::acero;
using namespace arrow::acero::util;
using namespace arrow::compute;

namespace detail {

Result<Batch> KeyPayloadFromInput(const HashJoinProjectionMaps* schema, MemoryPool* pool,
                                  Batch* input) {
  Batch projected({}, input->length);
  int num_key_cols = schema->num_cols(HashJoinProjection::KEY);
  int num_payload_cols = schema->num_cols(HashJoinProjection::PAYLOAD);
  projected.values.resize(num_key_cols + num_payload_cols);

  auto key_to_input = schema->map(HashJoinProjection::KEY, HashJoinProjection::INPUT);
  for (int icol = 0; icol < num_key_cols; ++icol) {
    const Datum& value_in = input->values[key_to_input.get(icol)];
    if (value_in.is_scalar()) {
      ARROW_ASSIGN_OR_RAISE(
          projected.values[icol],
          MakeArrayFromScalar(*value_in.scalar(), projected.length, pool));
    } else {
      projected.values[icol] = value_in;
    }
  }
  auto payload_to_input =
      schema->map(HashJoinProjection::PAYLOAD, HashJoinProjection::INPUT);
  for (int icol = 0; icol < num_payload_cols; ++icol) {
    const Datum& value_in = input->values[payload_to_input.get(icol)];
    if (value_in.is_scalar()) {
      ARROW_ASSIGN_OR_RAISE(
          projected.values[num_key_cols + icol],
          MakeArrayFromScalar(*value_in.scalar(), projected.length, pool));
    } else {
      projected.values[num_key_cols + icol] = value_in;
    }
  }

  return projected;
}

Status BuildProcessor::Init(int64_t hardware_flags, MemoryPool* pool,
                            const HashJoinProjectionMaps* schema, JoinType join_type,
                            SwissTableForJoinBuild* hash_table_build,
                            SwissTableForJoin* hash_table, AccumulationQueue* batches,
                            const std::vector<JoinResultMaterialize*>& materialize) {
  dop_ = materialize.size();
  hardware_flags_ = hardware_flags;
  pool_ = pool;

  schema_ = schema;
  join_type_ = join_type;
  hash_table_build_ = hash_table_build;
  hash_table_ = hash_table;
  batches_ = batches;

  thread_locals_.resize(dop_);
  for (int i = 0; i < materialize.size(); ++i) {
    thread_locals_[i].materialize = materialize[i];
  }

  return Status::OK();
}

Status BuildProcessor::StartBuild() {
  bool reject_duplicate_keys =
      join_type_ == JoinType::LEFT_SEMI || join_type_ == JoinType::LEFT_ANTI;
  bool no_payload =
      reject_duplicate_keys || schema_->num_cols(HashJoinProjection::PAYLOAD) == 0;

  std::vector<KeyColumnMetadata> key_types;
  for (int i = 0; i < schema_->num_cols(HashJoinProjection::KEY); ++i) {
    ARA_ASSIGN_OR_RAISE(
        KeyColumnMetadata metadata,
        ColumnMetadataFromDataType(schema_->data_type(HashJoinProjection::KEY, i)));
    key_types.push_back(metadata);
  }
  std::vector<KeyColumnMetadata> payload_types;
  for (int i = 0; i < schema_->num_cols(HashJoinProjection::PAYLOAD); ++i) {
    ARA_ASSIGN_OR_RAISE(
        KeyColumnMetadata metadata,
        ColumnMetadataFromDataType(schema_->data_type(HashJoinProjection::PAYLOAD, i)));
    payload_types.push_back(metadata);
  }
  return hash_table_build_->Init(hash_table_, dop_, batches_->row_count(),
                                 reject_duplicate_keys, no_payload, key_types,
                                 payload_types, pool_, hardware_flags_);
}

TaskResult BuildProcessor::Build(ThreadId thread_id,
                                 arrow::util::TempVectorStack* temp_stack) {
  auto batch_id = dop_ * thread_locals_[thread_id].round + thread_id;
  if (batch_id >= batches_->batch_count()) {
    return TaskStatus::Finished();
  }
  thread_locals_[thread_id].round++;

  bool no_payload = hash_table_build_->no_payload();

  Batch input_batch;
  ARA_ASSIGN_OR_RAISE(input_batch,
                      KeyPayloadFromInput(schema_, pool_, &(*batches_)[batch_id]));

  if (input_batch.length == 0) {
    return TaskStatus::Continue();
  }

  // Split batch into key batch and optional payload batch
  //
  // Input batch is key-payload batch (key columns followed by payload
  // columns). We split it into two separate batches.
  //
  // TODO: Change SwissTableForJoinBuild interface to use key-payload
  // batch instead to avoid this operation, which involves increasing
  // shared pointer ref counts.
  //
  Batch key_batch({}, input_batch.length);
  key_batch.values.resize(schema_->num_cols(HashJoinProjection::KEY));
  for (size_t icol = 0; icol < key_batch.values.size(); ++icol) {
    key_batch.values[icol] = input_batch.values[icol];
  }
  Batch payload_batch({}, input_batch.length);

  if (!no_payload) {
    payload_batch.values.resize(schema_->num_cols(HashJoinProjection::PAYLOAD));
    for (size_t icol = 0; icol < payload_batch.values.size(); ++icol) {
      payload_batch.values[icol] =
          input_batch.values[schema_->num_cols(HashJoinProjection::KEY) + icol];
    }
  }

  ARA_RETURN_NOT_OK(hash_table_build_->PushNextBatch(
      static_cast<int64_t>(thread_id), key_batch, no_payload ? nullptr : &payload_batch,
      temp_stack));

  // Release input batch
  //
  input_batch.values.clear();

  return TaskStatus::Continue();
}

TaskResult BuildProcessor::FinishBuild() {
  batches_->Clear();
  return TaskStatus::Finished();
}

TaskResult BuildProcessor::StartMerge() {
  auto status = hash_table_build_->PreparePrtnMerge();
  if (status.ok()) {
    return TaskStatus::Finished();
  } else {
    return status;
  }
}

TaskResult BuildProcessor::Merge(ThreadId thread_id,
                                 arrow::util::TempVectorStack* temp_stack) {
  hash_table_build_->PrtnMerge(static_cast<int>(thread_id));
  return TaskStatus::Finished();
}

TaskResult BuildProcessor::FinishMerge(arrow::util::TempVectorStack* temp_stack) {
  hash_table_build_->FinishPrtnMerge(temp_stack);

  for (int i = 0; i < thread_locals_.size(); ++i) {
    thread_locals_[i].materialize->SetBuildSide(hash_table_->keys()->keys(),
                                                hash_table_->payloads(),
                                                hash_table_->key_to_payload() == nullptr);
  }

  return TaskStatus::Finished();
}

}  // namespace detail

Status HashJoinBuild::Init(size_t dop, arrow::acero::QueryContext* ctx,
                           int64_t hardware_flags, MemoryPool* pool,
                           const HashJoinProjectionMaps* schema, JoinType join_type,
                           SwissTableForJoinBuild* hash_table_build,
                           SwissTableForJoin* hash_table,
                           const std::vector<JoinResultMaterialize*>& materialize) {
  dop_ = dop;
  ctx_ = ctx;
  hash_table_build_ = hash_table_build;
  return build_processor_.Init(hardware_flags, pool, schema, join_type, hash_table_build,
                               hash_table, &build_side_batches_, materialize);
}

PipelineSink HashJoinBuild::Sink() {
  return
      [&](const PipelineContext& pipeline_context, const task::TaskContext& task_context,
          ThreadId thread_id, std::optional<Batch> batch) -> OpResult {
        if (!batch.has_value()) {
          return OpOutput::PipeSinkNeedsMore();
        }
        std::lock_guard<std::mutex> guard(build_side_mutex_);
        build_side_batches_.InsertBatch(std::move(batch.value()));
        return OpOutput::PipeSinkNeedsMore();
      };
}

TaskGroups HashJoinBuild::Frontend(const PipelineContext&) {
  ARA_CHECK_OK(build_processor_.StartBuild());

  Task build_task("HashJoinBuild::BuildTask", "",
                  [&](const TaskContext&, TaskId task_id) -> TaskResult {
                    ARA_ASSIGN_OR_RAISE(TempVectorStack * temp_stack,
                                        ctx_->GetTempStack(task_id));
                    return build_processor_.Build(task_id, temp_stack);
                  });
  Continuation build_task_cont("HashJoinBuild::BuildCont", "",
                               [&](const TaskContext&) -> TaskResult {
                                 ARA_RETURN_NOT_OK(build_processor_.FinishBuild());
                                 return build_processor_.StartMerge();
                               });

  Task merge_task("HashJoinBuild::MergeTask", "",
                  [&](const TaskContext&, TaskId task_id) -> TaskResult {
                    ARA_ASSIGN_OR_RAISE(TempVectorStack * temp_stack,
                                        ctx_->GetTempStack(task_id));
                    return build_processor_.Merge(task_id, temp_stack);
                  });
  size_t num_merge_tasks = hash_table_build_->num_prtns();

  Task finish_merge_task("HashJoinBuild::FinishMergeTask", "",
                         [&](const TaskContext&, TaskId task_id) -> TaskResult {
                           ARA_ASSIGN_OR_RAISE(TempVectorStack * temp_stack,
                                               ctx_->GetTempStack(task_id));
                           return build_processor_.FinishMerge(temp_stack);
                         });

  return {{"HashJoinBuld::Build", "", std::move(build_task), dop_,
           std::move(build_task_cont), nullptr},
          {"HashJoinBuld::Merge", "", std::move(merge_task), num_merge_tasks,
           std::nullopt, nullptr},
          {"HashJoinBuld::FinishMerge", "", std::move(finish_merge_task), 1, std::nullopt,
           nullptr}};
}

}  // namespace ara::pipeline

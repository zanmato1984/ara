#include "hash_join_build.h"
#include "hash_join.h"
#include "op_output.h"

#include <ara/task/task_status.h>
#include <ara/util/util.h>

namespace ara::pipeline {

using namespace ara::task;
using namespace ara::util;

using namespace arrow;
using namespace arrow::util;
using namespace arrow::acero;
using namespace arrow::acero::util;
using namespace arrow::compute;

namespace detail {

class BuildProcessor {
 public:
  Status Init(HashJoin* hash_join, AccumulationQueue* batches) {
    hash_join_ = hash_join;
    dop_ = hash_join->dop;
    schema_ = hash_join->schema[1];
    hash_table_build_ = &hash_join->hash_table_build;
    batches_ = batches;
    return Status::OK();
  }

  Status StartBuild() {
    std::vector<KeyColumnMetadata> key_types;
    for (int i = 0; i < schema_->num_cols(HashJoinProjection::KEY); ++i) {
      ARROW_ASSIGN_OR_RAISE(
          KeyColumnMetadata metadata,
          ColumnMetadataFromDataType(schema_->data_type(HashJoinProjection::KEY, i)));
      key_types.push_back(metadata);
    }

    std::vector<KeyColumnMetadata> payload_types;
    for (int i = 0; i < schema_->num_cols(HashJoinProjection::PAYLOAD); ++i) {
      ARROW_ASSIGN_OR_RAISE(
          KeyColumnMetadata metadata,
          ColumnMetadataFromDataType(schema_->data_type(HashJoinProjection::PAYLOAD, i)));
      payload_types.push_back(metadata);
    }

    bool reject_duplicate_keys = hash_join_->join_type == JoinType::LEFT_SEMI ||
                                hash_join_->join_type == JoinType::LEFT_ANTI;
    bool no_payload = reject_duplicate_keys ||
                      schema_->num_cols(HashJoinProjection::PAYLOAD) == 0;

    return hash_table_build_->Init(&hash_join_->hash_table, static_cast<int>(dop_),
                                   batches_->row_count(), batches_->batch_count(),
                                   reject_duplicate_keys, no_payload, key_types,
                                   payload_types, hash_join_->pool,
                                   hash_join_->hardware_flags);
  }

  TaskResult Partition(ThreadId thread_id) {
    for (int64_t batch_id = static_cast<int64_t>(thread_id);
         batch_id < static_cast<int64_t>(batches_->batch_count());
         batch_id += static_cast<int64_t>(dop_)) {
      Batch input_batch;
      ARA_ASSIGN_OR_RAISE(input_batch, hash_join_->KeyPayloadFromInput(
                                           /*side=*/1, &(*batches_)[batch_id]));

      Batch key_batch({}, input_batch.length);
      key_batch.values.resize(schema_->num_cols(HashJoinProjection::KEY));
      for (size_t icol = 0; icol < key_batch.values.size(); ++icol) {
        key_batch.values[icol] = input_batch.values[icol];
      }

      auto* temp_stack = &hash_join_->temp_stacks[thread_id];
      ARA_RETURN_NOT_OK(hash_table_build_->PartitionBatch(
          static_cast<size_t>(thread_id), batch_id, key_batch, temp_stack));
    }

    return TaskStatus::Finished();
  }

  TaskResult Build(ThreadId thread_id) {
    bool no_payload = hash_table_build_->no_payload();

    ExecBatch key_batch;
    ExecBatch payload_batch;
    auto num_keys = schema_->num_cols(HashJoinProjection::KEY);
    auto num_payloads = schema_->num_cols(HashJoinProjection::PAYLOAD);
    key_batch.values.resize(num_keys);
    if (!no_payload) {
      payload_batch.values.resize(num_payloads);
    }

    auto* temp_stack = &hash_join_->temp_stacks[thread_id];

    for (int64_t prtn_id = static_cast<int64_t>(thread_id);
         prtn_id < static_cast<int64_t>(hash_table_build_->num_prtns());
         prtn_id += static_cast<int64_t>(dop_)) {
      for (int64_t batch_id = 0;
           batch_id < static_cast<int64_t>(batches_->batch_count()); ++batch_id) {
        Batch input_batch;
        ARA_ASSIGN_OR_RAISE(input_batch, hash_join_->KeyPayloadFromInput(
                                             /*side=*/1, &(*batches_)[batch_id]));

        key_batch.length = input_batch.length;
        for (size_t icol = 0; icol < key_batch.values.size(); ++icol) {
          key_batch.values[icol] = input_batch.values[icol];
        }

        if (!no_payload) {
          payload_batch.length = input_batch.length;
          for (size_t icol = 0; icol < payload_batch.values.size(); ++icol) {
            payload_batch.values[icol] = input_batch.values[num_keys + icol];
          }
        }

        ARA_RETURN_NOT_OK(hash_table_build_->ProcessPartition(
            static_cast<size_t>(thread_id), batch_id, static_cast<int>(prtn_id),
            key_batch, no_payload ? nullptr : &payload_batch, temp_stack));
      }
    }

    return TaskStatus::Finished();
  }

  TaskResult PrepareMerge() {
    batches_->Clear();
    ARA_RETURN_NOT_OK(hash_table_build_->PreparePrtnMerge());
    return TaskStatus::Finished();
  }

  TaskResult Merge(TaskId task_id) {
    hash_table_build_->PrtnMerge(static_cast<int>(task_id));
    return TaskStatus::Finished();
  }

  TaskResult FinishMerge() {
    hash_table_build_->FinishPrtnMerge(&hash_join_->temp_stacks[0]);

    for (int i = 0; i < hash_join_->materialize.size(); ++i) {
      hash_join_->materialize[i].SetBuildSide(
          hash_join_->hash_table.keys()->keys(), hash_join_->hash_table.payloads(),
          hash_join_->hash_table.key_to_payload() == nullptr);
    }

    return TaskStatus::Finished();
  }

  size_t NumPartitions() const { return hash_table_build_->num_prtns(); }

 private:
  HashJoin* hash_join_ = nullptr;
  size_t dop_ = 0;
  HashJoinProjectionMaps* schema_ = nullptr;
  SwissTableForJoinBuild* hash_table_build_ = nullptr;
  AccumulationQueue* batches_ = nullptr;
};

}  // namespace detail

HashJoinBuild::HashJoinBuild(std::string name, std::string desc)
    : SinkOp(std::move(name), std::move(desc)),
      build_processor_(std::make_unique<detail::BuildProcessor>()) {}

HashJoinBuild::~HashJoinBuild() = default;

Status HashJoinBuild::Init(const PipelineContext& ctx,
                           std::shared_ptr<detail::HashJoin> hash_join) {
  hash_join_ = std::move(hash_join);
  dop_ = hash_join_->dop;
  ctx_ = hash_join_->ctx;
  hash_table_build_ = &hash_join_->hash_table_build;
  return build_processor_->Init(hash_join_.get(), &build_side_batches_);
}

PipelineSink HashJoinBuild::Sink(const PipelineContext&) {
  return [&](const PipelineContext& pipeline_context, const TaskContext& task_context,
             ThreadId thread_id, std::optional<Batch> batch) -> OpResult {
    if (!batch.has_value()) {
      return OpOutput::PipeSinkNeedsMore();
    }
    std::lock_guard<std::mutex> lock(build_side_mutex_);
    build_side_batches_.InsertBatch(std::move(batch.value()));
    return OpOutput::PipeSinkNeedsMore();
  };
}

TaskGroups HashJoinBuild::Frontend(const PipelineContext&) {
  ARA_CHECK_OK(build_processor_->StartBuild());

  Task partition_task("HashJoinBuild::PartitionTask", "",
                      [&](const TaskContext&, TaskId task_id) -> TaskResult {
                        return build_processor_->Partition(task_id);
                      });

  Task build_task("HashJoinBuild::BuildTask", "",
                  [&](const TaskContext&, TaskId task_id) -> TaskResult {
                    return build_processor_->Build(task_id);
                  });
  Continuation build_task_cont("HashJoinBuild::BuildCont", "",
                               [&](const TaskContext&) -> TaskResult {
                                 return build_processor_->PrepareMerge();
                               });

  Task merge_task("HashJoinBuild::MergeTask", "",
                  [&](const TaskContext&, TaskId task_id) -> TaskResult {
                    return build_processor_->Merge(task_id);
                  });
  size_t num_merge_tasks = build_processor_->NumPartitions();
  Continuation merge_task_cont("HashJoinBuild::MergeCont", "",
                               [&](const TaskContext&) -> TaskResult {
                                 return build_processor_->FinishMerge();
                               });

  return {{"HashJoinBuild::Partition", "", std::move(partition_task), dop_, std::nullopt,
           nullptr},
          {"HashJoinBuild::Build", "", std::move(build_task), dop_,
           std::move(build_task_cont), nullptr},
          {"HashJoinBuild::Merge", "", std::move(merge_task), num_merge_tasks,
           std::move(merge_task_cont), nullptr}};
}

}  // namespace ara::pipeline

#include "swiss_join.h"
#include <arrow/compute/exec/accumulation_queue.h>
#include <arrow/compute/exec/query_context.h>

namespace arra {

    using namespace arrow;
    using namespace arrow::compute;
    using namespace arrow::util;

class SwissJoin {
 public:
  Status Init(QueryContext* ctx, JoinType join_type, size_t num_threads,
              const HashJoinProjectionMaps* proj_map_left,
              const HashJoinProjectionMaps* proj_map_right,
              std::vector<JoinKeyCmp> key_cmp, Expression filter) {
    // START_COMPUTE_SPAN(span_, "SwissJoinImpl",
    //                    {{"detail", filter.ToString()},
    //                     {"join.kind", arrow::compute::ToString(join_type)},
    //                     {"join.threads", static_cast<uint32_t>(num_threads)}});

    num_threads_ = static_cast<int>(num_threads);
    ctx_ = ctx;
    hardware_flags_ = ctx->cpu_info()->hardware_flags();
    pool_ = ctx->memory_pool();

    join_type_ = join_type;
    key_cmp_.resize(key_cmp.size());
    for (size_t i = 0; i < key_cmp.size(); ++i) {
      key_cmp_[i] = key_cmp[i];
    }

    schema_[0] = proj_map_left;
    schema_[1] = proj_map_right;

    // register_task_group_callback_ = std::move(register_task_group_callback);
    // start_task_group_callback_ = std::move(start_task_group_callback);
    // output_batch_callback_ = std::move(output_batch_callback);
    // finished_callback_ = std::move(finished_callback);

    hash_table_ready_.store(false);
    cancelled_.store(false);
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      left_side_finished_ = false;
      right_side_finished_ = false;
      error_status_ = Status::OK();
    }

    local_states_.resize(num_threads_);
    for (int i = 0; i < num_threads_; ++i) {
      local_states_[i].hash_table_ready = false;
      local_states_[i].num_output_batches = 0;
      local_states_[i].materialize.Init(pool_, proj_map_left, proj_map_right);
    }

    std::vector<JoinResultMaterialize*> materialize;
    materialize.resize(num_threads_);
    for (int i = 0; i < num_threads_; ++i) {
      materialize[i] = &local_states_[i].materialize;
    }

    probe_processor_.Init(proj_map_left->num_cols(HashJoinProjection::KEY), join_type_,
                          &hash_table_, materialize, &key_cmp_, {});

    // InitTaskGroups();

    return Status::OK();
  }

//   void InitTaskGroups() {
//     task_group_build_ = register_task_group_callback_(
//         [this](size_t thread_index, int64_t task_id) -> Status {
//           return BuildTask(thread_index, task_id);
//         },
//         [this](size_t thread_index) -> Status { return BuildFinished(thread_index); });
//     task_group_merge_ = register_task_group_callback_(
//         [this](size_t thread_index, int64_t task_id) -> Status {
//           return MergeTask(thread_index, task_id);
//         },
//         [this](size_t thread_index) -> Status { return MergeFinished(thread_index); });
//     task_group_scan_ = register_task_group_callback_(
//         [this](size_t thread_index, int64_t task_id) -> Status {
//           return ScanTask(thread_index, task_id);
//         },
//         [this](size_t thread_index) -> Status { return ScanFinished(thread_index); });
//   }

  Status ProbeSingleBatch(size_t thread_index, ExecBatch batch) {
    if (IsCancelled()) {
      return status();
    }

    if (!local_states_[thread_index].hash_table_ready) {
      local_states_[thread_index].hash_table_ready = hash_table_ready_.load();
    }
    ARROW_DCHECK(local_states_[thread_index].hash_table_ready);

    ExecBatch keypayload_batch;
    ARROW_ASSIGN_OR_RAISE(keypayload_batch, KeyPayloadFromInput(/*side=*/0, &batch));
    ARROW_ASSIGN_OR_RAISE(util::TempVectorStack * temp_stack,
                          ctx_->GetTempStack(thread_index));

    return CancelIfNotOK(
        probe_processor_.OnNextBatch(thread_index, keypayload_batch, temp_stack,
                                     &local_states_[thread_index].temp_column_arrays));
  }

  Status ProbingFinished(size_t thread_index) {
    if (IsCancelled()) {
      return status();
    }

    return CancelIfNotOK(StartScanHashTable(static_cast<int64_t>(thread_index)));
  }

  Status BuildHashTable(size_t thread_id, AccumulationQueue batches) {
    if (IsCancelled()) {
      return status();
    }

    build_side_batches_ = std::move(batches);

    return CancelIfNotOK(StartBuildHashTable(static_cast<int64_t>(thread_id)));
  }

//   void Abort(AbortContinuationImpl pos_abort_callback) {
//     EVENT(span_, "Abort");
//     END_SPAN(span_);
//     std::ignore = CancelIfNotOK(Status::Cancelled("Hash Join Cancelled"));
//     pos_abort_callback();
//   }

  std::string ToString() const { return "SwissJoin"; }

 private:
  Status StartBuildHashTable(int64_t thread_id) {
    // Initialize build class instance
    //
    const HashJoinProjectionMaps* schema = schema_[1];
    bool reject_duplicate_keys =
        join_type_ == JoinType::LEFT_SEMI || join_type_ == JoinType::LEFT_ANTI;
    bool no_payload =
        reject_duplicate_keys || schema->num_cols(HashJoinProjection::PAYLOAD) == 0;

    std::vector<KeyColumnMetadata> key_types;
    for (int i = 0; i < schema->num_cols(HashJoinProjection::KEY); ++i) {
      ARROW_ASSIGN_OR_RAISE(
          KeyColumnMetadata metadata,
          ColumnMetadataFromDataType(schema->data_type(HashJoinProjection::KEY, i)));
      key_types.push_back(metadata);
    }
    std::vector<KeyColumnMetadata> payload_types;
    for (int i = 0; i < schema->num_cols(HashJoinProjection::PAYLOAD); ++i) {
      ARROW_ASSIGN_OR_RAISE(
          KeyColumnMetadata metadata,
          ColumnMetadataFromDataType(schema->data_type(HashJoinProjection::PAYLOAD, i)));
      payload_types.push_back(metadata);
    }
    RETURN_NOT_OK(CancelIfNotOK(hash_table_build_.Init(
        &hash_table_, num_threads_, build_side_batches_.row_count(),
        reject_duplicate_keys, no_payload, key_types, payload_types, pool_,
        hardware_flags_)));

    // Process all input batches
    //
    // return CancelIfNotOK(
    //     start_task_group_callback_(task_group_build_, build_side_batches_.batch_count()));
    return Status::OK();
  }

  Status BuildTask(size_t thread_id, int64_t batch_id) {
    if (IsCancelled()) {
      return Status::OK();
    }

    const HashJoinProjectionMaps* schema = schema_[1];
    bool no_payload = hash_table_build_.no_payload();

    ExecBatch input_batch;
    ARROW_ASSIGN_OR_RAISE(
        input_batch, KeyPayloadFromInput(/*side=*/1, &build_side_batches_[batch_id]));

    if (input_batch.length == 0) {
      return Status::OK();
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
    ExecBatch key_batch({}, input_batch.length);
    key_batch.values.resize(schema->num_cols(HashJoinProjection::KEY));
    for (size_t icol = 0; icol < key_batch.values.size(); ++icol) {
      key_batch.values[icol] = input_batch.values[icol];
    }
    ExecBatch payload_batch({}, input_batch.length);

    if (!no_payload) {
      payload_batch.values.resize(schema->num_cols(HashJoinProjection::PAYLOAD));
      for (size_t icol = 0; icol < payload_batch.values.size(); ++icol) {
        payload_batch.values[icol] =
            input_batch.values[schema->num_cols(HashJoinProjection::KEY) + icol];
      }
    }
    ARROW_ASSIGN_OR_RAISE(util::TempVectorStack * temp_stack,
                          ctx_->GetTempStack(thread_id));
    RETURN_NOT_OK(CancelIfNotOK(hash_table_build_.PushNextBatch(
        static_cast<int64_t>(thread_id), key_batch, no_payload ? nullptr : &payload_batch,
        temp_stack)));

    // Release input batch
    //
    input_batch.values.clear();

    return Status::OK();
  }

  Status BuildFinished(size_t thread_id) {
    RETURN_NOT_OK(status());

    build_side_batches_.Clear();

    // On a single thread prepare for merging partitions of the resulting hash
    // table.
    //
    RETURN_NOT_OK(CancelIfNotOK(hash_table_build_.PreparePrtnMerge()));
    // return CancelIfNotOK(
    //     start_task_group_callback_(task_group_merge_, hash_table_build_.num_prtns()));
    return Status::OK();
  }

  Status MergeTask(size_t /*thread_id*/, int64_t prtn_id) {
    if (IsCancelled()) {
      return Status::OK();
    }
    hash_table_build_.PrtnMerge(static_cast<int>(prtn_id));
    return Status::OK();
  }

  Status MergeFinished(size_t thread_id) {
    RETURN_NOT_OK(status());
    ARROW_ASSIGN_OR_RAISE(util::TempVectorStack * temp_stack,
                          ctx_->GetTempStack(thread_id));
    hash_table_build_.FinishPrtnMerge(temp_stack);
    return CancelIfNotOK(OnBuildHashTableFinished(static_cast<int64_t>(thread_id)));
  }

  Status OnBuildHashTableFinished(int64_t thread_id) {
    if (IsCancelled()) {
      return status();
    }

    for (int i = 0; i < num_threads_; ++i) {
      local_states_[i].materialize.SetBuildSide(hash_table_.keys()->keys(),
                                                hash_table_.payloads(),
                                                hash_table_.key_to_payload() == nullptr);
    }
    hash_table_ready_.store(true);

    // return build_finished_callback_(thread_id);
    return Status::OK();
  }

  Status StartScanHashTable(int64_t thread_id) {
    if (IsCancelled()) {
      return status();
    }

    bool need_to_scan =
        (join_type_ == JoinType::RIGHT_SEMI || join_type_ == JoinType::RIGHT_ANTI ||
         join_type_ == JoinType::RIGHT_OUTER || join_type_ == JoinType::FULL_OUTER);

    if (need_to_scan) {
      hash_table_.MergeHasMatch();
      int64_t num_tasks = arrow::bit_util::CeilDiv(hash_table_.num_rows(), kNumRowsPerScanTask);

    //   return CancelIfNotOK(start_task_group_callback_(task_group_scan_, num_tasks));
    return Status::OK();
    } else {
      return CancelIfNotOK(OnScanHashTableFinished());
    }
  }

  Status ScanTask(size_t thread_id, int64_t task_id) {
    if (IsCancelled()) {
      return Status::OK();
    }

    // Should we output matches or non-matches?
    //
    bool bit_to_output = (join_type_ == JoinType::RIGHT_SEMI);

    int64_t start_row = task_id * kNumRowsPerScanTask;
    int64_t end_row =
        std::min((task_id + 1) * kNumRowsPerScanTask, hash_table_.num_rows());
    // Get thread index and related temp vector stack
    //
    ARROW_ASSIGN_OR_RAISE(util::TempVectorStack * temp_stack,
                          ctx_->GetTempStack(thread_id));

    // Split into mini-batches
    //
    auto payload_ids_buf =
        util::TempVectorHolder<uint32_t>(temp_stack, util::MiniBatch::kMiniBatchLength);
    auto key_ids_buf =
        util::TempVectorHolder<uint32_t>(temp_stack, util::MiniBatch::kMiniBatchLength);
    auto selection_buf =
        util::TempVectorHolder<uint16_t>(temp_stack, util::MiniBatch::kMiniBatchLength);
    for (int64_t mini_batch_start = start_row; mini_batch_start < end_row;) {
      // Compute the size of the next mini-batch
      //
      int64_t mini_batch_size_next =
          std::min(end_row - mini_batch_start,
                   static_cast<int64_t>(util::MiniBatch::kMiniBatchLength));

      // Get the list of key and payload ids from this mini-batch to output.
      //
      uint32_t first_key_id =
          hash_table_.payload_id_to_key_id(static_cast<uint32_t>(mini_batch_start));
      uint32_t last_key_id = hash_table_.payload_id_to_key_id(
          static_cast<uint32_t>(mini_batch_start + mini_batch_size_next - 1));
      int num_output_rows = 0;
      for (uint32_t key_id = first_key_id; key_id <= last_key_id; ++key_id) {
        if (arrow::bit_util::GetBit(hash_table_.has_match(), key_id) == bit_to_output) {
          uint32_t first_payload_for_key =
              std::max(static_cast<uint32_t>(mini_batch_start),
                       hash_table_.key_to_payload() ? hash_table_.key_to_payload()[key_id]
                                                    : key_id);
          uint32_t last_payload_for_key = std::min(
              static_cast<uint32_t>(mini_batch_start + mini_batch_size_next - 1),
              hash_table_.key_to_payload() ? hash_table_.key_to_payload()[key_id + 1] - 1
                                           : key_id);
          uint32_t num_payloads_for_key =
              last_payload_for_key - first_payload_for_key + 1;
          for (uint32_t i = 0; i < num_payloads_for_key; ++i) {
            key_ids_buf.mutable_data()[num_output_rows + i] = key_id;
            payload_ids_buf.mutable_data()[num_output_rows + i] =
                first_payload_for_key + i;
          }
          num_output_rows += num_payloads_for_key;
        }
      }

      if (num_output_rows > 0) {
        // Materialize (and output whenever buffers get full) hash table
        // values according to the generated list of ids.
        //
        Status status = local_states_[thread_id].materialize.AppendBuildOnly(
            num_output_rows, key_ids_buf.mutable_data(), payload_ids_buf.mutable_data(),
            [&](ExecBatch batch) {
            //   output_batch_callback_(static_cast<int64_t>(thread_id), std::move(batch));
            });
        RETURN_NOT_OK(CancelIfNotOK(status));
        if (!status.ok()) {
          break;
        }
      }
      mini_batch_start += mini_batch_size_next;
    }

    return Status::OK();
  }

  Status ScanFinished(size_t thread_id) {
    if (IsCancelled()) {
      return status();
    }

    return CancelIfNotOK(OnScanHashTableFinished());
  }

  Status OnScanHashTableFinished() {
    if (IsCancelled()) {
      return status();
    }
    // END_SPAN(span_);

    // Flush all instances of materialize that have non-zero accumulated output
    // rows.
    //
    RETURN_NOT_OK(CancelIfNotOK(probe_processor_.OnFinished()));

    int64_t num_produced_batches = 0;
    for (size_t i = 0; i < local_states_.size(); ++i) {
      JoinResultMaterialize& materialize = local_states_[i].materialize;
      num_produced_batches += materialize.num_produced_batches();
    }

    // finished_callback_(num_produced_batches);

    return Status::OK();
  }

  Result<ExecBatch> KeyPayloadFromInput(int side, ExecBatch* input) {
    ExecBatch projected({}, input->length);
    int num_key_cols = schema_[side]->num_cols(HashJoinProjection::KEY);
    int num_payload_cols = schema_[side]->num_cols(HashJoinProjection::PAYLOAD);
    projected.values.resize(num_key_cols + num_payload_cols);

    auto key_to_input =
        schema_[side]->map(HashJoinProjection::KEY, HashJoinProjection::INPUT);
    for (int icol = 0; icol < num_key_cols; ++icol) {
      const Datum& value_in = input->values[key_to_input.get(icol)];
      if (value_in.is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(
            projected.values[icol],
            MakeArrayFromScalar(*value_in.scalar(), projected.length, pool_));
      } else {
        projected.values[icol] = value_in;
      }
    }
    auto payload_to_input =
        schema_[side]->map(HashJoinProjection::PAYLOAD, HashJoinProjection::INPUT);
    for (int icol = 0; icol < num_payload_cols; ++icol) {
      const Datum& value_in = input->values[payload_to_input.get(icol)];
      if (value_in.is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(
            projected.values[num_key_cols + icol],
            MakeArrayFromScalar(*value_in.scalar(), projected.length, pool_));
      } else {
        projected.values[num_key_cols + icol] = value_in;
      }
    }

    return projected;
  }

  bool IsCancelled() { return cancelled_.load(); }

  Status status() {
    if (IsCancelled()) {
      std::lock_guard<std::mutex> lock(state_mutex_);
      return error_status_;
    }
    return Status::OK();
  }

  Status CancelIfNotOK(Status status) {
    if (!status.ok()) {
      {
        std::lock_guard<std::mutex> lock(state_mutex_);
        // Only update the status for the first error encountered.
        //
        if (error_status_.ok()) {
          error_status_ = status;
        }
      }
      cancelled_.store(true);
    }
    return status;
  }

  static constexpr int kNumRowsPerScanTask = 512 * 1024;

  QueryContext* ctx_;
  int64_t hardware_flags_;
  MemoryPool* pool_;
  int num_threads_;
  JoinType join_type_;
  std::vector<JoinKeyCmp> key_cmp_;
  const HashJoinProjectionMaps* schema_[2];

  // Task scheduling
  int task_group_build_;
  int task_group_merge_;
  int task_group_scan_;

  struct ThreadLocalState {
    JoinResultMaterialize materialize;
    std::vector<KeyColumnArray> temp_column_arrays;
    int64_t num_output_batches;
    bool hash_table_ready;
  };
  std::vector<ThreadLocalState> local_states_;

  SwissTableForJoin hash_table_;
  JoinProbeProcessor probe_processor_;
  SwissTableForJoinBuild hash_table_build_;
  AccumulationQueue build_side_batches_;

  // Atomic state flags.
  // These flags are kept outside of mutex, since they can be queried for every
  // batch.
  //
  // The other flags that follow them, protected by mutex, will be queried or
  // updated only a fixed number of times during entire join processing.
  //
  std::atomic<bool> hash_table_ready_;
  std::atomic<bool> cancelled_;

  // Mutex protecting state flags.
  //
  std::mutex state_mutex_;

  // Mutex protected state flags.
  //
  bool left_side_finished_;
  bool right_side_finished_;
  Status error_status_;
};

// Result<std::unique_ptr<HashJoinImpl>> HashJoinImpl::MakeSwiss() {
//   std::unique_ptr<HashJoinImpl> impl{new SwissJoin()};
//   return std::move(impl);
// }

}  // namespace arra
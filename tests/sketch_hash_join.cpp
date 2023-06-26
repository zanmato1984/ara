#include "sketch_hash_join.h"

namespace arra::detail {

static constexpr size_t kMaxRowsPerBatch = 4096;

void ProbeProcessor::Init(int num_key_columns, JoinType join_type,
                          const std::vector<JoinKeyCmp>* cmp,
                          SwissTableForJoin* hash_table,
                          JoinResultMaterialize* materialize,
                          arrow::util::TempVectorStack* temp_stack) {
  num_key_columns_ = num_key_columns;
  join_type_ = join_type;
  cmp_ = cmp;
  hash_table_ = hash_table;
  materialize_ = materialize;
}

std::optional<ExecBatch> ProbeProcessor::LeftSemiOrAnti(
    int64_t thread_id, ExecBatch input, arrow::util::TempVectorStack* temp_stack,
    std::vector<KeyColumnArray>* temp_column_arrays) {
  const SwissTable* swiss_table = hash_table_->keys()->swiss_table();
  int64_t hardware_flags = swiss_table->hardware_flags();
  int minibatch_size = swiss_table->minibatch_size();

  std::optional<ExecBatch> output;
  auto process = [&]() -> State {
    int num_rows = static_cast<int>(input_->batch.length);

    // Break into minibatches
    State state_next = State::CLEAN;
    for (; input_->minibatch_start < num_rows && state_next == State::CLEAN;) {
      uint32_t minibatch_size_next =
          std::min(minibatch_size, num_rows - input_->minibatch_start);

      // Calculate hash and matches for this minibatch.
      {
        SwissTableWithKeys::Input hash_table_input(
            &input_->key_batch, input_->minibatch_start,
            input_->minibatch_start + minibatch_size_next, temp_stack,
            temp_column_arrays);
        hash_table_->keys()->Hash(&hash_table_input, input_->hashes_buf.mutable_data(),
                                  hardware_flags);
        hash_table_->keys()->MapReadOnly(&hash_table_input,
                                         input_->hashes_buf.mutable_data(),
                                         input_->match_bitvector_buf.mutable_data(),
                                         input_->key_ids_buf.mutable_data());
      }

      // AND bit vector with null key filter for join
      {
        bool ignored;
        JoinNullFilter::Filter(input_->key_batch, input_->minibatch_start,
                               minibatch_size_next, *cmp_, &ignored,
                               /*and_with_input=*/true,
                               input_->match_bitvector_buf.mutable_data());
      }

      // Semi-joins
      int num_passing_ids = 0;
      arrow::util::bit_util::bits_to_indexes(
          (join_type_ == JoinType::LEFT_ANTI) ? 0 : 1, hardware_flags,
          minibatch_size_next, input_->match_bitvector_buf.mutable_data(),
          &num_passing_ids, input_->materialize_batch_ids_buf.mutable_data());

      // Add base batch row index.
      for (int i = 0; i < num_passing_ids; ++i) {
        input_->materialize_batch_ids_buf.mutable_data()[i] +=
            static_cast<uint16_t>(input_->minibatch_start);
      }

      // If we are to exceed the maximum number of rows per batch, output.
      if (materialize_->num_rows() + num_passing_ids > kMaxRowsPerBatch) {
        DCHECK_OK(materialize_->Flush([&](ExecBatch batch) {
          output.emplace(std::move(batch));
          return Status::OK();
        }));
        state_next = State::HAS_MORE;
      }

      {
        int ignored;
        DCHECK_OK(materialize_->AppendProbeOnly(
            input_->batch, num_passing_ids,
            input_->materialize_batch_ids_buf.mutable_data(), &ignored));
      }

      input_->minibatch_start += minibatch_size_next;
    }

    return state_next;
  };

  // Process.
  State state_next = State::CLEAN;
  switch (state_) {
    case State::CLEAN: {
      // Some check.
      DCHECK(!input_.has_value());
      DCHECK(materialize_->num_rows() == 0);

      // Prepare input.
      auto batch_length = input.length;
      input_ = Input{
          std::move(input),
          ExecBatch({}, batch_length),
          0,
          arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size),
          arrow::util::TempVectorHolder<uint8_t>(
              temp_stack, static_cast<uint32_t>(bit_util::BytesForBits(minibatch_size))),
          arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size),
          arrow::util::TempVectorHolder<uint16_t>(temp_stack, minibatch_size),
          arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size),
          arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size)};
      input_->key_batch.values.resize(num_key_columns_);
      for (int i = 0; i < num_key_columns_; ++i) {
        input_->key_batch.values[i] = input_->batch.values[i];
      }

      // Process input.
      state_next = process();

      break;
    }
    case State::HAS_MORE: {
      // Some check.
      DCHECK(input_.has_value());
      DCHECK(materialize_->num_rows() > 0);

      // Process input.
      state_next = process();

      break;
    }
  }

  // Transition.
  switch (state_next) {
    case State::CLEAN: {
      input_.reset();
      break;
    }
    case State::HAS_MORE: {
      break;
    }
  }

  state_ = state_next;

  return output;
}

Status HashJoin::Init(QueryContext* ctx, JoinType join_type, size_t num_threads,
                      const HashJoinProjectionMaps* proj_map_left,
                      const HashJoinProjectionMaps* proj_map_right,
                      std::vector<JoinKeyCmp> key_cmp, Expression filter) {
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

  local_states_.resize(num_threads_);
  for (int i = 0; i < num_threads_; ++i) {
    local_states_[i].hash_table_ready = false;
    local_states_[i].num_output_batches = 0;
    local_states_[i].materialize.Init(pool_, proj_map_left, proj_map_right);
    ARROW_ASSIGN_OR_RAISE(arrow::util::TempVectorStack * temp_stack,
                          ctx_->GetTempStack(i));
    local_states_[i].probe_processor.Init(
        proj_map_left->num_cols(HashJoinProjection::KEY), join_type_, &key_cmp_,
        &hash_table_, &local_states_[i].materialize, temp_stack);
  }

  return Status::OK();
}

}  // namespace arra::detail

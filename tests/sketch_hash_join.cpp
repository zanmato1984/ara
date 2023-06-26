#include "sketch_hash_join.h"

namespace arra::detail {

static constexpr size_t kMaxRowsPerBatch = 4096;

void ProbeProcessor::Init(int num_key_columns, JoinType join_type,
                          const std::vector<JoinKeyCmp>* cmp,
                          SwissTableForJoin* hash_table,
                          JoinResultMaterialize* materialize,
                          TempVectorStack* temp_stack) {
  num_key_columns_ = num_key_columns;
  join_type_ = join_type;
  cmp_ = cmp;
  hash_table_ = hash_table;
  materialize_ = materialize;
  swiss_table_ = hash_table_->keys()->swiss_table();
  hardware_flags_ = swiss_table_->hardware_flags();
  minibatch_size_ = swiss_table_->minibatch_size();
}

Status ProbeProcessor::ProbeBatch(int64_t thread_id, std::optional<ExecBatch> input,
                                  TempVectorStack* temp_stack,
                                  std::vector<KeyColumnArray>* temp_column_arrays,
                                  OperatorStatus& status) {
  switch (join_type_) {
    case JoinType::INNER:
    case JoinType::LEFT_OUTER:
    case JoinType::RIGHT_OUTER:
    case JoinType::FULL_OUTER: {
      return ProbeBatch(
          thread_id, std::move(input), temp_stack, temp_column_arrays, status,
          [&](int64_t thread_id, TempVectorStack* temp_stack,
              std::vector<KeyColumnArray>* temp_column_arrays,
              std::optional<ExecBatch>& output, State& state_next) -> Status {
            return InnerOuter(thread_id, temp_stack, temp_column_arrays, output,
                              state_next);
          });
    }
    case JoinType::LEFT_SEMI:
    case JoinType::LEFT_ANTI: {
      return ProbeBatch(
          thread_id, std::move(input), temp_stack, temp_column_arrays, status,
          [&](int64_t thread_id, TempVectorStack* temp_stack,
              std::vector<KeyColumnArray>* temp_column_arrays,
              std::optional<ExecBatch>& output, State& state_next) -> Status {
            return LeftSemiAnti(thread_id, temp_stack, temp_column_arrays, output,
                                state_next);
          });
    }
    case JoinType::RIGHT_SEMI:
    case JoinType::RIGHT_ANTI: {
      return ProbeBatch(
          thread_id, std::move(input), temp_stack, temp_column_arrays, status,
          [&](int64_t thread_id, TempVectorStack* temp_stack,
              std::vector<KeyColumnArray>* temp_column_arrays,
              std::optional<ExecBatch>& output, State& state_next) -> Status {
            return RightSemiAnti(thread_id, temp_stack, temp_column_arrays, output,
                                 state_next);
          });
    }
  }
}

Status ProbeProcessor::InnerOuter(int64_t thread_id, TempVectorStack* temp_stack,
                                  std::vector<KeyColumnArray>* temp_column_arrays,
                                  std::optional<ExecBatch>& output, State& state_next) {
  int num_rows = static_cast<int>(input_->batch.length);
  state_next = State::CLEAN;

  // Break into minibatches
  for (; input_->minibatch_start < num_rows && state_next == State::CLEAN;) {
    uint32_t minibatch_size_next =
        std::min(minibatch_size_, num_rows - input_->minibatch_start);
    bool no_duplicate_keys = (hash_table_->key_to_payload() == nullptr);
    bool no_payload_columns = (hash_table_->payloads() == nullptr);

    if (state_ != State::MATCH_HAS_MORE) {
      // Calculate hash and matches for this minibatch.
      SwissTableWithKeys::Input hash_table_input(
          &input_->key_batch, input_->minibatch_start,
          input_->minibatch_start + minibatch_size_next, temp_stack, temp_column_arrays);
      hash_table_->keys()->Hash(&hash_table_input, input_->hashes_buf.mutable_data(),
                                hardware_flags_);
      hash_table_->keys()->MapReadOnly(
          &hash_table_input, input_->hashes_buf.mutable_data(),
          input_->match_bitvector_buf.mutable_data(), input_->key_ids_buf.mutable_data());

      // AND bit vector with null key filter for join
      bool ignored;
      JoinNullFilter::Filter(input_->key_batch, input_->minibatch_start,
                             minibatch_size_next, *cmp_, &ignored,
                             /*and_with_input=*/true,
                             input_->match_bitvector_buf.mutable_data());

      // We need to output matching pairs of rows from both sides of the join.
      // Since every hash table lookup for an input row might have multiple
      // matches we use a helper class that implements enumerating all of them.
      input_->match_iterator.SetLookupResult(
          minibatch_size_next, input_->minibatch_start,
          input_->match_bitvector_buf.mutable_data(), input_->key_ids_buf.mutable_data(),
          no_duplicate_keys, hash_table_->key_to_payload());
    }

    if (state_ != State::MINIBATCH_HAS_MORE) {
      int num_matches_next;
      while (state_next != State::MATCH_HAS_MORE &&
             input_->match_iterator.GetNextBatch(
                 minibatch_size_, &num_matches_next,
                 input_->materialize_batch_ids_buf.mutable_data(),
                 input_->materialize_key_ids_buf.mutable_data(),
                 input_->materialize_payload_ids_buf.mutable_data())) {
        const uint16_t* materialize_batch_ids =
            input_->materialize_batch_ids_buf.mutable_data();
        const uint32_t* materialize_key_ids =
            input_->materialize_key_ids_buf.mutable_data();
        const uint32_t* materialize_payload_ids =
            no_duplicate_keys || no_payload_columns
                ? input_->materialize_key_ids_buf.mutable_data()
                : input_->materialize_payload_ids_buf.mutable_data();

        // For right-outer, full-outer joins we need to update has-match flags
        // for the rows in hash table.
        if (join_type_ == JoinType::RIGHT_OUTER || join_type_ == JoinType::FULL_OUTER) {
          hash_table_->UpdateHasMatchForKeys(thread_id, num_matches_next,
                                             materialize_key_ids);
        }

        // If we are to exceed the maximum number of rows per batch, output.
        if (materialize_->num_rows() + num_matches_next > kMaxRowsPerBatch) {
          ARRA_RETURN_NOT_OK(materialize_->Flush([&](ExecBatch batch) {
            output.emplace(std::move(batch));
            return Status::OK();
          }));
          state_next = State::MATCH_HAS_MORE;
        }

        // Call materialize for resulting id tuples pointing to matching pairs
        // of rows.
        {
          int ignored;
          ARRA_RETURN_NOT_OK(materialize_->Append(
              input_->batch, num_matches_next, materialize_batch_ids, materialize_key_ids,
              materialize_payload_ids, &ignored));
        }
      }
    }

    if (state_next != State::MATCH_HAS_MORE) {
      // For left-outer and full-outer joins output non-matches.
      //
      // Call materialize. Nulls will be output in all columns that come from
      // the other side of the join.
      //
      if (join_type_ == JoinType::LEFT_OUTER || join_type_ == JoinType::FULL_OUTER) {
        int num_passing_ids = 0;
        bit_util::bits_to_indexes(
            /*bit_to_search=*/0, hardware_flags_, minibatch_size_next,
            input_->match_bitvector_buf.mutable_data(), &num_passing_ids,
            input_->materialize_batch_ids_buf.mutable_data());

        // Add base batch row index.
        for (int i = 0; i < num_passing_ids; ++i) {
          input_->materialize_batch_ids_buf.mutable_data()[i] +=
              static_cast<uint16_t>(input_->minibatch_start);
        }

        // If we are to exceed the maximum number of rows per batch, output.
        if (materialize_->num_rows() + num_passing_ids > kMaxRowsPerBatch) {
          ARRA_RETURN_NOT_OK(materialize_->Flush([&](ExecBatch batch) {
            output.emplace(std::move(batch));
            return Status::OK();
          }));
          state_next = State::MINIBATCH_HAS_MORE;
        }

        {
          int ignored;
          ARRA_RETURN_NOT_OK(materialize_[thread_id].AppendProbeOnly(
              input_->batch, num_passing_ids,
              input_->materialize_batch_ids_buf.mutable_data(), &ignored));
        }
      }

      input_->minibatch_start += minibatch_size_next;
    }
  }

  return Status::OK();
}

Status ProbeProcessor::LeftSemiAnti(int64_t thread_id, TempVectorStack* temp_stack,
                                    std::vector<KeyColumnArray>* temp_column_arrays,
                                    std::optional<ExecBatch>& output, State& state_next) {
  int num_rows = static_cast<int>(input_->batch.length);
  state_next = State::CLEAN;

  // Break into minibatches
  for (; input_->minibatch_start < num_rows && state_next == State::CLEAN;) {
    uint32_t minibatch_size_next =
        std::min(minibatch_size_, num_rows - input_->minibatch_start);

    // Calculate hash and matches for this minibatch.
    {
      SwissTableWithKeys::Input hash_table_input(
          &input_->key_batch, input_->minibatch_start,
          input_->minibatch_start + minibatch_size_next, temp_stack, temp_column_arrays);
      hash_table_->keys()->Hash(&hash_table_input, input_->hashes_buf.mutable_data(),
                                hardware_flags_);
      hash_table_->keys()->MapReadOnly(
          &hash_table_input, input_->hashes_buf.mutable_data(),
          input_->match_bitvector_buf.mutable_data(), input_->key_ids_buf.mutable_data());
    }

    // AND bit vector with null key filter for join
    {
      bool ignored;
      JoinNullFilter::Filter(input_->key_batch, input_->minibatch_start,
                             minibatch_size_next, *cmp_, &ignored,
                             /*and_with_input=*/true,
                             input_->match_bitvector_buf.mutable_data());
    }

    int num_passing_ids = 0;
    bit_util::bits_to_indexes(
        (join_type_ == JoinType::LEFT_ANTI) ? 0 : 1, hardware_flags_, minibatch_size_next,
        input_->match_bitvector_buf.mutable_data(), &num_passing_ids,
        input_->materialize_batch_ids_buf.mutable_data());

    // Add base batch row index.
    for (int i = 0; i < num_passing_ids; ++i) {
      input_->materialize_batch_ids_buf.mutable_data()[i] +=
          static_cast<uint16_t>(input_->minibatch_start);
    }

    // If we are to exceed the maximum number of rows per batch, output.
    if (materialize_->num_rows() + num_passing_ids > kMaxRowsPerBatch) {
      ARRA_RETURN_NOT_OK(materialize_->Flush([&](ExecBatch batch) {
        output.emplace(std::move(batch));
        return Status::OK();
      }));
      state_next = State::MINIBATCH_HAS_MORE;
    }

    {
      int ignored;
      ARRA_RETURN_NOT_OK(materialize_->AppendProbeOnly(
          input_->batch, num_passing_ids,
          input_->materialize_batch_ids_buf.mutable_data(), &ignored));
    }

    input_->minibatch_start += minibatch_size_next;
  }

  return Status::OK();
}

Status ProbeProcessor::RightSemiAnti(int64_t thread_id, TempVectorStack* temp_stack,
                                     std::vector<KeyColumnArray>* temp_column_arrays,
                                     std::optional<ExecBatch>& output,
                                     State& state_next) {
  int num_rows = static_cast<int>(input_->batch.length);
  state_next = State::CLEAN;

  // Break into minibatches
  for (; input_->minibatch_start < num_rows;) {
    uint32_t minibatch_size_next =
        std::min(minibatch_size_, num_rows - input_->minibatch_start);

    // Calculate hash and matches for this minibatch.
    {
      SwissTableWithKeys::Input hash_table_input(
          &input_->key_batch, input_->minibatch_start,
          input_->minibatch_start + minibatch_size_next, temp_stack, temp_column_arrays);
      hash_table_->keys()->Hash(&hash_table_input, input_->hashes_buf.mutable_data(),
                                hardware_flags_);
      hash_table_->keys()->MapReadOnly(
          &hash_table_input, input_->hashes_buf.mutable_data(),
          input_->match_bitvector_buf.mutable_data(), input_->key_ids_buf.mutable_data());
    }

    // AND bit vector with null key filter for join
    {
      bool ignored;
      JoinNullFilter::Filter(input_->key_batch, input_->minibatch_start,
                             minibatch_size_next, *cmp_, &ignored,
                             /*and_with_input=*/true,
                             input_->match_bitvector_buf.mutable_data());
    }

    int num_passing_ids = 0;
    bit_util::bits_to_indexes(
        (join_type_ == JoinType::LEFT_ANTI) ? 0 : 1, hardware_flags_, minibatch_size_next,
        input_->match_bitvector_buf.mutable_data(), &num_passing_ids,
        input_->materialize_batch_ids_buf.mutable_data());

    // For right-semi, right-anti joins: update has-match flags for the rows
    // in hash table.
    for (int i = 0; i < num_passing_ids; ++i) {
      uint16_t id = input_->materialize_batch_ids_buf.mutable_data()[i];
      input_->key_ids_buf.mutable_data()[i] = input_->key_ids_buf.mutable_data()[id];
    }
    hash_table_->UpdateHasMatchForKeys(thread_id, num_passing_ids,
                                       input_->key_ids_buf.mutable_data());

    input_->minibatch_start += minibatch_size_next;
  }

  return Status::OK();
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

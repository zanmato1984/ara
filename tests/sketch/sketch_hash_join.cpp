#include <arrow/acero/query_context.h>

#include "sketch_hash_join.h"

namespace arra::sketch::detail {

Status ValidateHashJoinNodeOptions(const HashJoinNodeOptions& join_options) {
  if (join_options.key_cmp.empty() || join_options.left_keys.empty() ||
      join_options.right_keys.empty()) {
    return Status::Invalid("key_cmp and keys cannot be empty");
  }

  if ((join_options.key_cmp.size() != join_options.left_keys.size()) ||
      (join_options.key_cmp.size() != join_options.right_keys.size())) {
    return Status::Invalid("key_cmp and keys must have the same size");
  }

  return Status::OK();
}

Result<ExecBatch> KeyPayloadFromInput(const HashJoinProjectionMaps* schema,
                                      MemoryPool* pool, ExecBatch* input) {
  ExecBatch projected({}, input->length);
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

bool NeedToScan(JoinType join_type) {
  return join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI ||
         join_type == JoinType::RIGHT_OUTER || join_type == JoinType::FULL_OUTER;
}

Status BuildProcessor::Init(int64_t hardware_flags, MemoryPool* pool,
                            const HashJoinProjectionMaps* schema, JoinType join_type,
                            SwissTableForJoinBuild* hash_table_build,
                            SwissTableForJoin* hash_table, AccumulationQueue* batches,
                            const std::vector<JoinResultMaterialize*>& materialize) {
  hardware_flags_ = hardware_flags;
  pool_ = pool;
  dop_ = materialize.size();

  schema_ = schema;
  join_type_ = join_type;
  hash_table_build_ = hash_table_build;
  hash_table_ = hash_table;
  batches_ = batches;

  local_states_.resize(dop_);
  for (int i = 0; i < materialize.size(); ++i) {
    local_states_[i].materialize = materialize[i];
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
  return hash_table_build_->Init(hash_table_, dop_, batches_->row_count(),
                                 reject_duplicate_keys, no_payload, key_types,
                                 payload_types, pool_, hardware_flags_);
}

Status BuildProcessor::Build(ThreadId thread_id, TempVectorStack* temp_stack,
                             OperatorStatus& status) {
  status = OperatorStatus::HasOutput(std::nullopt);
  auto batch_id = dop_ * local_states_[thread_id].round + thread_id;
  if (batch_id >= batches_->batch_count()) {
    status = OperatorStatus::Finished(std::nullopt);
    return Status::OK();
  }
  local_states_[thread_id].round++;

  bool no_payload = hash_table_build_->no_payload();

  ExecBatch input_batch;
  ARROW_ASSIGN_OR_RAISE(input_batch,
                        KeyPayloadFromInput(schema_, pool_, &(*batches_)[batch_id]));

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
  key_batch.values.resize(schema_->num_cols(HashJoinProjection::KEY));
  for (size_t icol = 0; icol < key_batch.values.size(); ++icol) {
    key_batch.values[icol] = input_batch.values[icol];
  }
  ExecBatch payload_batch({}, input_batch.length);

  if (!no_payload) {
    payload_batch.values.resize(schema_->num_cols(HashJoinProjection::PAYLOAD));
    for (size_t icol = 0; icol < payload_batch.values.size(); ++icol) {
      payload_batch.values[icol] =
          input_batch.values[schema_->num_cols(HashJoinProjection::KEY) + icol];
    }
  }

  ARRA_SET_AND_RETURN_NOT_OK(
      hash_table_build_->PushNextBatch(static_cast<int64_t>(thread_id), key_batch,
                                       no_payload ? nullptr : &payload_batch, temp_stack),
      { status = OperatorStatus::Other(__s); });

  // Release input batch
  //
  input_batch.values.clear();

  return Status::OK();
}

Status BuildProcessor::FinishBuild() {
  batches_->Clear();
  return Status::OK();
}

Status BuildProcessor::StartMerge() { return hash_table_build_->PreparePrtnMerge(); }

Status BuildProcessor::Merge(ThreadId thread_id, TempVectorStack* temp_stack,
                             OperatorStatus& status) {
  hash_table_build_->PrtnMerge(static_cast<int>(thread_id));
  status = OperatorStatus::Finished(std::nullopt);
  return Status::OK();
}

Status BuildProcessor::FinishMerge(TempVectorStack* temp_stack) {
  hash_table_build_->FinishPrtnMerge(temp_stack);

  for (int i = 0; i < local_states_.size(); ++i) {
    local_states_[i].materialize->SetBuildSide(hash_table_->keys()->keys(),
                                               hash_table_->payloads(),
                                               hash_table_->key_to_payload() == nullptr);
  }

  return Status::OK();
}

Status ProbeProcessor::Init(int64_t hardware_flags, MemoryPool* pool, int num_key_columns,
                            const HashJoinProjectionMaps* schema, JoinType join_type,
                            const std::vector<JoinKeyCmp>* cmp,
                            SwissTableForJoin* hash_table,
                            const std::vector<JoinResultMaterialize*>& materialize) {
  hardware_flags_ = hardware_flags;
  pool_ = pool;

  num_key_columns_ = num_key_columns;
  schema_ = schema;
  join_type_ = join_type;
  cmp_ = cmp;

  hash_table_ = hash_table;
  swiss_table_ = hash_table_->keys()->swiss_table();

  local_states_.resize(materialize.size());
  for (int i = 0; i < materialize.size(); i++) {
    local_states_[i].materialize = materialize[i];

    local_states_[i].hashes_buf.resize(MiniBatch::kMiniBatchLength);
    local_states_[i].match_bitvector_buf.resize(MiniBatch::kMiniBatchLength);
    local_states_[i].key_ids_buf.resize(MiniBatch::kMiniBatchLength);
    local_states_[i].materialize_batch_ids_buf.resize(MiniBatch::kMiniBatchLength);
    local_states_[i].materialize_key_ids_buf.resize(MiniBatch::kMiniBatchLength);
    local_states_[i].materialize_payload_ids_buf.resize(MiniBatch::kMiniBatchLength);
  }

  return Status::OK();
}

Status ProbeProcessor::Probe(ThreadId thread_id, std::optional<ExecBatch> input,
                             TempVectorStack* temp_stack,
                             std::vector<KeyColumnArray>* temp_column_arrays,
                             OperatorStatus& status) {
  switch (join_type_) {
    case JoinType::INNER:
    case JoinType::LEFT_OUTER:
    case JoinType::RIGHT_OUTER:
    case JoinType::FULL_OUTER: {
      return Probe(thread_id, std::move(input), temp_stack, temp_column_arrays, status,
                   [&](ThreadId thread_id, TempVectorStack* temp_stack,
                       std::vector<KeyColumnArray>* temp_column_arrays,
                       std::optional<ExecBatch>& output, State& state_next) -> Status {
                     return InnerOuter(thread_id, temp_stack, temp_column_arrays, output,
                                       state_next);
                   });
    }
    case JoinType::LEFT_SEMI:
    case JoinType::LEFT_ANTI: {
      return Probe(thread_id, std::move(input), temp_stack, temp_column_arrays, status,
                   [&](ThreadId thread_id, TempVectorStack* temp_stack,
                       std::vector<KeyColumnArray>* temp_column_arrays,
                       std::optional<ExecBatch>& output, State& state_next) -> Status {
                     return LeftSemiAnti(thread_id, temp_stack, temp_column_arrays,
                                         output, state_next);
                   });
    }
    case JoinType::RIGHT_SEMI:
    case JoinType::RIGHT_ANTI: {
      return Probe(thread_id, std::move(input), temp_stack, temp_column_arrays, status,
                   [&](ThreadId thread_id, TempVectorStack* temp_stack,
                       std::vector<KeyColumnArray>* temp_column_arrays,
                       std::optional<ExecBatch>& output, State& state_next) -> Status {
                     return RightSemiAnti(thread_id, temp_stack, temp_column_arrays,
                                          output, state_next);
                   });
    }
  }
}

Status ProbeProcessor::Drain(ThreadId thread_id, OperatorStatus& status) {
  if (NeedToScan(join_type_)) {
    // No need to drain now, scan will output the remaining rows in materialize.
    status = OperatorStatus::Finished(std::nullopt);
    return Status::OK();
  }

  std::optional<ExecBatch> output;
  if (local_states_[thread_id].materialize->num_rows() > 0) {
    ARRA_SET_AND_RETURN_NOT_OK(
        local_states_[thread_id].materialize->Flush([&](ExecBatch batch) {
          output.emplace(std::move(batch));
          return Status::OK();
        }),
        { status = OperatorStatus::Other(__s); });
  }
  status = OperatorStatus::Finished(std::move(output));

  return Status::OK();
}

Status ProbeProcessor::Probe(ThreadId thread_id, std::optional<ExecBatch> input,
                             TempVectorStack* temp_stack,
                             std::vector<KeyColumnArray>* temp_column_arrays,
                             OperatorStatus& status, const JoinFn& join_fn) {
  // Process.
  std::optional<ExecBatch> output;
  State state_next = State::CLEAN;
  switch (local_states_[thread_id].state) {
    case State::CLEAN: {
      // Some check.
      ARRA_DCHECK(!local_states_[thread_id].input.has_value());

      // Prepare input.
      ExecBatch batch;
      ARROW_ASSIGN_OR_RAISE(batch, KeyPayloadFromInput(schema_, pool_, &input.value()));

      auto batch_length = input->length;
      local_states_[thread_id].input.emplace(
          Input{std::move(batch), ExecBatch({}, batch_length), 0, {}});
      local_states_[thread_id].input->key_batch.values.resize(num_key_columns_);
      for (int i = 0; i < num_key_columns_; ++i) {
        local_states_[thread_id].input->key_batch.values[i] =
            local_states_[thread_id].input->batch.values[i];
      }

      // Process input.
      ARRA_SET_AND_RETURN_NOT_OK(
          join_fn(thread_id, temp_stack, temp_column_arrays, output, state_next),
          { status = OperatorStatus::Other(__s); });

      break;
    }
    case State::MINIBATCH_HAS_MORE:
    case State::MATCH_HAS_MORE: {
      // Some check.
      ARRA_DCHECK(local_states_[thread_id].input.has_value());

      // Process input.
      ARRA_SET_AND_RETURN_NOT_OK(
          join_fn(thread_id, temp_stack, temp_column_arrays, output, state_next),
          { status = OperatorStatus::Other(__s); });

      break;
    }
  }

  // Transition.
  switch (state_next) {
    case State::CLEAN: {
      local_states_[thread_id].input.reset();
      status = OperatorStatus::HasOutput(std::move(output));
      break;
    }
    case State::MINIBATCH_HAS_MORE:
    case State::MATCH_HAS_MORE: {
      status = OperatorStatus::HasMoreOutput(std::move(output));
      break;
    }
  }

  local_states_[thread_id].state = state_next;

  return Status::OK();
}

Status ProbeProcessor::InnerOuter(ThreadId thread_id, TempVectorStack* temp_stack,
                                  std::vector<KeyColumnArray>* temp_column_arrays,
                                  std::optional<ExecBatch>& output, State& state_next) {
  int num_rows = static_cast<int>(local_states_[thread_id].input->batch.length);
  state_next = State::CLEAN;
  bool match_has_more_last = local_states_[thread_id].state == State::MATCH_HAS_MORE;

  // Break into minibatches
  for (; local_states_[thread_id].input->minibatch_start < num_rows &&
         state_next == State::CLEAN;) {
    uint32_t minibatch_size_next =
        std::min(MiniBatch::kMiniBatchLength,
                 num_rows - local_states_[thread_id].input->minibatch_start);
    bool no_duplicate_keys = (hash_table_->key_to_payload() == nullptr);
    bool no_payload_columns = (hash_table_->payloads() == nullptr);

    if (!match_has_more_last) {
      // Calculate hash and matches for this minibatch.
      SwissTableWithKeys::Input hash_table_input(
          &local_states_[thread_id].input->key_batch,
          local_states_[thread_id].input->minibatch_start,
          local_states_[thread_id].input->minibatch_start + minibatch_size_next,
          temp_stack, temp_column_arrays);
      hash_table_->keys()->Hash(
          &hash_table_input, local_states_[thread_id].hashes_buf_data(), hardware_flags_);
      hash_table_->keys()->MapReadOnly(
          &hash_table_input, local_states_[thread_id].hashes_buf_data(),
          local_states_[thread_id].match_bitvector_buf_data(),
          local_states_[thread_id].key_ids_buf_data());

      // AND bit vector with null key filter for join
      bool ignored;
      JoinNullFilter::Filter(local_states_[thread_id].input->key_batch,
                             local_states_[thread_id].input->minibatch_start,
                             minibatch_size_next, *cmp_, &ignored,
                             /*and_with_input=*/true,
                             local_states_[thread_id].match_bitvector_buf_data());

      // We need to output matching pairs of rows from both sides of the join.
      // Since every hash table lookup for an input row might have multiple
      // matches we use a helper class that implements enumerating all of them.
      local_states_[thread_id].input->match_iterator.SetLookupResult(
          minibatch_size_next, local_states_[thread_id].input->minibatch_start,
          local_states_[thread_id].match_bitvector_buf_data(),
          local_states_[thread_id].key_ids_buf_data(), no_duplicate_keys,
          hash_table_->key_to_payload());
    }

    int num_matches_next;
    while (state_next != State::MATCH_HAS_MORE &&
           local_states_[thread_id].input->match_iterator.GetNextBatch(
               MiniBatch::kMiniBatchLength, &num_matches_next,
               local_states_[thread_id].materialize_batch_ids_buf_data(),
               local_states_[thread_id].materialize_key_ids_buf_data(),
               local_states_[thread_id].materialize_payload_ids_buf_data())) {
      const uint16_t* materialize_batch_ids =
          local_states_[thread_id].materialize_batch_ids_buf_data();
      const uint32_t* materialize_key_ids =
          local_states_[thread_id].materialize_key_ids_buf_data();
      const uint32_t* materialize_payload_ids =
          no_duplicate_keys || no_payload_columns
              ? local_states_[thread_id].materialize_key_ids_buf_data()
              : local_states_[thread_id].materialize_payload_ids_buf_data();

      // For right-outer, full-outer joins we need to update has-match flags
      // for the rows in hash table.
      if (join_type_ == JoinType::RIGHT_OUTER || join_type_ == JoinType::FULL_OUTER) {
        hash_table_->UpdateHasMatchForKeys(thread_id, num_matches_next,
                                           materialize_key_ids);
      }

      size_t rows_appended = 0;

      // If we are to exceed the maximum number of rows per batch, output.
      if (local_states_[thread_id].materialize->num_rows() + num_matches_next >
          kMaxRowsPerBatch) {
        rows_appended =
            kMaxRowsPerBatch - local_states_[thread_id].materialize->num_rows();
        num_matches_next -= rows_appended;
        int ignored;
        ARRA_RETURN_NOT_OK(local_states_[thread_id].materialize->Append(
            local_states_[thread_id].input->batch, rows_appended, materialize_batch_ids,
            materialize_key_ids, materialize_payload_ids, &ignored));
        ARRA_RETURN_NOT_OK(
            local_states_[thread_id].materialize->Flush([&](ExecBatch batch) {
              output.emplace(std::move(batch));
              return Status::OK();
            }));
        state_next = State::MATCH_HAS_MORE;
      }

      // Call materialize for resulting id tuples pointing to matching pairs
      // of rows.
      if (num_matches_next > 0) {
        int ignored;
        ARRA_RETURN_NOT_OK(local_states_[thread_id].materialize->Append(
            local_states_[thread_id].input->batch, num_matches_next,
            materialize_batch_ids + rows_appended, materialize_key_ids + rows_appended,
            materialize_payload_ids + rows_appended, &ignored));
      }
    }

    if (state_next != State::MATCH_HAS_MORE) {
      match_has_more_last = false;

      // For left-outer and full-outer joins output non-matches.
      //
      // Call materialize. Nulls will be output in all columns that come from
      // the other side of the join.
      //
      if (join_type_ == JoinType::LEFT_OUTER || join_type_ == JoinType::FULL_OUTER) {
        int num_passing_ids = 0;
        bit_util::bits_to_indexes(
            /*bit_to_search=*/0, hardware_flags_, minibatch_size_next,
            local_states_[thread_id].match_bitvector_buf_data(), &num_passing_ids,
            local_states_[thread_id].materialize_batch_ids_buf_data());

        // Add base batch row index.
        for (int i = 0; i < num_passing_ids; ++i) {
          local_states_[thread_id].materialize_batch_ids_buf_data()[i] +=
              static_cast<uint16_t>(local_states_[thread_id].input->minibatch_start);
        }

        size_t rows_appended = 0;

        // If we are to exceed the maximum number of rows per batch, output.
        if (local_states_[thread_id].materialize->num_rows() + num_passing_ids >
            kMaxRowsPerBatch) {
          rows_appended =
              kMaxRowsPerBatch - local_states_[thread_id].materialize->num_rows();
          num_passing_ids -= rows_appended;
          int ignored;
          ARRA_RETURN_NOT_OK(local_states_[thread_id].materialize->AppendProbeOnly(
              local_states_[thread_id].input->batch, rows_appended,
              local_states_[thread_id].materialize_batch_ids_buf_data(), &ignored));
          ARRA_RETURN_NOT_OK(
              local_states_[thread_id].materialize->Flush([&](ExecBatch batch) {
                output.emplace(std::move(batch));
                return Status::OK();
              }));
          state_next = State::MINIBATCH_HAS_MORE;
        }

        if (num_passing_ids > 0) {
          int ignored;
          ARRA_RETURN_NOT_OK(local_states_[thread_id].materialize->AppendProbeOnly(
              local_states_[thread_id].input->batch, num_passing_ids,
              local_states_[thread_id].materialize_batch_ids_buf_data() + rows_appended,
              &ignored));
        }
      }

      local_states_[thread_id].input->minibatch_start += minibatch_size_next;
    }
  }

  return Status::OK();
}

Status ProbeProcessor::LeftSemiAnti(ThreadId thread_id, TempVectorStack* temp_stack,
                                    std::vector<KeyColumnArray>* temp_column_arrays,
                                    std::optional<ExecBatch>& output, State& state_next) {
  int num_rows = static_cast<int>(local_states_[thread_id].input->batch.length);
  state_next = State::CLEAN;

  // Break into minibatches
  for (; local_states_[thread_id].input->minibatch_start < num_rows &&
         state_next == State::CLEAN;) {
    uint32_t minibatch_size_next =
        std::min(MiniBatch::kMiniBatchLength,
                 num_rows - local_states_[thread_id].input->minibatch_start);

    // Calculate hash and matches for this minibatch.
    {
      SwissTableWithKeys::Input hash_table_input(
          &local_states_[thread_id].input->key_batch,
          local_states_[thread_id].input->minibatch_start,
          local_states_[thread_id].input->minibatch_start + minibatch_size_next,
          temp_stack, temp_column_arrays);
      hash_table_->keys()->Hash(
          &hash_table_input, local_states_[thread_id].hashes_buf_data(), hardware_flags_);
      hash_table_->keys()->MapReadOnly(
          &hash_table_input, local_states_[thread_id].hashes_buf_data(),
          local_states_[thread_id].match_bitvector_buf_data(),
          local_states_[thread_id].key_ids_buf_data());
    }

    // AND bit vector with null key filter for join
    {
      bool ignored;
      JoinNullFilter::Filter(local_states_[thread_id].input->key_batch,
                             local_states_[thread_id].input->minibatch_start,
                             minibatch_size_next, *cmp_, &ignored,
                             /*and_with_input=*/true,
                             local_states_[thread_id].match_bitvector_buf_data());
    }

    int num_passing_ids = 0;
    bit_util::bits_to_indexes(
        (join_type_ == JoinType::LEFT_ANTI) ? 0 : 1, hardware_flags_, minibatch_size_next,
        local_states_[thread_id].match_bitvector_buf_data(), &num_passing_ids,
        local_states_[thread_id].materialize_batch_ids_buf_data());

    // Add base batch row index.
    for (int i = 0; i < num_passing_ids; ++i) {
      local_states_[thread_id].materialize_batch_ids_buf_data()[i] +=
          static_cast<uint16_t>(local_states_[thread_id].input->minibatch_start);
    }

    size_t rows_appended = 0;

    // If we are to exceed the maximum number of rows per batch, output.
    if (local_states_[thread_id].materialize->num_rows() + num_passing_ids >
        kMaxRowsPerBatch) {
      rows_appended = kMaxRowsPerBatch - local_states_[thread_id].materialize->num_rows();
      num_passing_ids -= rows_appended;
      int ignored;
      ARRA_RETURN_NOT_OK(local_states_[thread_id].materialize->AppendProbeOnly(
          local_states_[thread_id].input->batch, rows_appended,
          local_states_[thread_id].materialize_batch_ids_buf_data(), &ignored));
      ARRA_RETURN_NOT_OK(
          local_states_[thread_id].materialize->Flush([&](ExecBatch batch) {
            output.emplace(std::move(batch));
            return Status::OK();
          }));
      state_next = State::MINIBATCH_HAS_MORE;
    }

    if (num_passing_ids > 0) {
      int ignored;
      ARRA_RETURN_NOT_OK(local_states_[thread_id].materialize->AppendProbeOnly(
          local_states_[thread_id].input->batch, num_passing_ids,
          local_states_[thread_id].materialize_batch_ids_buf_data() + rows_appended,
          &ignored));
    }

    local_states_[thread_id].input->minibatch_start += minibatch_size_next;
  }

  return Status::OK();
}

Status ProbeProcessor::RightSemiAnti(ThreadId thread_id, TempVectorStack* temp_stack,
                                     std::vector<KeyColumnArray>* temp_column_arrays,
                                     std::optional<ExecBatch>& output,
                                     State& state_next) {
  int num_rows = static_cast<int>(local_states_[thread_id].input->batch.length);
  state_next = State::CLEAN;

  // Break into minibatches
  for (; local_states_[thread_id].input->minibatch_start < num_rows;) {
    uint32_t minibatch_size_next =
        std::min(MiniBatch::kMiniBatchLength,
                 num_rows - local_states_[thread_id].input->minibatch_start);

    // Calculate hash and matches for this minibatch.
    {
      SwissTableWithKeys::Input hash_table_input(
          &local_states_[thread_id].input->key_batch,
          local_states_[thread_id].input->minibatch_start,
          local_states_[thread_id].input->minibatch_start + minibatch_size_next,
          temp_stack, temp_column_arrays);
      hash_table_->keys()->Hash(
          &hash_table_input, local_states_[thread_id].hashes_buf_data(), hardware_flags_);
      hash_table_->keys()->MapReadOnly(
          &hash_table_input, local_states_[thread_id].hashes_buf_data(),
          local_states_[thread_id].match_bitvector_buf_data(),
          local_states_[thread_id].key_ids_buf_data());
    }

    // AND bit vector with null key filter for join
    {
      bool ignored;
      JoinNullFilter::Filter(local_states_[thread_id].input->key_batch,
                             local_states_[thread_id].input->minibatch_start,
                             minibatch_size_next, *cmp_, &ignored,
                             /*and_with_input=*/true,
                             local_states_[thread_id].match_bitvector_buf_data());
    }

    int num_passing_ids = 0;
    bit_util::bits_to_indexes(
        (join_type_ == JoinType::LEFT_ANTI) ? 0 : 1, hardware_flags_, minibatch_size_next,
        local_states_[thread_id].match_bitvector_buf_data(), &num_passing_ids,
        local_states_[thread_id].materialize_batch_ids_buf_data());

    // For right-semi, right-anti joins: update has-match flags for the rows
    // in hash table.
    for (int i = 0; i < num_passing_ids; ++i) {
      uint16_t id = local_states_[thread_id].materialize_batch_ids_buf_data()[i];
      local_states_[thread_id].key_ids_buf_data()[i] =
          local_states_[thread_id].key_ids_buf_data()[id];
    }
    hash_table_->UpdateHasMatchForKeys(thread_id, num_passing_ids,
                                       local_states_[thread_id].key_ids_buf_data());

    local_states_[thread_id].input->minibatch_start += minibatch_size_next;
  }

  return Status::OK();
}

Status ScanProcessor::Init(JoinType join_type, SwissTableForJoin* hash_table,
                           const std::vector<JoinResultMaterialize*>& materialize) {
  dop_ = materialize.size();
  join_type_ = join_type;
  hash_table_ = hash_table;

  local_states_.resize(materialize.size());
  for (int i = 0; i < materialize.size(); i++) {
    local_states_[i].materialize = materialize[i];
  }

  return Status::OK();
}

Status ScanProcessor::StartScan() {
  hash_table_->MergeHasMatch();

  num_rows_per_thread_ = CeilDiv(hash_table_->num_rows(), dop_);

  return Status::OK();
}

Status ScanProcessor::Scan(ThreadId thread_id, TempVectorStack* temp_stack,
                           OperatorStatus& status) {
  // Should we output matches or non-matches?
  //
  bool bit_to_output = (join_type_ == JoinType::RIGHT_SEMI);

  int64_t start_row =
      num_rows_per_thread_ * thread_id + local_states_[thread_id].current_start_;
  int64_t end_row = std::min(num_rows_per_thread_ * (thread_id + 1),
                             static_cast<size_t>(hash_table_->num_rows()));

  if (start_row >= end_row) {
    std::optional<ExecBatch> output;
    if (local_states_[thread_id].materialize->num_rows() > 0) {
      ARRA_SET_AND_RETURN_NOT_OK(
          local_states_[thread_id].materialize->Flush([&](ExecBatch batch) {
            output.emplace(std::move(batch));
            return Status::OK();
          }),
          { status = OperatorStatus::Other(__s); });
    }
    status = OperatorStatus::Finished(std::move(output));
    return Status::OK();
  }

  // Split into mini-batches
  //
  auto payload_ids_buf =
      TempVectorHolder<uint32_t>(temp_stack, MiniBatch::kMiniBatchLength);
  auto key_ids_buf = TempVectorHolder<uint32_t>(temp_stack, MiniBatch::kMiniBatchLength);
  auto selection_buf =
      TempVectorHolder<uint16_t>(temp_stack, MiniBatch::kMiniBatchLength);
  bool has_output = false;
  for (int64_t mini_batch_start = start_row; mini_batch_start < end_row && !has_output;) {
    // Compute the size of the next mini-batch
    //
    int64_t mini_batch_size_next =
        std::min(end_row - mini_batch_start,
                 static_cast<int64_t>(arrow::util::MiniBatch::kMiniBatchLength));

    // Get the list of key and payload ids from this mini-batch to output.
    //
    uint32_t first_key_id =
        hash_table_->payload_id_to_key_id(static_cast<uint32_t>(mini_batch_start));
    uint32_t last_key_id = hash_table_->payload_id_to_key_id(
        static_cast<uint32_t>(mini_batch_start + mini_batch_size_next - 1));
    int num_output_rows = 0;
    for (uint32_t key_id = first_key_id; key_id <= last_key_id; ++key_id) {
      if (GetBit(hash_table_->has_match(), key_id) == bit_to_output) {
        uint32_t first_payload_for_key =
            std::max(static_cast<uint32_t>(mini_batch_start),
                     hash_table_->key_to_payload() ? hash_table_->key_to_payload()[key_id]
                                                   : key_id);
        uint32_t last_payload_for_key = std::min(
            static_cast<uint32_t>(mini_batch_start + mini_batch_size_next - 1),
            hash_table_->key_to_payload() ? hash_table_->key_to_payload()[key_id + 1] - 1
                                          : key_id);
        uint32_t num_payloads_for_key = last_payload_for_key - first_payload_for_key + 1;
        for (uint32_t i = 0; i < num_payloads_for_key; ++i) {
          key_ids_buf.mutable_data()[num_output_rows + i] = key_id;
          payload_ids_buf.mutable_data()[num_output_rows + i] = first_payload_for_key + i;
        }
        num_output_rows += num_payloads_for_key;
      }
    }

    size_t rows_appended = 0;

    // If we are to exceed the maximum number of rows per batch, output.
    if (local_states_[thread_id].materialize->num_rows() + num_output_rows >
        kMaxRowsPerBatch) {
      rows_appended = kMaxRowsPerBatch - local_states_[thread_id].materialize->num_rows();
      num_output_rows -= rows_appended;
      int ignored;
      ARRA_SET_AND_RETURN_NOT_OK(local_states_[thread_id].materialize->AppendBuildOnly(
                                     rows_appended, key_ids_buf.mutable_data(),
                                     payload_ids_buf.mutable_data(), &ignored),
                                 { status = OperatorStatus::Other(__s); });

      std::optional<ExecBatch> output;
      ARRA_SET_AND_RETURN_NOT_OK(
          local_states_[thread_id].materialize->Flush([&](ExecBatch batch) {
            output.emplace(std::move(batch));
            return Status::OK();
          }),
          { status = OperatorStatus::Other(__s); });
      status = OperatorStatus::HasMoreOutput(std::move(output));
      has_output = true;
    }

    if (num_output_rows > 0) {
      // Materialize (and output whenever buffers get full) hash table
      // values according to the generated list of ids.
      //
      int ignored;
      ARRA_SET_AND_RETURN_NOT_OK(
          local_states_[thread_id].materialize->AppendBuildOnly(
              num_output_rows, key_ids_buf.mutable_data() + rows_appended,
              payload_ids_buf.mutable_data() + rows_appended, &ignored),
          { status = OperatorStatus::Other(__s); });
    }

    mini_batch_start += mini_batch_size_next;
    local_states_[thread_id].current_start_ += mini_batch_size_next;
  }

  if (!has_output) {
    // I know, this is weird.
    status = OperatorStatus::HasOutput(std::nullopt);
  }

  return Status::OK();
}

Status HashJoin::Init(QueryContext* ctx, size_t dop, const HashJoinNodeOptions& options,
                      const Schema& left_schema, const Schema& right_schema) {
  ctx_ = ctx;
  hardware_flags_ = ctx->cpu_info()->hardware_flags();
  pool_ = ctx->memory_pool();
  dop_ = dop;

  join_type_ = options.join_type;
  key_cmp_ = options.key_cmp;

  ARRA_RETURN_NOT_OK(ValidateHashJoinNodeOptions(options));

  if (options.output_all) {
    ARRA_RETURN_NOT_OK(schema_mgr_.Init(options.join_type, left_schema, options.left_keys,
                                        right_schema, options.right_keys, options.filter,
                                        options.output_suffix_for_left,
                                        options.output_suffix_for_right));
  } else {
    ARRA_RETURN_NOT_OK(schema_mgr_.Init(
        options.join_type, left_schema, options.left_keys, options.left_output,
        right_schema, options.right_keys, options.right_output, options.filter,
        options.output_suffix_for_left, options.output_suffix_for_right));
  }

  schema_[0] = &schema_mgr_.proj_maps[0];
  schema_[1] = &schema_mgr_.proj_maps[1];

  output_schema_ = schema_mgr_.MakeOutputSchema("", "");

  // Initialize thread local states and associated probe processors.
  local_states_.resize(dop_);
  for (int i = 0; i < dop_; ++i) {
    local_states_[i].materialize.Init(pool_, schema_[0], schema_[1]);
  }

  std::vector<JoinResultMaterialize*> materialize;
  materialize.resize(dop_);
  for (int i = 0; i < dop_; ++i) {
    materialize[i] = &local_states_[i].materialize;
  }
  ARRA_RETURN_NOT_OK(probe_processor_.Init(
      hardware_flags_, pool_, schema_[0]->num_cols(HashJoinProjection::KEY), schema_[0],
      join_type_, &key_cmp_, &hash_table_, materialize));

  return Status::OK();
}

std::shared_ptr<Schema> HashJoin::OutputSchema() const { return output_schema_; }

std::unique_ptr<HashJoinBuildSink> HashJoin::BuildSink() {
  std::vector<JoinResultMaterialize*> materialize;
  materialize.resize(dop_);
  for (int i = 0; i < dop_; ++i) {
    materialize[i] = &local_states_[i].materialize;
  }
  std::unique_ptr<HashJoinBuildSink> build_sink = std::make_unique<HashJoinBuildSink>();
  ARRA_DCHECK_OK(build_sink->Init(ctx_, dop_, hardware_flags_, pool_, schema_[1],
                                  join_type_, &hash_table_build_, &hash_table_,
                                  materialize));
  return build_sink;
}

PipelineTaskPipe HashJoin::ProbePipe() {
  return [&](ThreadId thread_id, std::optional<arrow::ExecBatch> input,
             OperatorStatus& status) {
    ARROW_ASSIGN_OR_RAISE(TempVectorStack * temp_stack, ctx_->GetTempStack(thread_id));
    auto temp_column_arrays = &local_states_[thread_id].temp_column_arrays;
    return probe_processor_.Probe(thread_id, std::move(input), temp_stack,
                                  temp_column_arrays, status);
  };
}

std::optional<PipelineTaskPipe> HashJoin::ProbeDrain() {
  return
      [&](ThreadId thread_id, std::optional<arrow::ExecBatch>, OperatorStatus& status) {
        return probe_processor_.Drain(thread_id, status);
      };
}

std::unique_ptr<HashJoinScanSource> HashJoin::ScanSource() {
  if (!NeedToScan(join_type_)) {
    return nullptr;
  }
  std::unique_ptr<HashJoinScanSource> scan_source =
      std::make_unique<HashJoinScanSource>();
  std::vector<JoinResultMaterialize*> materialize;
  materialize.resize(dop_);
  for (int i = 0; i < dop_; ++i) {
    materialize[i] = &local_states_[i].materialize;
  }
  ARRA_DCHECK_OK(scan_source->Init(ctx_, join_type_, &hash_table_, materialize));
  return scan_source;
}

Status HashJoinBuildSink::Init(QueryContext* ctx, size_t dop, int64_t hardware_flags,
                               MemoryPool* pool, const HashJoinProjectionMaps* schema,
                               JoinType join_type,
                               SwissTableForJoinBuild* hash_table_build,
                               SwissTableForJoin* hash_table,
                               const std::vector<JoinResultMaterialize*>& materialize) {
  ctx_ = ctx;
  dop_ = dop;
  hash_table_build_ = hash_table_build;
  return build_processor_.Init(hardware_flags, pool, schema, join_type, hash_table_build,
                               hash_table, &build_side_batches_, materialize);
}

PipelineTaskPipe HashJoinBuildSink::Pipe() {
  return [&](ThreadId thread_id, std::optional<arrow::ExecBatch> input,
             OperatorStatus& status) {
    status = OperatorStatus::HasOutput(std::nullopt);
    if (!input.has_value()) {
      return Status::OK();
    }
    std::lock_guard<std::mutex> guard(build_side_mutex_);
    build_side_batches_.InsertBatch(std::move(input.value()));
    return Status::OK();
  };
}

TaskGroups HashJoinBuildSink::Frontend() {
  ARRA_DCHECK_OK(build_processor_.StartBuild());

  auto build_task = [&](TaskId task_id, OperatorStatus& status) {
    ARROW_ASSIGN_OR_RAISE(TempVectorStack * temp_stack, ctx_->GetTempStack(task_id));
    return build_processor_.Build(task_id, temp_stack, status);
  };
  auto build_task_cont = [&]() {
    ARRA_RETURN_NOT_OK(build_processor_.FinishBuild());
    return build_processor_.StartMerge();
  };

  auto merge_task = [&](TaskId task_id, OperatorStatus& status) {
    ARROW_ASSIGN_OR_RAISE(TempVectorStack * temp_stack, ctx_->GetTempStack(task_id));
    return build_processor_.Merge(task_id, temp_stack, status);
  };
  auto num_merge_tasks = hash_table_build_->num_prtns();

  auto finish_merge_task = [&](TaskId task_id, OperatorStatus& status) {
    ARROW_ASSIGN_OR_RAISE(TempVectorStack * temp_stack, ctx_->GetTempStack(task_id));
    return build_processor_.FinishMerge(temp_stack);
  };

  return {
      {std::move(build_task), dop_, std::move(build_task_cont)},
      {std::move(merge_task), num_merge_tasks, std::nullopt},
      {std::move(finish_merge_task), 1, std::nullopt},
  };
}

TaskGroups HashJoinBuildSink::Backend() { return {}; }

Status HashJoinScanSource::Init(QueryContext* ctx, JoinType join_type,
                                SwissTableForJoin* hash_table,
                                const std::vector<JoinResultMaterialize*>& materializ) {
  ctx_ = ctx;

  return scan_processor_.Init(join_type, hash_table, materializ);
}

PipelineTaskSource HashJoinScanSource::Source() {
  return [&](ThreadId thread_id, OperatorStatus& status) {
    ARROW_ASSIGN_OR_RAISE(TempVectorStack * temp_stack, ctx_->GetTempStack(thread_id));
    return scan_processor_.Scan(thread_id, temp_stack, status);
  };
}

TaskGroups HashJoinScanSource::Frontend() {
  auto start_scan_task = [&](TaskId, OperatorStatus& status) {
    status = OperatorStatus::Finished(std::nullopt);
    return scan_processor_.StartScan();
  };

  return {{std::move(start_scan_task), 1, std::nullopt}};
}

TaskGroups HashJoinScanSource::Backend() { return {}; }

}  // namespace arra::sketch::detail

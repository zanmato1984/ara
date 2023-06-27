#include "sketch_hash_join.h"

namespace arra::detail {

static constexpr size_t kMaxRowsPerBatch = 4096;

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

Status BuildProcessor::Init(int64_t hardware_flags, MemoryPool* pool, size_t dop,
                            const HashJoinProjectionMaps* schema, JoinType join_type,
                            SwissTableForJoinBuild* hash_table_build,
                            SwissTableForJoin* hash_table, AccumulationQueue* batches,
                            std::vector<JoinResultMaterialize*> materialize) {
  hardware_flags_ = hardware_flags;
  pool_ = pool;
  dop_ = dop;

  schema_ = schema;
  join_type_ = join_type;
  hash_table_build_ = hash_table_build;
  hash_table_ = hash_table;
  batches_ = batches;
  materialize_ = std::move(materialize);

  local_states_.resize(dop_);
  for (int i = 0; i < dop_; ++i) {
    local_states_[i].round = 0;
  }

  return Status::OK();
}

Status BuildProcessor::InitHashTable() {
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
    status = OperatorStatus::Finished();
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
  return hash_table_build_->PreparePrtnMerge();
}

Status BuildProcessor::Merge(ThreadId thread_id, TempVectorStack* temp_stack,
                             OperatorStatus& status) {
  status = OperatorStatus::HasOutput(std::nullopt);
  hash_table_build_->PrtnMerge(static_cast<int>(thread_id));
  return Status::OK();
}

Status BuildProcessor::FinishMerge(TempVectorStack* temp_stack) {
  hash_table_build_->FinishPrtnMerge(temp_stack);

  for (int i = 0; i < materialize_.size(); ++i) {
    materialize_[i]->SetBuildSide(hash_table_->keys()->keys(), hash_table_->payloads(),
                                  hash_table_->key_to_payload() == nullptr);
  }

  return Status::OK();
}

Status ProbeProcessor::Init(int64_t hardware_flags, int num_key_columns,
                            JoinType join_type, const std::vector<JoinKeyCmp>* cmp,
                            SwissTableForJoin* hash_table,
                            std::vector<JoinResultMaterialize*> materialize) {
  hardware_flags_ = hardware_flags;

  num_key_columns_ = num_key_columns;
  join_type_ = join_type;
  cmp_ = cmp;

  hash_table_ = hash_table;
  swiss_table_ = hash_table_->keys()->swiss_table();
  materialize_ = std::move(materialize);

  minibatch_size_ = swiss_table_->minibatch_size();

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

Status ProbeProcessor::InnerOuter(ThreadId thread_id, TempVectorStack* temp_stack,
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
        if (materialize_[thread_id]->num_rows() + num_matches_next > kMaxRowsPerBatch) {
          ARRA_RETURN_NOT_OK(materialize_[thread_id]->Flush([&](ExecBatch batch) {
            output.emplace(std::move(batch));
            return Status::OK();
          }));
          state_next = State::MATCH_HAS_MORE;
        }

        // Call materialize for resulting id tuples pointing to matching pairs
        // of rows.
        {
          int ignored;
          ARRA_RETURN_NOT_OK(materialize_[thread_id]->Append(
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
        if (materialize_[thread_id]->num_rows() + num_passing_ids > kMaxRowsPerBatch) {
          ARRA_RETURN_NOT_OK(materialize_[thread_id]->Flush([&](ExecBatch batch) {
            output.emplace(std::move(batch));
            return Status::OK();
          }));
          state_next = State::MINIBATCH_HAS_MORE;
        }

        {
          int ignored;
          ARRA_RETURN_NOT_OK(materialize_[thread_id]->AppendProbeOnly(
              input_->batch, num_passing_ids,
              input_->materialize_batch_ids_buf.mutable_data(), &ignored));
        }
      }

      input_->minibatch_start += minibatch_size_next;
    }
  }

  return Status::OK();
}

Status ProbeProcessor::LeftSemiAnti(ThreadId thread_id, TempVectorStack* temp_stack,
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
    if (materialize_[thread_id]->num_rows() + num_passing_ids > kMaxRowsPerBatch) {
      ARRA_RETURN_NOT_OK(materialize_[thread_id]->Flush([&](ExecBatch batch) {
        output.emplace(std::move(batch));
        return Status::OK();
      }));
      state_next = State::MINIBATCH_HAS_MORE;
    }

    {
      int ignored;
      ARRA_RETURN_NOT_OK(materialize_[thread_id]->AppendProbeOnly(
          input_->batch, num_passing_ids,
          input_->materialize_batch_ids_buf.mutable_data(), &ignored));
    }

    input_->minibatch_start += minibatch_size_next;
  }

  return Status::OK();
}

Status ProbeProcessor::RightSemiAnti(ThreadId thread_id, TempVectorStack* temp_stack,
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

Status HashJoinScanSource::Init(HashJoin* hash_join) {
  hash_join_ = hash_join;
  return Status::OK();
}

Status HashJoin::Init(QueryContext* ctx, JoinType join_type, size_t dop,
                      const HashJoinProjectionMaps* proj_map_left,
                      const HashJoinProjectionMaps* proj_map_right,
                      std::vector<JoinKeyCmp> key_cmp, Expression filter) {
  dop_ = dop;
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

  // Initialize thread local states and associated probe processors.
  local_states_.resize(dop_);
  for (int i = 0; i < dop_; ++i) {
    local_states_[i].materialize.Init(pool_, proj_map_left, proj_map_right);
  }

  std::vector<JoinResultMaterialize*> materialize;
  materialize.resize(dop_);
  for (int i = 0; i < dop_; ++i) {
    materialize[i] = &local_states_[i].materialize;
  }
  ARRA_RETURN_NOT_OK(build_processor_.Init(hardware_flags_, pool_, dop_, schema_[1],
                                           join_type_, &hash_table_build_, &hash_table_,
                                           &build_side_batches_, materialize));
  ARRA_RETURN_NOT_OK(probe_processor_.Init(
      hardware_flags_, proj_map_left->num_cols(HashJoinProjection::KEY), join_type_,
      &key_cmp_, &hash_table_, std::move(materialize)));

  if (join_type_ == JoinType::RIGHT_SEMI || join_type_ == JoinType::RIGHT_ANTI ||
      join_type_ == JoinType::RIGHT_OUTER || join_type_ == JoinType::FULL_OUTER) {
    scan_source_.emplace();
    ARRA_RETURN_NOT_OK(scan_source_->Init(this));
  }

  return Status::OK();
}

}  // namespace arra::detail

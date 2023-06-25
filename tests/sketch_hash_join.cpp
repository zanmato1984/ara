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

  auto minibatch_size = hash_table_->keys()->swiss_table()->minibatch_size();

  hashes_buf_ = arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size);
  match_bitvector_buf_ = arrow::util::TempVectorHolder<uint8_t>(
      temp_stack, static_cast<uint32_t>(bit_util::BytesForBits(minibatch_size)));
  key_ids_buf_ = arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size);
  materialize_batch_ids_buf_ =
      arrow::util::TempVectorHolder<uint16_t>(temp_stack, minibatch_size);
  materialize_key_ids_buf_ =
      arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size);
  materialize_payload_ids_buf_ =
      arrow::util::TempVectorHolder<uint32_t>(temp_stack, minibatch_size);
}

// std::optional<ExecBatch> ProbeProcessor::ProbeMinibatch(
//     int64_t thread_id, arrow::util::TempVectorStack* temp_stack,
//     std::vector<KeyColumnArray>* temp_column_arrays) {
//   if (!current_batch_.has_value()) {
//     return std::nullopt;
//   }

//   const ExecBatch& keypayload_batch = current_batch_.value();
//   const SwissTable* swiss_table = hash_table_->keys()->swiss_table();
//   int64_t hardware_flags = swiss_table->hardware_flags();
//   int minibatch_size = swiss_table->minibatch_size();
//   int num_rows = static_cast<int>(keypayload_batch.length);

//   ExecBatch key_batch({}, keypayload_batch.length);
//   key_batch.values.resize(num_key_columns_);
//   for (int i = 0; i < num_key_columns_; ++i) {
//     key_batch.values[i] = keypayload_batch.values[i];
//   }
//   uint32_t minibatch_size_next = std::min(minibatch_size, num_rows - minibatch_start_);

//   if (join_type_ != JoinType::LEFT_SEMI && join_type_ != JoinType::LEFT_ANTI &&
//       join_type_ != JoinType::RIGHT_SEMI && join_type_ != JoinType::RIGHT_ANTI) {
//     bool no_duplicate_keys = (hash_table_->key_to_payload() == nullptr);
//     bool no_payload_columns = (hash_table_->payloads() == nullptr);

//     if (!match_iterator_.has_value()) {
//       match_iterator_.emplace();
//       match_iterator_->SetLookupResult(minibatch_size_next, minibatch_start_,
//                                        match_bitvector_buf_.value().mutable_data(),
//                                        key_ids_buf_.value().mutable_data(),
//                                        no_duplicate_keys,
//                                        hash_table_->key_to_payload());
//     }

//     int num_matches_next;
//     bool has_next = (match_iterator_.value().GetNextBatch(
//         minibatch_size, &num_matches_next,
//         materialize_batch_ids_buf_.value().mutable_data(),
//         materialize_key_ids_buf_.value().mutable_data(),
//         materialize_payload_ids_buf_.value().mutable_data()));

//     if (has_next) {
//       const uint16_t* materialize_batch_ids =
//           materialize_batch_ids_buf_.value().mutable_data();
//       const uint32_t* materialize_key_ids =
//           materialize_key_ids_buf_.value().mutable_data();
//       const uint32_t* materialize_payload_ids =
//           no_duplicate_keys || no_payload_columns
//               ? materialize_key_ids_buf_.value().mutable_data()
//               : materialize_payload_ids_buf_.value().mutable_data();
//       // For right-outer, full-outer joins we need to update has-match flags
//       // for the rows in hash table.
//       //
//       if (join_type_ == JoinType::RIGHT_OUTER || join_type_ == JoinType::FULL_OUTER) {
//         hash_table_->UpdateHasMatchForKeys(thread_id, num_matches_next,
//                                            materialize_key_ids);
//       }

//       // Call materialize for resulting id tuples pointing to matching pairs
//       // of rows.
//       //
//       RETURN_NOT_OK(materialize_->Append(
//           keypayload_batch, num_matches_next, materialize_batch_ids,
//           materialize_key_ids, materialize_payload_ids, [&](ExecBatch batch) {
//             return output_batch_fn_(thread_id, std::move(batch));
//           }));
//     }
//   }
// }

// // Semi-joins
// //
// if (join_type_ == JoinType::LEFT_SEMI || join_type_ == JoinType::LEFT_ANTI ||
//     join_type_ == JoinType::RIGHT_SEMI || join_type_ == JoinType::RIGHT_ANTI) {
//   int num_passing_ids = 0;
//   arrow::util::bit_util::bits_to_indexes(
//       (join_type_ == JoinType::LEFT_ANTI) ? 0 : 1, hardware_flags, minibatch_size_next,
//       match_bitvector_buf_.value().mutable_data(), &num_passing_ids,
//       materialize_batch_ids_buf_.value().mutable_data());

//   // For right-semi, right-anti joins: update has-match flags for the rows
//   // in hash table.
//   //
//   if (join_type_ == JoinType::RIGHT_SEMI || join_type_ == JoinType::RIGHT_ANTI) {
//     for (int i = 0; i < num_passing_ids; ++i) {
//       uint16_t id = materialize_batch_ids_buf_.value().mutable_data()[i];
//       key_ids_buf_.value().mutable_data()[i] = key_ids_buf_.value().mutable_data()[id];
//     }
//     hash_table_->UpdateHasMatchForKeys(thread_id, num_passing_ids,
//                                        key_ids_buf_.value().mutable_data());
//   } else {
//     // For left-semi, left-anti joins: call materialize using match
//     // bit-vector.
//     //

//     // Add base batch row index.
//     //
//     for (int i = 0; i < num_passing_ids; ++i) {
//       materialize_batch_ids_buf_.value().mutable_data()[i] +=
//           static_cast<uint16_t>(minibatch_start);
//     }

//     RETURN_NOT_OK(materialize_->AppendProbeOnly(
//         keypayload_batch, num_passing_ids,
//         materialize_batch_ids_buf_.value().mutable_data(),
//         [&](ExecBatch batch) { return output_batch_fn_(thread_id, std::move(batch));
//         }));
//   }
// } else {
//   // We need to output matching pairs of rows from both sides of the join.
//   // Since every hash table lookup for an input row might have multiple
//   // matches we use a helper class that implements enumerating all of them.
//   //
//   bool no_duplicate_keys = (hash_table_->key_to_payload() == nullptr);
//   bool no_payload_columns = (hash_table_->payloads() == nullptr);
//   JoinMatchIterator match_iterator;
//   match_iterator.SetLookupResult(minibatch_size_next, minibatch_start,
//                                  match_bitvector_buf_.value().mutable_data(),
//                                  key_ids_buf_.value().mutable_data(),
//                                  no_duplicate_keys, hash_table_->key_to_payload());
//   int num_matches_next;
//   while (
//       match_iterator.GetNextBatch(minibatch_size, &num_matches_next,
//                                   materialize_batch_ids_buf_.value().mutable_data(),
//                                   materialize_key_ids_buf_.value().mutable_data(),
//                                   materialize_payload_ids_buf_.value().mutable_data()))
//                                   {
//     const uint16_t* materialize_batch_ids =
//         materialize_batch_ids_buf_.value().mutable_data();
//     const uint32_t* materialize_key_ids =
//     materialize_key_ids_buf_.value().mutable_data(); const uint32_t*
//     materialize_payload_ids =
//         no_duplicate_keys || no_payload_columns
//             ? materialize_key_ids_buf_.value().mutable_data()
//             : materialize_payload_ids_buf_.value().mutable_data();

//     // For right-outer, full-outer joins we need to update has-match flags
//     // for the rows in hash table.
//     //
//     if (join_type_ == JoinType::RIGHT_OUTER || join_type_ == JoinType::FULL_OUTER) {
//       hash_table_->UpdateHasMatchForKeys(thread_id, num_matches_next,
//                                          materialize_key_ids);
//     }

//     // Call materialize for resulting id tuples pointing to matching pairs
//     // of rows.
//     //
//     RETURN_NOT_OK(materialize_->Append(
//         keypayload_batch, num_matches_next, materialize_batch_ids, materialize_key_ids,
//         materialize_payload_ids,
//         [&](ExecBatch batch) { return output_batch_fn_(thread_id, std::move(batch));
//         }));
//   }

//   // For left-outer and full-outer joins output non-matches.
//   //
//   // Call materialize. Nulls will be output in all columns that come from
//   // the other side of the join.
//   //
//   if (join_type_ == JoinType::LEFT_OUTER || join_type_ == JoinType::FULL_OUTER) {
//     int num_passing_ids = 0;
//     arrow::util::bit_util::bits_to_indexes(
//         /*bit_to_search=*/0, hardware_flags, minibatch_size_next,
//         match_bitvector_buf_.value().mutable_data(), &num_passing_ids,
//         materialize_batch_ids_buf_.value().mutable_data());

//     // Add base batch row index.
//     //
//     for (int i = 0; i < num_passing_ids; ++i) {
//       materialize_batch_ids_buf_.value().mutable_data()[i] +=
//           static_cast<uint16_t>(minibatch_start);
//     }

//     RETURN_NOT_OK(materialize_->AppendProbeOnly(
//         keypayload_batch, num_passing_ids,
//         materialize_batch_ids_buf_.value().mutable_data(),
//         [&](ExecBatch batch) { return output_batch_fn_(thread_id, std::move(batch));
//         }));
//   }
// }

// minibatch_start_ += minibatch_size_next;
// if (minibatch_start_ >= num_rows) {
//   current_batch_ = std::nullopt;
//   minibatch_start_ = 0;
// }
// }

Status ProbeProcessor::ProbeBatch(int64_t thread_id, ExecBatch keypayload_batch,
                                  arrow::util::TempVectorStack* temp_stack,
                                  std::vector<KeyColumnArray>* temp_column_arrays) {
  const SwissTable* swiss_table = hash_table_->keys()->swiss_table();
  int64_t hardware_flags = swiss_table->hardware_flags();
  int minibatch_size = swiss_table->minibatch_size();
  int num_rows = static_cast<int>(keypayload_batch.length);

  ExecBatch key_batch({}, keypayload_batch.length);
  key_batch.values.resize(num_key_columns_);
  for (int i = 0; i < num_key_columns_; ++i) {
    key_batch.values[i] = keypayload_batch.values[i];
  }

  // Break into mini-batches

  for (int minibatch_start = 0; minibatch_start < num_rows;) {
    uint32_t minibatch_size_next = std::min(minibatch_size, num_rows - minibatch_start);

    SwissTableWithKeys::Input input(&key_batch, minibatch_start,
                                    minibatch_start + minibatch_size_next, temp_stack,
                                    temp_column_arrays);
    hash_table_->keys()->Hash(&input, hashes_buf_.value().mutable_data(), hardware_flags);
    hash_table_->keys()->MapReadOnly(&input, hashes_buf_.value().mutable_data(),
                                     match_bitvector_buf_.value().mutable_data(),
                                     key_ids_buf_.value().mutable_data());

    // AND bit vector with null key filter for join
    //
    bool ignored;
    JoinNullFilter::Filter(
        key_batch, minibatch_start, minibatch_size_next, *cmp_, &ignored,
        /*and_with_input=*/true, match_bitvector_buf_.value().mutable_data());
    // Semi-joins
    //
    if (join_type_ == JoinType::LEFT_SEMI || join_type_ == JoinType::LEFT_ANTI ||
        join_type_ == JoinType::RIGHT_SEMI || join_type_ == JoinType::RIGHT_ANTI) {
      int num_passing_ids = 0;
      arrow::util::bit_util::bits_to_indexes(
          (join_type_ == JoinType::LEFT_ANTI) ? 0 : 1, hardware_flags,
          minibatch_size_next, match_bitvector_buf_.value().mutable_data(),
          &num_passing_ids, materialize_batch_ids_buf_.value().mutable_data());

      // For right-semi, right-anti joins: update has-match flags for the rows
      // in hash table.
      //
      if (join_type_ == JoinType::RIGHT_SEMI || join_type_ == JoinType::RIGHT_ANTI) {
        for (int i = 0; i < num_passing_ids; ++i) {
          uint16_t id = materialize_batch_ids_buf_.value().mutable_data()[i];
          key_ids_buf_.value().mutable_data()[i] =
              key_ids_buf_.value().mutable_data()[id];
        }
        hash_table_->UpdateHasMatchForKeys(thread_id, num_passing_ids,
                                           key_ids_buf_.value().mutable_data());
      } else {
        // For left-semi, left-anti joins: call materialize using match
        // bit-vector.
        //

        // Add base batch row index.
        //
        for (int i = 0; i < num_passing_ids; ++i) {
          materialize_batch_ids_buf_.value().mutable_data()[i] +=
              static_cast<uint16_t>(minibatch_start);
        }

        RETURN_NOT_OK(materialize_->AppendProbeOnly(
            keypayload_batch, num_passing_ids,
            materialize_batch_ids_buf_.value().mutable_data(), [&](ExecBatch batch) {
              return output_batch_fn_(thread_id, std::move(batch));
            }));
      }
    } else {
      // We need to output matching pairs of rows from both sides of the join.
      // Since every hash table lookup for an input row might have multiple
      // matches we use a helper class that implements enumerating all of them.
      //
      bool no_duplicate_keys = (hash_table_->key_to_payload() == nullptr);
      bool no_payload_columns = (hash_table_->payloads() == nullptr);
      JoinMatchIterator match_iterator;
      match_iterator.SetLookupResult(minibatch_size_next, minibatch_start,
                                     match_bitvector_buf_.value().mutable_data(),
                                     key_ids_buf_.value().mutable_data(),
                                     no_duplicate_keys, hash_table_->key_to_payload());
      int num_matches_next;
      while (match_iterator.GetNextBatch(
          minibatch_size, &num_matches_next,
          materialize_batch_ids_buf_.value().mutable_data(),
          materialize_key_ids_buf_.value().mutable_data(),
          materialize_payload_ids_buf_.value().mutable_data())) {
        const uint16_t* materialize_batch_ids =
            materialize_batch_ids_buf_.value().mutable_data();
        const uint32_t* materialize_key_ids =
            materialize_key_ids_buf_.value().mutable_data();
        const uint32_t* materialize_payload_ids =
            no_duplicate_keys || no_payload_columns
                ? materialize_key_ids_buf_.value().mutable_data()
                : materialize_payload_ids_buf_.value().mutable_data();

        // For right-outer, full-outer joins we need to update has-match flags
        // for the rows in hash table.
        //
        if (join_type_ == JoinType::RIGHT_OUTER || join_type_ == JoinType::FULL_OUTER) {
          hash_table_->UpdateHasMatchForKeys(thread_id, num_matches_next,
                                             materialize_key_ids);
        }

        // Call materialize for resulting id tuples pointing to matching pairs
        // of rows.
        //
        RETURN_NOT_OK(materialize_->Append(
            keypayload_batch, num_matches_next, materialize_batch_ids,
            materialize_key_ids, materialize_payload_ids, [&](ExecBatch batch) {
              return output_batch_fn_(thread_id, std::move(batch));
            }));
      }

      // For left-outer and full-outer joins output non-matches.
      //
      // Call materialize. Nulls will be output in all columns that come from
      // the other side of the join.
      //
      if (join_type_ == JoinType::LEFT_OUTER || join_type_ == JoinType::FULL_OUTER) {
        int num_passing_ids = 0;
        arrow::util::bit_util::bits_to_indexes(
            /*bit_to_search=*/0, hardware_flags, minibatch_size_next,
            match_bitvector_buf_.value().mutable_data(), &num_passing_ids,
            materialize_batch_ids_buf_.value().mutable_data());

        // Add base batch row index.
        //
        for (int i = 0; i < num_passing_ids; ++i) {
          materialize_batch_ids_buf_.value().mutable_data()[i] +=
              static_cast<uint16_t>(minibatch_start);
        }

        RETURN_NOT_OK(materialize_->AppendProbeOnly(
            keypayload_batch, num_passing_ids,
            materialize_batch_ids_buf_.value().mutable_data(), [&](ExecBatch batch) {
              return output_batch_fn_(thread_id, std::move(batch));
            }));
      }
    }

    minibatch_start_ += minibatch_size_next;
  }

  return Status::OK();
}

Status ProbeProcessor::OnFinished() {
  // Flush all instances of materialize that have
  // non-zero accumulated output rows.
  //
  RETURN_NOT_OK(materialize_->Flush(
      [&](ExecBatch batch) { return output_batch_fn_(0, std::move(batch)); }));

  return Status::OK();
}

std::optional<ExecBatch> ProbeProcessor::LeftSemiOrAnti(
    int64_t thread_id, ExecBatch keypayload_batch,
    arrow::util::TempVectorStack* temp_stack,
    std::vector<KeyColumnArray>* temp_column_arrays) {
  const SwissTable* swiss_table = hash_table_->keys()->swiss_table();
  int64_t hardware_flags = swiss_table->hardware_flags();
  int minibatch_size = swiss_table->minibatch_size();
  int num_rows = static_cast<int>(keypayload_batch.length);

  ExecBatch key_batch({}, keypayload_batch.length);
  key_batch.values.resize(num_key_columns_);
  for (int i = 0; i < num_key_columns_; ++i) {
    key_batch.values[i] = keypayload_batch.values[i];
  }

  std::optional<ExecBatch> output;

  // Break into mini-batches
  for (; minibatch_start_ < num_rows;) {
    uint32_t minibatch_size_next = std::min(minibatch_size, num_rows - minibatch_start_);

    SwissTableWithKeys::Input input(&key_batch, minibatch_start_,
                                    minibatch_start_ + minibatch_size_next, temp_stack,
                                    temp_column_arrays);
    hash_table_->keys()->Hash(&input, hashes_buf_.value().mutable_data(), hardware_flags);
    hash_table_->keys()->MapReadOnly(&input, hashes_buf_.value().mutable_data(),
                                     match_bitvector_buf_.value().mutable_data(),
                                     key_ids_buf_.value().mutable_data());

    // AND bit vector with null key filter for join
    //
    bool ignored;
    JoinNullFilter::Filter(
        key_batch, minibatch_start_, minibatch_size_next, *cmp_, &ignored,
        /*and_with_input=*/true, match_bitvector_buf_.value().mutable_data());
    // Semi-joins
    //
    int num_passing_ids = 0;
    arrow::util::bit_util::bits_to_indexes(
        (join_type_ == JoinType::LEFT_ANTI) ? 0 : 1, hardware_flags, minibatch_size_next,
        match_bitvector_buf_.value().mutable_data(), &num_passing_ids,
        materialize_batch_ids_buf_.value().mutable_data());

    // For left-semi, left-anti joins: call materialize using match
    // bit-vector.
    //

    // Add base batch row index.
    //
    for (int i = 0; i < num_passing_ids; ++i) {
      materialize_batch_ids_buf_.value().mutable_data()[i] +=
          static_cast<uint16_t>(minibatch_start_);
    }

    if (materialize_->num_rows() + num_passing_ids > kMaxRowsPerBatch) {
      // Flush all instances of materialize that have
      // non-zero accumulated output rows.
      //
      DCHECK_OK(materialize_->Flush([&](ExecBatch batch) {
        output = std::move(batch);
        return Status::OK();
      }));
      last_unfinished_ = true;
    }

    int foo;
    DCHECK_OK(materialize_->AppendProbeOnly(
        keypayload_batch, num_passing_ids,
        materialize_batch_ids_buf_.value().mutable_data(), &foo));
    minibatch_start_ += minibatch_size_next;
    if (last_unfinished_) {
      break;
    }
  }

  if (!last_unfinished_) {
    // Flush all instances of materialize that have
    // non-zero accumulated output rows.
    //
    DCHECK_OK(materialize_->Flush([&](ExecBatch batch) {
      output = std::move(batch);
      return Status::OK();
    }));
  }

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

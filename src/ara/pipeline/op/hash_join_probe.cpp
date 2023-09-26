#include "hash_join_probe.h"
#include "hash_join.h"
#include "op_output.h"

#include <ara/pipeline/pipeline_context.h>
#include <ara/pipeline/pipeline_observer.h>
#include <ara/task/task_status.h>
#include <ara/util/util.h>

#include <arrow/acero/query_context.h>
#include <arrow/compute/util.h>

namespace ara::pipeline {

using namespace ara::task;
using namespace ara::util;

using namespace arrow;
using namespace arrow::acero;
using namespace arrow::bit_util;
using namespace arrow::compute;
using namespace arrow::util;

namespace detail {

bool NeedToScan(JoinType join_type) {
  return join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI ||
         join_type == JoinType::RIGHT_OUTER || join_type == JoinType::FULL_OUTER;
}

class ProbeProcessor {
 public:
  Status Init(const PipelineContext& pipeline_ctx, HashJoin* hash_join) {
    hardware_flags_ = hash_join->hardware_flags;
    pool_ = hash_join->pool;

    num_key_columns_ = hash_join->schema[0]->num_cols(HashJoinProjection::KEY);
    schema_ = hash_join->schema[0];
    join_type_ = hash_join->join_type;
    cmp_ = &hash_join->key_cmp;

    hash_table_ = &hash_join->hash_table;
    swiss_table_ = hash_join->hash_table.keys()->swiss_table();

    size_t dop = hash_join->dop;
    auto mini_batch_length = pipeline_ctx.query_ctx->options.mini_batch_length;
    thread_locals_.resize(dop);
    for (size_t i = 0; i < dop; ++i) {
      thread_locals_[i].materialize = &hash_join->materialize[i];

      thread_locals_[i].hashes_buf.resize(mini_batch_length);
      thread_locals_[i].match_bitvector_buf.resize(mini_batch_length);
      thread_locals_[i].key_ids_buf.resize(mini_batch_length);
      thread_locals_[i].materialize_batch_ids_buf.resize(mini_batch_length);
      thread_locals_[i].materialize_key_ids_buf.resize(mini_batch_length);
      thread_locals_[i].materialize_payload_ids_buf.resize(mini_batch_length);
    }

    return Status::OK();
  }

  OpResult Probe(const PipelineContext& pipeline_ctx, const TaskContext& task_ctx,
                 ThreadId thread_id, std::optional<Batch> input,
                 TempVectorStack* temp_stack,
                 std::vector<KeyColumnArray>* temp_column_arrays) {
    State state_next = State::CLEAN;
    switch (join_type_) {
      case JoinType::INNER:
      case JoinType::LEFT_OUTER:
      case JoinType::RIGHT_OUTER:
      case JoinType::FULL_OUTER: {
        return Probe(pipeline_ctx, task_ctx, thread_id, std::move(input), temp_stack,
                     temp_column_arrays,
                     [&](const PipelineContext& pipeline_ctx, const TaskContext& task_ctx,
                         ThreadId thread_id, TempVectorStack* temp_stack,
                         std::vector<KeyColumnArray>* temp_column_arrays,
                         std::optional<Batch>& output, State& state_next) -> Status {
                       return InnerOuter(pipeline_ctx, task_ctx, thread_id, temp_stack,
                                         temp_column_arrays, output, state_next);
                     });
      }
      case JoinType::LEFT_SEMI:
      case JoinType::LEFT_ANTI: {
        return Probe(pipeline_ctx, task_ctx, thread_id, std::move(input), temp_stack,
                     temp_column_arrays,
                     [&](const PipelineContext& pipeline_ctx, const TaskContext& task_ctx,
                         ThreadId thread_id, TempVectorStack* temp_stack,
                         std::vector<KeyColumnArray>* temp_column_arrays,
                         std::optional<Batch>& output, State& state_next) -> Status {
                       return LeftSemiAnti(pipeline_ctx, task_ctx, thread_id, temp_stack,
                                           temp_column_arrays, output, state_next);
                     });
      }
      case JoinType::RIGHT_SEMI:
      case JoinType::RIGHT_ANTI: {
        return Probe(pipeline_ctx, task_ctx, thread_id, std::move(input), temp_stack,
                     temp_column_arrays,
                     [&](const PipelineContext& pipeline_ctx, const TaskContext& task_ctx,
                         ThreadId thread_id, TempVectorStack* temp_stack,
                         std::vector<KeyColumnArray>* temp_column_arrays,
                         std::optional<Batch>& output, State& state_next) -> Status {
                       return RightSemiAnti(pipeline_ctx, task_ctx, thread_id, temp_stack,
                                            temp_column_arrays, output, state_next);
                     });
      }
    }
  }

  OpResult Drain(const PipelineContext& pipeline_ctx, const TaskContext& task_ctx,
                 ThreadId thread_id) {
    std::optional<Batch> output;
    if (thread_locals_[thread_id].materialize->num_rows() > 0) {
      ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->Flush([&](Batch batch) {
        output.emplace(std::move(batch));
        return Status::OK();
      }));
    }
    return OpOutput::Finished(std::move(output));
  }

 private:
  int64_t hardware_flags_;
  MemoryPool* pool_;

  int num_key_columns_;
  HashJoinProjectionMaps* schema_;
  JoinType join_type_;
  std::vector<JoinKeyCmp>* cmp_;

  SwissTableForJoin* hash_table_;
  SwissTable* swiss_table_;

 private:
  enum class State { CLEAN, MINI_BATCH_HAS_MORE, MATCH_HAS_MORE };

  struct Input {
    Batch batch;
    Batch key_batch;
    size_t mini_batch_start = 0;

    JoinMatchIterator match_iterator;
  };

  struct ThreadLocal {
    State state = State::CLEAN;
    std::optional<Input> input = std::nullopt;
    JoinResultMaterialize* materialize = nullptr;

    std::vector<uint32_t> hashes_buf;
    std::vector<uint8_t> match_bitvector_buf;
    std::vector<uint32_t> key_ids_buf;
    std::vector<uint16_t> materialize_batch_ids_buf;
    std::vector<uint32_t> materialize_key_ids_buf;
    std::vector<uint32_t> materialize_payload_ids_buf;

    uint32_t* hashes_buf_data() { return hashes_buf.data(); }
    uint8_t* match_bitvector_buf_data() { return match_bitvector_buf.data(); }
    uint32_t* key_ids_buf_data() { return key_ids_buf.data(); }
    uint16_t* materialize_batch_ids_buf_data() {
      return materialize_batch_ids_buf.data();
    }
    uint32_t* materialize_key_ids_buf_data() { return materialize_key_ids_buf.data(); }
    uint32_t* materialize_payload_ids_buf_data() {
      return materialize_payload_ids_buf.data();
    }
  };
  std::vector<ThreadLocal> thread_locals_;

  using JoinFn = std::function<Status(
      const PipelineContext&, const TaskContext&, ThreadId, TempVectorStack*,
      std::vector<KeyColumnArray>*, std::optional<Batch>&, State&)>;

  OpResult Probe(const PipelineContext& pipeline_ctx, const TaskContext& task_ctx,
                 ThreadId thread_id, std::optional<Batch> input,
                 TempVectorStack* temp_stack,
                 std::vector<KeyColumnArray>* temp_column_arrays, const JoinFn& join_fn) {
    // Process.
    std::optional<Batch> output_batch;
    State state_next = State::CLEAN;
    switch (thread_locals_[thread_id].state) {
      case State::CLEAN: {
        // Some check.
        ARA_CHECK(!thread_locals_[thread_id].input.has_value());
        ARA_CHECK(input.has_value());

        // Prepare input.
        Batch batch;
        ARA_ASSIGN_OR_RAISE(batch, KeyPayloadFromInput(schema_, pool_, &input.value()));

        auto batch_length = input->length;
        thread_locals_[thread_id].input.emplace(
            Input{std::move(batch), Batch({}, batch_length), 0, {}});
        thread_locals_[thread_id].input->key_batch.values.resize(num_key_columns_);
        for (int i = 0; i < num_key_columns_; ++i) {
          thread_locals_[thread_id].input->key_batch.values[i] =
              thread_locals_[thread_id].input->batch.values[i];
        }

        // Process input.
        ARA_RETURN_NOT_OK(join_fn(pipeline_ctx, task_ctx, thread_id, temp_stack,
                                  temp_column_arrays, output_batch, state_next));

        break;
      }
      case State::MINI_BATCH_HAS_MORE:
      case State::MATCH_HAS_MORE: {
        // Some check.
        ARA_CHECK(thread_locals_[thread_id].input.has_value());
        ARA_CHECK(!input.has_value());

        // Process input.
        ARA_RETURN_NOT_OK(join_fn(pipeline_ctx, task_ctx, thread_id, temp_stack,
                                  temp_column_arrays, output_batch, state_next));

        break;
      }
    }

    // Transition.
    thread_locals_[thread_id].state = state_next;

    OpOutput op_output = OpOutput::Finished();
    switch (state_next) {
      case State::CLEAN: {
        thread_locals_[thread_id].input.reset();
        if (output_batch.has_value()) {
          op_output = OpOutput::PipeEven(std::move(output_batch.value()));
        } else {
          op_output = OpOutput::PipeSinkNeedsMore();
        }
        break;
      }
      case State::MINI_BATCH_HAS_MORE:
      case State::MATCH_HAS_MORE: {
        ARA_CHECK(output_batch.has_value());
        op_output = OpOutput::SourcePipeHasMore(std::move(output_batch.value()));
        break;
      }
    }

    return std::move(op_output);
  }

  Status InnerOuter(const PipelineContext& pipeline_ctx, const TaskContext& task_ctx,
                    ThreadId thread_id, TempVectorStack* temp_stack,
                    std::vector<KeyColumnArray>* temp_column_arrays,
                    std::optional<Batch>& output, State& state_next) {
    size_t pipe_max_batch_length = pipeline_ctx.query_ctx->options.pipe_max_batch_length;
    size_t mini_batch_length = pipeline_ctx.query_ctx->options.mini_batch_length;
    int num_rows = static_cast<int>(thread_locals_[thread_id].input->batch.length);
    state_next = State::CLEAN;
    bool match_has_output = false, mini_batch_has_output = false;
    bool match_has_more_last = thread_locals_[thread_id].state == State::MATCH_HAS_MORE;

    // Break into mini-batches
    for (; thread_locals_[thread_id].input->mini_batch_start < num_rows &&
           !match_has_output && !mini_batch_has_output;) {
      uint32_t mini_batch_size_next =
          std::min(mini_batch_length,
                   num_rows - thread_locals_[thread_id].input->mini_batch_start);
      bool no_duplicate_keys = (hash_table_->key_to_payload() == nullptr);
      bool no_payload_columns = (hash_table_->payloads() == nullptr);

      if (!match_has_more_last) {
        // Calculate hash and matches for this mini-batch.
        SwissTableWithKeys::Input hash_table_input(
            &thread_locals_[thread_id].input->key_batch,
            thread_locals_[thread_id].input->mini_batch_start,
            thread_locals_[thread_id].input->mini_batch_start + mini_batch_size_next,
            temp_stack, temp_column_arrays);
        hash_table_->keys()->Hash(&hash_table_input,
                                  thread_locals_[thread_id].hashes_buf_data(),
                                  hardware_flags_);
        hash_table_->keys()->MapReadOnly(
            &hash_table_input, thread_locals_[thread_id].hashes_buf_data(),
            thread_locals_[thread_id].match_bitvector_buf_data(),
            thread_locals_[thread_id].key_ids_buf_data());

        // AND bit vector with null key filter for join
        bool ignored;
        JoinNullFilter::Filter(thread_locals_[thread_id].input->key_batch,
                               thread_locals_[thread_id].input->mini_batch_start,
                               mini_batch_size_next, *cmp_, &ignored,
                               /*and_with_input=*/true,
                               thread_locals_[thread_id].match_bitvector_buf_data());

        // We need to output matching pairs of rows from both sides of the join.
        // Since every hash table lookup for an input row might have multiple
        // matches we use a helper class that implements enumerating all of them.
        thread_locals_[thread_id].input->match_iterator.SetLookupResult(
            mini_batch_size_next, thread_locals_[thread_id].input->mini_batch_start,
            thread_locals_[thread_id].match_bitvector_buf_data(),
            thread_locals_[thread_id].key_ids_buf_data(), no_duplicate_keys,
            hash_table_->key_to_payload());
      }

      int num_matches_next;
      while (!match_has_output &&
             thread_locals_[thread_id].input->match_iterator.GetNextBatch(
                 mini_batch_length, &num_matches_next,
                 thread_locals_[thread_id].materialize_batch_ids_buf_data(),
                 thread_locals_[thread_id].materialize_key_ids_buf_data(),
                 thread_locals_[thread_id].materialize_payload_ids_buf_data())) {
        const uint16_t* materialize_batch_ids =
            thread_locals_[thread_id].materialize_batch_ids_buf_data();
        const uint32_t* materialize_key_ids =
            thread_locals_[thread_id].materialize_key_ids_buf_data();
        const uint32_t* materialize_payload_ids =
            no_duplicate_keys || no_payload_columns
                ? thread_locals_[thread_id].materialize_key_ids_buf_data()
                : thread_locals_[thread_id].materialize_payload_ids_buf_data();

        // For right-outer, full-outer joins we need to update has-match flags
        // for the rows in hash table.
        if (join_type_ == JoinType::RIGHT_OUTER || join_type_ == JoinType::FULL_OUTER) {
          hash_table_->UpdateHasMatchForKeys(thread_id, num_matches_next,
                                             materialize_key_ids);
        }

        size_t rows_appended = 0;

        // If we are to exceed the maximum number of rows per batch, output.
        if (thread_locals_[thread_id].materialize->num_rows() + num_matches_next >=
            pipe_max_batch_length) {
          rows_appended =
              pipe_max_batch_length - thread_locals_[thread_id].materialize->num_rows();
          num_matches_next -= rows_appended;
          int ignored;
          ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->Append(
              thread_locals_[thread_id].input->batch, rows_appended,
              materialize_batch_ids, materialize_key_ids, materialize_payload_ids,
              &ignored));
          ARA_RETURN_NOT_OK(
              thread_locals_[thread_id].materialize->Flush([&](Batch batch) {
                output.emplace(std::move(batch));
                return Status::OK();
              }));
          match_has_output = true;
        }

        // Call materialize for resulting id tuples pointing to matching pairs
        // of rows.
        if (num_matches_next > 0) {
          int ignored;
          ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->Append(
              thread_locals_[thread_id].input->batch, num_matches_next,
              materialize_batch_ids + rows_appended, materialize_key_ids + rows_appended,
              materialize_payload_ids + rows_appended, &ignored));
        }
      }

      if (!match_has_output) {
        match_has_more_last = false;

        // For left-outer and full-outer joins output non-matches.
        //
        // Call materialize. Nulls will be output in all columns that come from
        // the other side of the join.
        //
        if (join_type_ == JoinType::LEFT_OUTER || join_type_ == JoinType::FULL_OUTER) {
          int num_passing_ids = 0;
          arrow::util::bit_util::bits_to_indexes(
              /*bit_to_search=*/0, hardware_flags_, mini_batch_size_next,
              thread_locals_[thread_id].match_bitvector_buf_data(), &num_passing_ids,
              thread_locals_[thread_id].materialize_batch_ids_buf_data());

          // Add base batch row index.
          for (int i = 0; i < num_passing_ids; ++i) {
            thread_locals_[thread_id].materialize_batch_ids_buf_data()[i] +=
                static_cast<uint16_t>(thread_locals_[thread_id].input->mini_batch_start);
          }

          size_t rows_appended = 0;

          // If we are to exceed the maximum number of rows per batch, output.
          if (thread_locals_[thread_id].materialize->num_rows() + num_passing_ids >=
              pipe_max_batch_length) {
            rows_appended =
                pipe_max_batch_length - thread_locals_[thread_id].materialize->num_rows();
            num_passing_ids -= rows_appended;
            int ignored;
            ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->AppendProbeOnly(
                thread_locals_[thread_id].input->batch, rows_appended,
                thread_locals_[thread_id].materialize_batch_ids_buf_data(), &ignored));
            ARA_RETURN_NOT_OK(
                thread_locals_[thread_id].materialize->Flush([&](Batch batch) {
                  output.emplace(std::move(batch));
                  return Status::OK();
                }));
            mini_batch_has_output = true;
          }

          if (num_passing_ids > 0) {
            int ignored;
            ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->AppendProbeOnly(
                thread_locals_[thread_id].input->batch, num_passing_ids,
                thread_locals_[thread_id].materialize_batch_ids_buf_data() +
                    rows_appended,
                &ignored));
          }
        }

        thread_locals_[thread_id].input->mini_batch_start += mini_batch_size_next;
      }
    }

    if (match_has_output) {
      state_next = State::MATCH_HAS_MORE;
    } else if (thread_locals_[thread_id].input->mini_batch_start < num_rows) {
      state_next = State::MINI_BATCH_HAS_MORE;
    }

    return Status::OK();
  }

  Status LeftSemiAnti(const PipelineContext& pipeline_ctx, const TaskContext& task_ctx,
                      ThreadId thread_id, TempVectorStack* temp_stack,
                      std::vector<KeyColumnArray>* temp_column_arrays,
                      std::optional<Batch>& output, State& state_next) {
    size_t pipe_max_batch_length = pipeline_ctx.query_ctx->options.pipe_max_batch_length;
    size_t mini_batch_length = pipeline_ctx.query_ctx->options.mini_batch_length;
    int num_rows = static_cast<int>(thread_locals_[thread_id].input->batch.length);
    state_next = State::CLEAN;
    bool mini_batch_has_output = false;

    // Break into mini_batches
    for (; thread_locals_[thread_id].input->mini_batch_start < num_rows &&
           !mini_batch_has_output;) {
      uint32_t mini_batch_size_next =
          std::min(mini_batch_length,
                   num_rows - thread_locals_[thread_id].input->mini_batch_start);

      // Calculate hash and matches for this mini_batch.
      {
        SwissTableWithKeys::Input hash_table_input(
            &thread_locals_[thread_id].input->key_batch,
            thread_locals_[thread_id].input->mini_batch_start,
            thread_locals_[thread_id].input->mini_batch_start + mini_batch_size_next,
            temp_stack, temp_column_arrays);
        hash_table_->keys()->Hash(&hash_table_input,
                                  thread_locals_[thread_id].hashes_buf_data(),
                                  hardware_flags_);
        hash_table_->keys()->MapReadOnly(
            &hash_table_input, thread_locals_[thread_id].hashes_buf_data(),
            thread_locals_[thread_id].match_bitvector_buf_data(),
            thread_locals_[thread_id].key_ids_buf_data());
      }

      // AND bit vector with null key filter for join
      {
        bool ignored;
        JoinNullFilter::Filter(thread_locals_[thread_id].input->key_batch,
                               thread_locals_[thread_id].input->mini_batch_start,
                               mini_batch_size_next, *cmp_, &ignored,
                               /*and_with_input=*/true,
                               thread_locals_[thread_id].match_bitvector_buf_data());
      }

      int num_passing_ids = 0;
      arrow::util::bit_util::bits_to_indexes(
          (join_type_ == JoinType::LEFT_ANTI) ? 0 : 1, hardware_flags_,
          mini_batch_size_next, thread_locals_[thread_id].match_bitvector_buf_data(),
          &num_passing_ids, thread_locals_[thread_id].materialize_batch_ids_buf_data());

      // Add base batch row index.
      for (int i = 0; i < num_passing_ids; ++i) {
        thread_locals_[thread_id].materialize_batch_ids_buf_data()[i] +=
            static_cast<uint16_t>(thread_locals_[thread_id].input->mini_batch_start);
      }

      size_t rows_appended = 0;

      // If we are to exceed the maximum number of rows per batch, output.
      if (thread_locals_[thread_id].materialize->num_rows() + num_passing_ids >=
          pipe_max_batch_length) {
        rows_appended =
            pipe_max_batch_length - thread_locals_[thread_id].materialize->num_rows();
        num_passing_ids -= rows_appended;
        int ignored;
        ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->AppendProbeOnly(
            thread_locals_[thread_id].input->batch, rows_appended,
            thread_locals_[thread_id].materialize_batch_ids_buf_data(), &ignored));
        ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->Flush([&](Batch batch) {
          output.emplace(std::move(batch));
          return Status::OK();
        }));
        mini_batch_has_output = true;
      }

      if (num_passing_ids > 0) {
        int ignored;
        ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->AppendProbeOnly(
            thread_locals_[thread_id].input->batch, num_passing_ids,
            thread_locals_[thread_id].materialize_batch_ids_buf_data() + rows_appended,
            &ignored));
      }

      thread_locals_[thread_id].input->mini_batch_start += mini_batch_size_next;
    }

    if (thread_locals_[thread_id].input->mini_batch_start < num_rows) {
      state_next = State::MINI_BATCH_HAS_MORE;
    }

    return Status::OK();
  }

  Status RightSemiAnti(const PipelineContext& pipeline_ctx, const TaskContext& task_ctx,
                       ThreadId thread_id, TempVectorStack* temp_stack,
                       std::vector<KeyColumnArray>* temp_column_arrays,
                       std::optional<Batch>& output, State& state_next) {
    size_t mini_batch_length = pipeline_ctx.query_ctx->options.mini_batch_length;
    int num_rows = static_cast<int>(thread_locals_[thread_id].input->batch.length);
    state_next = State::CLEAN;

    // Break into mini_batches
    for (; thread_locals_[thread_id].input->mini_batch_start < num_rows;) {
      uint32_t mini_batch_size_next =
          std::min(mini_batch_length,
                   num_rows - thread_locals_[thread_id].input->mini_batch_start);

      // Calculate hash and matches for this mini_batch.
      {
        SwissTableWithKeys::Input hash_table_input(
            &thread_locals_[thread_id].input->key_batch,
            thread_locals_[thread_id].input->mini_batch_start,
            thread_locals_[thread_id].input->mini_batch_start + mini_batch_size_next,
            temp_stack, temp_column_arrays);
        hash_table_->keys()->Hash(&hash_table_input,
                                  thread_locals_[thread_id].hashes_buf_data(),
                                  hardware_flags_);
        hash_table_->keys()->MapReadOnly(
            &hash_table_input, thread_locals_[thread_id].hashes_buf_data(),
            thread_locals_[thread_id].match_bitvector_buf_data(),
            thread_locals_[thread_id].key_ids_buf_data());
      }

      // AND bit vector with null key filter for join
      {
        bool ignored;
        JoinNullFilter::Filter(thread_locals_[thread_id].input->key_batch,
                               thread_locals_[thread_id].input->mini_batch_start,
                               mini_batch_size_next, *cmp_, &ignored,
                               /*and_with_input=*/true,
                               thread_locals_[thread_id].match_bitvector_buf_data());
      }

      int num_passing_ids = 0;
      arrow::util::bit_util::bits_to_indexes(
          (join_type_ == JoinType::LEFT_ANTI) ? 0 : 1, hardware_flags_,
          mini_batch_size_next, thread_locals_[thread_id].match_bitvector_buf_data(),
          &num_passing_ids, thread_locals_[thread_id].materialize_batch_ids_buf_data());

      // For right-semi, right-anti joins: update has-match flags for the rows
      // in hash table.
      for (int i = 0; i < num_passing_ids; ++i) {
        uint16_t id = thread_locals_[thread_id].materialize_batch_ids_buf_data()[i];
        thread_locals_[thread_id].key_ids_buf_data()[i] =
            thread_locals_[thread_id].key_ids_buf_data()[id];
      }
      hash_table_->UpdateHasMatchForKeys(thread_id, num_passing_ids,
                                         thread_locals_[thread_id].key_ids_buf_data());

      thread_locals_[thread_id].input->mini_batch_start += mini_batch_size_next;
    }

    return Status::OK();
  }
};

class ScanProcessor {
 public:
  Status Init(const PipelineContext& pipeline_ctx, HashJoin* hash_join) {
    dop_ = hash_join->dop;
    join_type_ = hash_join->join_type;
    hash_table_ = &hash_join->hash_table;

    thread_locals_.resize(dop_);
    for (int i = 0; i < dop_; ++i) {
      thread_locals_[i].materialize = &hash_join->materialize[i];
    }

    return Status::OK();
  }

  TaskResult StartScan() {
    hash_table_->MergeHasMatch();

    num_rows_per_thread_ = CeilDiv(hash_table_->num_rows(), dop_);

    return TaskStatus::Finished();
  }

  OpResult Scan(const PipelineContext& pipeline_ctx, ThreadId thread_id,
                TempVectorStack* temp_stack) {
    size_t source_max_batch_length =
        pipeline_ctx.query_ctx->options.source_max_batch_length;

    // Clear materialize if we can't combine the rows remained in probe with the ones to
    // be scanned. And at this point, we can't respect the source_max_batch_length.
    //
    if (thread_locals_[thread_id].materialize->num_rows() >= source_max_batch_length) {
      std::optional<Batch> output;
      ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->Flush([&](Batch batch) {
        output.emplace(std::move(batch));
        return Status::OK();
      }));
      return OpOutput::SourcePipeHasMore(std::move(output.value()));
    }

    size_t mini_batch_length = pipeline_ctx.query_ctx->options.mini_batch_length;
    // Should we output matches or non-matches?
    //
    bool bit_to_output = (join_type_ == JoinType::RIGHT_SEMI);

    int64_t start_row =
        num_rows_per_thread_ * thread_id + thread_locals_[thread_id].current_start_;
    int64_t end_row = std::min(num_rows_per_thread_ * (thread_id + 1),
                               static_cast<size_t>(hash_table_->num_rows()));

    if (start_row >= end_row) {
      std::optional<Batch> output;
      if (thread_locals_[thread_id].materialize->num_rows() > 0) {
        ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->Flush([&](Batch batch) {
          output.emplace(std::move(batch));
          return Status::OK();
        }));
      }
      return OpOutput::Finished(std::move(output));
    }

    // Split into mini-batches
    //
    auto payload_ids_buf = TempVectorHolder<uint32_t>(temp_stack, mini_batch_length);
    auto key_ids_buf = TempVectorHolder<uint32_t>(temp_stack, mini_batch_length);
    auto selection_buf = TempVectorHolder<uint16_t>(temp_stack, mini_batch_length);
    std::optional<Batch> output;
    int64_t mini_batch_start = start_row;
    for (; mini_batch_start < end_row && !output.has_value();) {
      // Compute the size of the next mini-batch
      //
      int64_t mini_batch_size_next =
          std::min(end_row - mini_batch_start, static_cast<int64_t>(mini_batch_length));

      // Get the list of key and payload ids from this mini-batch to output.
      //
      uint32_t first_key_id =
          hash_table_->payload_id_to_key_id(static_cast<uint32_t>(mini_batch_start));
      uint32_t last_key_id = hash_table_->payload_id_to_key_id(
          static_cast<uint32_t>(mini_batch_start + mini_batch_size_next - 1));
      int num_output_rows = 0;
      for (uint32_t key_id = first_key_id; key_id <= last_key_id; ++key_id) {
        if (GetBit(hash_table_->has_match(), key_id) == bit_to_output) {
          uint32_t first_payload_for_key = std::max(
              static_cast<uint32_t>(mini_batch_start),
              hash_table_->key_to_payload() ? hash_table_->key_to_payload()[key_id]
                                            : key_id);
          uint32_t last_payload_for_key =
              std::min(static_cast<uint32_t>(mini_batch_start + mini_batch_size_next - 1),
                       hash_table_->key_to_payload()
                           ? hash_table_->key_to_payload()[key_id + 1] - 1
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

      size_t rows_appended = 0;

      // If we are to exceed the maximum number of rows per batch, output.
      if (thread_locals_[thread_id].materialize->num_rows() + num_output_rows >=
          source_max_batch_length) {
        rows_appended =
            source_max_batch_length - thread_locals_[thread_id].materialize->num_rows();
        num_output_rows -= rows_appended;
        int ignored;
        ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->AppendBuildOnly(
            rows_appended, key_ids_buf.mutable_data(), payload_ids_buf.mutable_data(),
            &ignored));

        ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->Flush([&](Batch batch) {
          output.emplace(std::move(batch));
          return Status::OK();
        }));
      }

      if (num_output_rows > 0) {
        // Materialize (and output whenever buffers get full) hash table
        // values according to the generated list of ids.
        //
        int ignored;
        ARA_RETURN_NOT_OK(thread_locals_[thread_id].materialize->AppendBuildOnly(
            num_output_rows, key_ids_buf.mutable_data() + rows_appended,
            payload_ids_buf.mutable_data() + rows_appended, &ignored));
      }

      mini_batch_start += mini_batch_size_next;
      thread_locals_[thread_id].current_start_ += mini_batch_size_next;
    }

    if (!output.has_value()) {
      return Scan(pipeline_ctx, thread_id, temp_stack);
    } else {
      if (mini_batch_start >= end_row) {
        return OpOutput::Finished(std::move(output.value()));
      } else {
        return OpOutput::SourcePipeHasMore(std::move(output.value()));
      }
    }
  }

 private:
  size_t dop_;
  size_t num_rows_per_thread_;

  JoinType join_type_;

  SwissTableForJoin* hash_table_;

 private:
  struct ThreadLocal {
    int current_start_ = 0;
    JoinResultMaterialize* materialize = nullptr;
  };
  std::vector<ThreadLocal> thread_locals_;
};

}  // namespace detail

class HashJoinScanSource : public SourceOp {
 public:
  HashJoinScanSource(std::string name, std::string desc)
      : SourceOp(std::move(name), std::move(desc)) {}

  Status Init(const PipelineContext& pipeline_ctx,
              std::shared_ptr<detail::HashJoin> hash_join) {
    hash_join_ = std::move(hash_join);
    ctx_ = hash_join_->ctx;
    return scan_processor_.Init(pipeline_ctx, hash_join_.get());
  }

  PipelineSource Source(const PipelineContext&) override {
    return [&](const PipelineContext& pipeline_ctx, const TaskContext& task_ctx,
               ThreadId thread_id) -> OpResult {
      ARA_ASSIGN_OR_RAISE(TempVectorStack * temp_stack, ctx_->GetTempStack(thread_id));
      return scan_processor_.Scan(pipeline_ctx, thread_id, temp_stack);
    };
  }

  TaskGroups Frontend(const PipelineContext&) override {
    Task start_scan_task("HashJoinScanSource::StartScanTask", "",
                         [&](const TaskContext&, TaskId task_id) -> TaskResult {
                           return scan_processor_.StartScan();
                         });

    return {{"HashJoinScanSource::StartScan", "", std::move(start_scan_task), 1,
             std::nullopt, nullptr}};
  }

  std::optional<TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }

 private:
  std::shared_ptr<detail::HashJoin> hash_join_;

  arrow::acero::QueryContext* ctx_;

  detail::ScanProcessor scan_processor_;
};

HashJoinProbe::HashJoinProbe(std::string name, std::string desc)
    : PipeOp(std::move(name), std::move(desc)),
      probe_processor_(std::make_unique<detail::ProbeProcessor>()) {}

HashJoinProbe::~HashJoinProbe() = default;

Status HashJoinProbe::Init(const PipelineContext& pipeline_ctx,
                           std::shared_ptr<detail::HashJoin> hash_join) {
  hash_join_ = std::move(hash_join);
  ctx_ = hash_join_->ctx;
  join_type_ = hash_join_->join_type;
  thread_locals_.resize(hash_join_->dop);
  return probe_processor_->Init(pipeline_ctx, hash_join_.get());
}

PipelinePipe HashJoinProbe::Pipe(const PipelineContext&) {
  return [&](const PipelineContext& pipeline_ctx, const TaskContext& task_ctx,
             ThreadId thread_id, std::optional<Batch> batch) -> OpResult {
    ARA_ASSIGN_OR_RAISE(TempVectorStack * temp_stack, ctx_->GetTempStack(thread_id));
    auto temp_column_arrays = &thread_locals_[thread_id].temp_column_arrays;
    return probe_processor_->Probe(pipeline_ctx, task_ctx, thread_id, std::move(batch),
                                   temp_stack, temp_column_arrays);
  };
}

PipelineDrain HashJoinProbe::Drain(const PipelineContext&) {
  if (detail::NeedToScan(join_type_)) {
    // No need to drain if scan is needed, scan will output the remaining rows in
    // materialize.
    return {};
  }
  return [&](const PipelineContext& pipeline_ctx, const TaskContext& task_ctx,
             ThreadId thread_id) {
    return probe_processor_->Drain(pipeline_ctx, task_ctx, thread_id);
  };
}

std::unique_ptr<SourceOp> HashJoinProbe::ImplicitSource(
    const PipelineContext& pipeline_ctx) {
  if (!detail::NeedToScan(join_type_)) {
    return nullptr;
  }
  std::unique_ptr<HashJoinScanSource> scan_source =
      std::make_unique<HashJoinScanSource>("HashJoinScanSource", "");
  ARA_CHECK_OK(scan_source->Init(pipeline_ctx, hash_join_));
  return scan_source;
}

}  // namespace ara::pipeline

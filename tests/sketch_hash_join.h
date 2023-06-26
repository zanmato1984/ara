#include <arrow/acero/hash_join.h>
#include <arrow/api.h>

#include "arrow/acero/swiss_join_internal.h"

#define ARRA_RETURN_NOT_OK ARROW_RETURN_NOT_OK

#define ARRA_SET_AND_RETURN_NOT_OK(status, set)                       \
  do {                                                                \
    ::arrow::Status __s = ::arrow::internal::GenericToStatus(status); \
    [&](const ::arrow::Status& __s) set(__s);                         \
    ARROW_RETURN_IF_(!__s.ok(), __s, ARROW_STRINGIFY(status));        \
  } while (false)

#define ARRA_DCHECK ARROW_DCHECK

namespace arra {

enum class OperatorStatusCode : char {
  HAS_OUTPUT = 0,
  HAS_MORE_OUTPUT = 1,
  FINISHED = 2,
  SPILLING = 3,
  OTHER = 4,
};

struct OperatorStatus {
  OperatorStatusCode code;
  std::variant<std::optional<arrow::ExecBatch>, arrow::Status> payload;

  static OperatorStatus HasOutput(std::optional<arrow::ExecBatch> output) {
    return OperatorStatus{OperatorStatusCode::HAS_OUTPUT, std::move(output)};
  }
  static OperatorStatus HasMoreOutput(std::optional<arrow::ExecBatch> output) {
    return OperatorStatus{OperatorStatusCode::HAS_MORE_OUTPUT, std::move(output)};
  }
  static OperatorStatus Finished() {
    return OperatorStatus{OperatorStatusCode::FINISHED, std::nullopt};
  }
  static OperatorStatus Spilling() {
    return OperatorStatus{OperatorStatusCode::SPILLING, std::nullopt};
  }
  static OperatorStatus Other(arrow::Status status) {
    return OperatorStatus{OperatorStatusCode::OTHER, std::move(status)};
  }
};

namespace detail {

using namespace arrow;
using namespace arrow::acero;
using namespace arrow::compute;
using namespace arrow::bit_util;
using arrow::util::bit_util;
using arrow::util::TempVectorHolder;
using arrow::util::TempVectorStack;

class ProbeProcessor {
 public:
  void Init(int num_key_columns, JoinType join_type, const std::vector<JoinKeyCmp>* cmp,
            SwissTableForJoin* hash_table, JoinResultMaterialize* materialize,
            TempVectorStack* temp_stack);

  Status ProbeBatch(int64_t thread_id, std::optional<ExecBatch> input,
                    TempVectorStack* temp_stack,
                    std::vector<KeyColumnArray>* temp_column_arrays,
                    OperatorStatus& status);

 private:
  int num_key_columns_;
  JoinType join_type_;
  const std::vector<JoinKeyCmp>* cmp_;

  SwissTableForJoin* hash_table_;
  const SwissTable* swiss_table_;
  int64_t hardware_flags_;
  int minibatch_size_;

  JoinResultMaterialize* materialize_;

 private:
  template <typename JOIN_FN>
  Status ProbeBatch(int64_t thread_id, std::optional<ExecBatch> input,
                    TempVectorStack* temp_stack,
                    std::vector<KeyColumnArray>* temp_column_arrays,
                    OperatorStatus& status, const JOIN_FN& join_fn) {
    // Process.
    std::optional<ExecBatch> output;
    State state_next = State::CLEAN;
    switch (state_) {
      case State::CLEAN: {
        // Some check.
        ARRA_DCHECK(!input_.has_value());
        ARRA_DCHECK(materialize_->num_rows() == 0);

        // Prepare input.
        auto batch_length = input->length;
        input_ =
            Input{std::move(input.value()),
                  ExecBatch({}, batch_length),
                  0,
                  TempVectorHolder<uint32_t>(temp_stack, minibatch_size_),
                  TempVectorHolder<uint8_t>(
                      temp_stack, static_cast<uint32_t>(BytesForBits(minibatch_size_))),
                  TempVectorHolder<uint32_t>(temp_stack, minibatch_size_),
                  TempVectorHolder<uint16_t>(temp_stack, minibatch_size_),
                  TempVectorHolder<uint32_t>(temp_stack, minibatch_size_),
                  TempVectorHolder<uint32_t>(temp_stack, minibatch_size_),
                  {}};
        input_->key_batch.values.resize(num_key_columns_);
        for (int i = 0; i < num_key_columns_; ++i) {
          input_->key_batch.values[i] = input_->batch.values[i];
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
        ARRA_DCHECK(input_.has_value());
        ARRA_DCHECK(materialize_->num_rows() > 0);

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
        input_.reset();
        status = OperatorStatus::HasOutput(std::move(output));
        break;
      }
      case State::MINIBATCH_HAS_MORE:
      case State::MATCH_HAS_MORE: {
        status = OperatorStatus::HasMoreOutput(std::move(output));
        break;
      }
    }

    state_ = state_next;

    return Status::OK();
  }

  enum class State { CLEAN, MINIBATCH_HAS_MORE, MATCH_HAS_MORE } state_ = State::CLEAN;
  struct Input {
    ExecBatch batch;
    ExecBatch key_batch;
    int minibatch_start = 0;

    TempVectorHolder<uint32_t> hashes_buf;
    TempVectorHolder<uint8_t> match_bitvector_buf;
    TempVectorHolder<uint32_t> key_ids_buf;
    TempVectorHolder<uint16_t> materialize_batch_ids_buf;
    TempVectorHolder<uint32_t> materialize_key_ids_buf;
    TempVectorHolder<uint32_t> materialize_payload_ids_buf;

    JoinMatchIterator match_iterator;
  };
  std::optional<Input> input_;

  Status InnerOrOuter(int64_t thread_id, TempVectorStack* temp_stack,
                      std::vector<KeyColumnArray>* temp_column_arrays,
                      std::optional<ExecBatch>& output, State& state_next);
  Status LeftSemiOrAnti(int64_t thread_id, TempVectorStack* temp_stack,
                        std::vector<KeyColumnArray>* temp_column_arrays,
                        std::optional<ExecBatch>& output, State& state_next);
  Status RightSemiOrAnti(int64_t thread_id, TempVectorStack* temp_stack,
                         std::vector<KeyColumnArray>* temp_column_arrays,
                         std::optional<ExecBatch>& output, State& state_next);
};

class HashJoin {
 public:
  Status Init(QueryContext* ctx, JoinType join_type, size_t num_threads,
              const HashJoinProjectionMaps* proj_map_left,
              const HashJoinProjectionMaps* proj_map_right,
              std::vector<JoinKeyCmp> key_cmp, Expression filter);

 private:
  QueryContext* ctx_;
  int64_t hardware_flags_;
  MemoryPool* pool_;
  int num_threads_;
  JoinType join_type_;
  std::vector<JoinKeyCmp> key_cmp_;
  const HashJoinProjectionMaps* schema_[2];

  struct ThreadLocalState {
    JoinResultMaterialize materialize;
    ProbeProcessor probe_processor;
    std::vector<KeyColumnArray> temp_column_arrays;
    int64_t num_output_batches;
    bool hash_table_ready;
  };
  std::vector<ThreadLocalState> local_states_;

  SwissTableForJoin hash_table_;
  SwissTableForJoinBuild hash_table_build_;
  AccumulationQueue build_side_batches_;
};

}  // namespace detail

using HashJoin = arra::detail::HashJoin;

}  // namespace arra

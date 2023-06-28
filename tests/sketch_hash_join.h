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

using TaskGroupId = size_t;
using TaskId = size_t;
using ThreadId = size_t;

enum class ResourcePreference {
  CPU = 0,
  MEMORY = 1,
  IO = 2,
  DISK = 3,
  NETWORK = 4,
  GPU = 5,
};

using Task =
    std::pair<std::function<arrow::Status(TaskId, OperatorStatus&)>, ResourcePreference>;
using TaskCont = std::function<arrow::Status(TaskId)>;
using TaskGroup = std::tuple<Task, size_t, std::optional<TaskCont>>;
using TaskGroups = std::vector<TaskGroup>;

using PipelineTaskSource = std::function<arrow::Status(ThreadId, OperatorStatus&)>;
using PipelineTaskPipe = std::function<arrow::Status(
    ThreadId, std::optional<arrow::ExecBatch>, OperatorStatus&)>;

namespace detail {
using namespace arrow;
using namespace arrow::acero;
using namespace arrow::compute;
using namespace arrow::bit_util;
using arrow::util::bit_util;
using arrow::util::MiniBatch;
using arrow::util::TempVectorHolder;
using arrow::util::TempVectorStack;

class BuildProcessor {
 public:
  Status Init(int64_t hardware_flags, MemoryPool* pool,
              const HashJoinProjectionMaps* schema, JoinType join_type,
              SwissTableForJoinBuild* hash_table_build, SwissTableForJoin* hash_table,
              AccumulationQueue* batches,
              const std::vector<JoinResultMaterialize*>& materialize);

  // Could execute in driver thread.
  Status StartBuild();

  // Must execute in task thread.
  Status Build(ThreadId thread_id, TempVectorStack* temp_stack, OperatorStatus& status);

  // Could execute in driver thread.
  Status FinishBuild();

  // Could execute in driver thread.
  Status StartMerge();

  // Must execute in task thread.
  Status Merge(ThreadId thread_id, TempVectorStack* temp_stack, OperatorStatus& status);

  // Must execute in task thread.
  Status FinishMerge(TempVectorStack* temp_stack);

 private:
  int64_t hardware_flags_;
  MemoryPool* pool_;
  size_t dop_;

  const HashJoinProjectionMaps* schema_;
  JoinType join_type_;
  SwissTableForJoinBuild* hash_table_build_;
  SwissTableForJoin* hash_table_;
  AccumulationQueue* batches_;

  struct ThreadLocalState {
    size_t round = 0;
    JoinResultMaterialize* materialize = nullptr;
  };
  std::vector<ThreadLocalState> local_states_;
};

class ProbeProcessor {
 public:
  Status Init(int64_t hardware_flags, int num_key_columns, JoinType join_type,
              const std::vector<JoinKeyCmp>* cmp, SwissTableForJoin* hash_table,
              const std::vector<JoinResultMaterialize*>& materialize);

  // Must execute in task thread.
  Status Probe(ThreadId thread_id, std::optional<ExecBatch> input,
               TempVectorStack* temp_stack,
               std::vector<KeyColumnArray>* temp_column_arrays, OperatorStatus& status);

 private:
  int64_t hardware_flags_;

  int num_key_columns_;
  JoinType join_type_;
  const std::vector<JoinKeyCmp>* cmp_;

  SwissTableForJoin* hash_table_;
  const SwissTable* swiss_table_;

 private:
  template <typename JOIN_FN>
  Status Probe(ThreadId thread_id, std::optional<ExecBatch> input,
               TempVectorStack* temp_stack,
               std::vector<KeyColumnArray>* temp_column_arrays, OperatorStatus& status,
               const JOIN_FN& join_fn) {
    // Process.
    std::optional<ExecBatch> output;
    State state_next = State::CLEAN;
    switch (local_states_[thread_id].state) {
      case State::CLEAN: {
        // Some check.
        ARRA_DCHECK(!local_states_[thread_id].input.has_value());
        ARRA_DCHECK(local_states_[thread_id].materialize->num_rows() == 0);

        // Prepare input.
        auto batch_length = input->length;
        local_states_[thread_id].input =
            Input{std::move(input.value()),
                  ExecBatch({}, batch_length),
                  0,
                  TempVectorHolder<uint32_t>(temp_stack, MiniBatch::kMiniBatchLength),
                  TempVectorHolder<uint8_t>(
                      temp_stack,
                      static_cast<uint32_t>(BytesForBits(MiniBatch::kMiniBatchLength))),
                  TempVectorHolder<uint32_t>(temp_stack, MiniBatch::kMiniBatchLength),
                  TempVectorHolder<uint16_t>(temp_stack, MiniBatch::kMiniBatchLength),
                  TempVectorHolder<uint32_t>(temp_stack, MiniBatch::kMiniBatchLength),
                  TempVectorHolder<uint32_t>(temp_stack, MiniBatch::kMiniBatchLength),
                  {}};
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
        ARRA_DCHECK(local_states_[thread_id].materialize->num_rows() > 0);

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

  enum class State { CLEAN, MINIBATCH_HAS_MORE, MATCH_HAS_MORE };

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

  struct ThreadLocalState {
    State state = State::CLEAN;
    std::optional<Input> input = std::nullopt;
    JoinResultMaterialize* materialize = nullptr;
  };
  std::vector<ThreadLocalState> local_states_;

  Status InnerOuter(ThreadId thread_id, TempVectorStack* temp_stack,
                    std::vector<KeyColumnArray>* temp_column_arrays,
                    std::optional<ExecBatch>& output, State& state_next);
  Status LeftSemiAnti(ThreadId thread_id, TempVectorStack* temp_stack,
                      std::vector<KeyColumnArray>* temp_column_arrays,
                      std::optional<ExecBatch>& output, State& state_next);
  Status RightSemiAnti(ThreadId thread_id, TempVectorStack* temp_stack,
                       std::vector<KeyColumnArray>* temp_column_arrays,
                       std::optional<ExecBatch>& output, State& state_next);
};

class ScanProcessor {
 public:
  Status Init(JoinType join_type, SwissTableForJoin* hash_table,
              const std::vector<JoinResultMaterialize*>& materialize);

  // Must execute in task thread.
  Status StartScan();

  // Must execute in task thread.
  Status Scan(ThreadId thread_id, TempVectorStack* temp_stack,
              std::vector<KeyColumnArray>* temp_column_arrays, OperatorStatus& status);

 private:
  size_t dop_;
  size_t num_rows_per_thread_;

  JoinType join_type_;

  SwissTableForJoin* hash_table_;

  std::vector<JoinResultMaterialize*> materialize_;

 private:
  enum class State { CLEAN, HAS_MORE };

  struct ThreadLocalState {
    State state = State::CLEAN;
    int current_start_ = 0;
    JoinResultMaterialize* materialize = nullptr;
  };
  std::vector<ThreadLocalState> local_states_;
};

class HashJoinScanSource {
 public:
  Status Init(ScanProcessor* scan_processor);

  TaskGroups ScanSourceBackend();

  std::pair<TaskGroups, PipelineTaskPipe> ScanSourceFrontend();

 private:
  ScanProcessor* scan_processor_;
};

class HashJoin {
 public:
  Status Init(QueryContext* ctx, JoinType join_type, size_t dop,
              const HashJoinProjectionMaps* proj_map_left,
              const HashJoinProjectionMaps* proj_map_right,
              std::vector<JoinKeyCmp> key_cmp, Expression filter);

  PipelineTaskPipe BuildPipe();

  TaskGroups BuildBreak();

  PipelineTaskPipe ProbePipe();

  std::optional<HashJoinScanSource> ScanSource();

 private:
  QueryContext* ctx_;
  int64_t hardware_flags_;
  MemoryPool* pool_;
  size_t dop_;
  JoinType join_type_;
  std::vector<JoinKeyCmp> key_cmp_;
  const HashJoinProjectionMaps* schema_[2];

  struct ThreadLocalState {
    JoinResultMaterialize materialize;
    std::vector<KeyColumnArray> temp_column_arrays;
    // int64_t num_output_batches;
  };
  std::vector<ThreadLocalState> local_states_;

  BuildProcessor build_processor_;
  ProbeProcessor probe_processor_;
  ScanProcessor scan_processor_;

  SwissTableForJoin hash_table_;
  SwissTableForJoinBuild hash_table_build_;
  AccumulationQueue build_side_batches_;

  std::optional<HashJoinScanSource> scan_source_;
};

}  // namespace detail

}  // namespace arra

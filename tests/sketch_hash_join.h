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
#define ARRA_DCHECK_OK ARROW_DCHECK_OK

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
  static OperatorStatus Finished(std::optional<arrow::ExecBatch> output) {
    return OperatorStatus{OperatorStatusCode::FINISHED, std::move(output)};
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

using Task = std::function<arrow::Status(TaskId, OperatorStatus&)>;
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
  Status Init(int64_t hardware_flags, MemoryPool* pool, int num_key_columns,
              const HashJoinProjectionMaps* schema, JoinType join_type,
              const std::vector<JoinKeyCmp>* cmp, SwissTableForJoin* hash_table,
              const std::vector<JoinResultMaterialize*>& materialize);

  // Must execute in task thread.
  Status Probe(ThreadId thread_id, std::optional<ExecBatch> input,
               TempVectorStack* temp_stack,
               std::vector<KeyColumnArray>* temp_column_arrays, OperatorStatus& status);

  // Must execute in task thread.
  Status Drain(ThreadId thread_id, OperatorStatus& status);

 private:
  int64_t hardware_flags_;
  MemoryPool* pool_;

  int num_key_columns_;
  const HashJoinProjectionMaps* schema_;
  JoinType join_type_;
  const std::vector<JoinKeyCmp>* cmp_;

  SwissTableForJoin* hash_table_;
  const SwissTable* swiss_table_;

 private:
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

  using JoinFn =
      std::function<Status(ThreadId thread_id, TempVectorStack* temp_stack,
                           std::vector<KeyColumnArray>* temp_column_arrays,
                           std::optional<ExecBatch>& output, State& state_next)>;

  Status Probe(ThreadId thread_id, std::optional<ExecBatch> input,
               TempVectorStack* temp_stack,
               std::vector<KeyColumnArray>* temp_column_arrays, OperatorStatus& status,
               const JoinFn& join_fn);

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
  Status Scan(ThreadId thread_id, TempVectorStack* temp_stack, OperatorStatus& status);

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
  Status Init(QueryContext* ctx, ScanProcessor* scan_processor);

  TaskGroups ScanSourceBackend();

  std::pair<TaskGroups, PipelineTaskSource> ScanSourceFrontend();

 private:
  QueryContext* ctx_;

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

  PipelineTaskPipe ProbeDrain();

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
  std::mutex build_side_mutex_;
  AccumulationQueue build_side_batches_;

  std::optional<HashJoinScanSource> scan_source_;
};

}  // namespace detail

}  // namespace arra

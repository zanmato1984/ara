#include <arrow/acero/hash_join.h>
#include <arrow/api.h>

#include "arrow/acero/swiss_join_internal.h"

namespace arra::detail {

using namespace arrow;
using namespace arrow::acero;
using namespace arrow::compute;

class ProbeProcessor {
 public:
  using OutputBatchFn = std::function<Status(int64_t, ExecBatch)>;

  void Init(int num_key_columns, JoinType join_type, const std::vector<JoinKeyCmp>* cmp,
            SwissTableForJoin* hash_table, JoinResultMaterialize* materialize,
            arrow::util::TempVectorStack* temp_stack);

  // std::optional<ExecBatch> ProbeMinibatch(
  //     int64_t thread_id, arrow::util::TempVectorStack* temp_stack,
  //     std::vector<KeyColumnArray>* temp_column_arrays);

  Status ProbeBatch(int64_t thread_id, ExecBatch keypayload_batch,
                    arrow::util::TempVectorStack* temp_stack,
                    std::vector<KeyColumnArray>* temp_column_arrays);

 private:
  std::optional<ExecBatch> LeftSemiOrAnti(
      int64_t thread_id, ExecBatch keypayload_batch,
      arrow::util::TempVectorStack* temp_stack,
      std::vector<KeyColumnArray>* temp_column_arrays);
  std::optional<ExecBatch> RightSemiOrAnti(
      int64_t thread_id, ExecBatch keypayload_batch,
      arrow::util::TempVectorStack* temp_stack,
      std::vector<KeyColumnArray>* temp_column_arrays);
  int num_key_columns_;
  JoinType join_type_;
  const std::vector<JoinKeyCmp>* cmp_;

  SwissTableForJoin* hash_table_;
  JoinResultMaterialize* materialize_;

  enum class State { CLEAN, HAS_MORE } state_ = State::CLEAN;
  struct Input {
    ExecBatch batch;
    ExecBatch key_batch;
    int minibatch_start = 0;

    arrow::util::TempVectorHolder<uint32_t> hashes_buf;
    arrow::util::TempVectorHolder<uint8_t> match_bitvector_buf;
    arrow::util::TempVectorHolder<uint32_t> key_ids_buf;
    arrow::util::TempVectorHolder<uint16_t> materialize_batch_ids_buf;
    arrow::util::TempVectorHolder<uint32_t> materialize_key_ids_buf;
    arrow::util::TempVectorHolder<uint32_t> materialize_payload_ids_buf;
  };

  std::optional<Input> input_;
  std::optional<JoinMatchIterator> match_iterator_ = std::nullopt;

  OutputBatchFn output_batch_fn_;
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

}  // namespace arra::detail

using HashJoin = arra::detail::HashJoin;
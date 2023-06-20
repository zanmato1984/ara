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

  void Init(int num_key_columns, JoinType join_type, SwissTableForJoin* hash_table,
            std::vector<JoinResultMaterialize*> materialize,
            const std::vector<JoinKeyCmp>* cmp);
  Status ProbeBatch(int64_t thread_id, const ExecBatch& keypayload_batch,
                     arrow::util::TempVectorStack* temp_stack,
                     std::vector<KeyColumnArray>* temp_column_arrays);

  // Must be called by a single-thread having exclusive access to the instance
  // of this class. The caller is responsible for ensuring that.
  //
  Status OnFinished();

 private:
  int num_key_columns_;
  JoinType join_type_;

  SwissTableForJoin* hash_table_;
  // One element per thread
  //
  std::vector<JoinResultMaterialize*> materialize_;
  const std::vector<JoinKeyCmp>* cmp_;
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
    std::vector<KeyColumnArray> temp_column_arrays;
    int64_t num_output_batches;
    bool hash_table_ready;
  };
  std::vector<ThreadLocalState> local_states_;

  SwissTableForJoin hash_table_;
  ProbeProcessor probe_processor_;
  SwissTableForJoinBuild hash_table_build_;
  AccumulationQueue build_side_batches_;
};

}  // namespace arra::detail

using HashJoin = arra::detail::HashJoin;
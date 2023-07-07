#include <arrow/acero/accumulation_queue.h>
#include <arrow/acero/hash_join_node.h>
#include <arrow/api.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>

#include "arrow/acero/swiss_join_internal.h"
#include "arrow/acero/test_util_internal.h"

#define ARRA_RETURN_NOT_OK ARROW_RETURN_NOT_OK

#define ARRA_SET_AND_RETURN_NOT_OK(status, set)                       \
  do {                                                                \
    ::arrow::Status __s = ::arrow::internal::GenericToStatus(status); \
    if (!__s.ok()) {                                                  \
      [&](const ::arrow::Status& __status) set(__s);                  \
    }                                                                 \
    ARROW_RETURN_IF_(!__s.ok(), __s, ARROW_STRINGIFY(status));        \
  } while (false)

#define ARRA_DCHECK ARROW_DCHECK
#define ARRA_DCHECK_OK ARROW_DCHECK_OK

namespace arra::sketch {

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

using TaskId = size_t;
using ThreadId = size_t;

using Task = std::function<arrow::Status(TaskId, OperatorStatus&)>;
using TaskCont = std::function<arrow::Status()>;
using TaskGroup = std::tuple<Task, size_t, std::optional<TaskCont>>;
using TaskGroups = std::vector<TaskGroup>;

using PipelineTaskSource = std::function<arrow::Status(ThreadId, OperatorStatus&)>;
using PipelineTaskPipe = std::function<arrow::Status(
    ThreadId, std::optional<arrow::ExecBatch>, OperatorStatus&)>;

class SourceOp {
 public:
  virtual ~SourceOp() = default;
  virtual PipelineTaskSource Source() = 0;
  virtual TaskGroups Frontend() = 0;
  virtual TaskGroups Backend() = 0;
};

class PipeOp {
 public:
  virtual ~PipeOp() = default;
  virtual PipelineTaskPipe Pipe() = 0;
  virtual std::optional<PipelineTaskPipe> Drain() = 0;
  virtual std::unique_ptr<SourceOp> Source() = 0;
};

class SinkOp {
 public:
  virtual ~SinkOp() = default;
  virtual PipelineTaskPipe Pipe() = 0;
  virtual std::optional<PipelineTaskPipe> Drain() = 0;
  virtual TaskGroups Frontend() = 0;
  virtual TaskGroups Backend() = 0;
};

constexpr size_t kMaxSourceBatchSize = 1 << 15;
constexpr size_t kMaxPipeBatchSize = 4096;

namespace detail {

using namespace arrow;
using namespace arrow::acero;
using namespace arrow::acero::util;
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

    JoinMatchIterator match_iterator;
  };

  struct ThreadLocalState {
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

 private:
  struct ThreadLocalState {
    int current_start_ = 0;
    JoinResultMaterialize* materialize = nullptr;
  };
  std::vector<ThreadLocalState> local_states_;
};

class HashJoin : public PipeOp {
 public:
  Status Init(QueryContext* ctx, size_t dop, const HashJoinNodeOptions& options,
              const Schema& left_schema, const Schema& right_schema);

  std::shared_ptr<Schema> OutputSchema() const;

  std::unique_ptr<SinkOp> BuildSink();

  PipelineTaskPipe Pipe() override;

  std::optional<PipelineTaskPipe> Drain() override;

  std::unique_ptr<SourceOp> Source() override;

 private:
  QueryContext* ctx_;
  int64_t hardware_flags_;
  MemoryPool* pool_;
  size_t dop_;

  JoinType join_type_;
  std::vector<JoinKeyCmp> key_cmp_;
  HashJoinSchema schema_mgr_;
  const HashJoinProjectionMaps* schema_[2];
  std::shared_ptr<Schema> output_schema_;

  struct ThreadLocalState {
    JoinResultMaterialize materialize;
    std::vector<KeyColumnArray> temp_column_arrays;
    // int64_t num_output_batches;
  };
  std::vector<ThreadLocalState> local_states_;

  ProbeProcessor probe_processor_;

  SwissTableForJoin hash_table_;
  SwissTableForJoinBuild hash_table_build_;
};

class HashJoinBuildSink : public SinkOp {
 public:
  Status Init(QueryContext* ctx, size_t dop, int64_t hardware_flags, MemoryPool* pool,
              const HashJoinProjectionMaps* schema, JoinType join_type,
              SwissTableForJoinBuild* hash_table_build, SwissTableForJoin* hash_table,
              const std::vector<JoinResultMaterialize*>& materialize);

  PipelineTaskPipe Pipe() override;

  std::optional<PipelineTaskPipe> Drain() override;

  TaskGroups Frontend() override;

  TaskGroups Backend() override;

 private:
  QueryContext* ctx_;
  size_t dop_;
  SwissTableForJoinBuild* hash_table_build_;
  std::mutex build_side_mutex_;
  AccumulationQueue build_side_batches_;
  BuildProcessor build_processor_;
};

class HashJoinScanSource : public SourceOp {
 public:
  Status Init(QueryContext* ctx, JoinType join_type, SwissTableForJoin* hash_table,
              const std::vector<JoinResultMaterialize*>& materializ);

  PipelineTaskSource Source() override;

  TaskGroups Frontend() override;

  TaskGroups Backend() override;

 private:
  QueryContext* ctx_;

  ScanProcessor scan_processor_;
};

class MemorySource : public SourceOp {
 public:
  MemorySource(size_t dop, std::vector<ExecBatch> batches);

  PipelineTaskSource Source() override;

  TaskGroups Frontend() override;

  TaskGroups Backend() override;

 private:
  size_t dop_;
  std::vector<ExecBatch> batches_;

  struct ThreadLocalState {
    size_t batch_id = 0;
    size_t batch_end = 0;
    size_t batch_offset = 0;
  };
  std::vector<ThreadLocalState> local_states_;
};

class MemorySink : public SinkOp {
 public:
  MemorySink(BatchesWithSchema& batches);

  PipelineTaskPipe Pipe() override;

  std::optional<PipelineTaskPipe> Drain() override;

  TaskGroups Frontend() override;

  TaskGroups Backend() override;

 private:
  std::mutex mutex_;
  BatchesWithSchema& batches_;
};

struct Pipeline {
  SourceOp* source;
  std::vector<PipeOp*> pipes;
  SinkOp* sink;
};

class PipelineTask {
 public:
  PipelineTask(
      size_t dop, const PipelineTaskSource& source,
      const std::vector<std::pair<PipelineTaskPipe, std::optional<PipelineTaskPipe>>>&
          pipes);

  Status Run(ThreadId thread_id, OperatorStatus& status);

 private:
  Status Pipe(ThreadId thread_id, OperatorStatus& status, size_t pipe_id,
              std::optional<ExecBatch> input);

 private:
  size_t dop_;
  PipelineTaskSource source_;
  std::vector<std::pair<PipelineTaskPipe, std::optional<PipelineTaskPipe>>> pipes_;

  struct ThreadLocalState {
    std::stack<size_t> pipe_stack;
    bool source_done = false;
    std::vector<size_t> drains;
    size_t draining = 0;
  };
  std::vector<ThreadLocalState> local_states_;
};

template <typename Scheduler>
class Driver {
 public:
  Driver(std::vector<Pipeline> pipelines, Scheduler* scheduler)
      : pipelines_(std::move(pipelines)), scheduler_(scheduler) {}

  void Run(size_t dop) {
    for (const auto& pipeline : pipelines_) {
      RunPipeline(dop, pipeline.source, pipeline.pipes, pipeline.sink);
    }
  }

 private:
  void RunPipeline(size_t dop, SourceOp* source, const std::vector<PipeOp*>& pipes,
                   SinkOp* sink) {
    auto sink_be = sink->Backend();
    auto sink_be_tgs = Scheduler::MakeTaskGroups(sink_be);
    auto sink_be_handle = scheduler_->ScheduleTaskGroups(sink_be_tgs);

    std::vector<std::unique_ptr<SourceOp>> source_lifecycles;
    std::vector<std::pair<SourceOp*, size_t>> sources;
    sources.emplace_back(source, 0);
    for (size_t i = 0; i < pipes.size(); i++) {
      if (auto pipe_source = pipes[i]->Source(); pipe_source != nullptr) {
        sources.emplace_back(pipe_source.get(), i + 1);
        source_lifecycles.emplace_back(std::move(pipe_source));
      }
    }

    for (const auto& [source, pipe_start] : sources) {
      RunPipeline(dop, source, pipes, pipe_start, sink);
    }

    auto sink_fe = sink->Frontend();
    auto sink_fe_tgs = Scheduler::MakeTaskGroups(sink_fe);
    auto sink_fe_status = scheduler_->ScheduleTaskGroups(sink_fe_tgs).wait().value();
    ARRA_DCHECK(sink_fe_status.code == OperatorStatusCode::FINISHED);

    auto sink_be_status = sink_be_handle.wait().value();
    ARRA_DCHECK(sink_be_status.code == OperatorStatusCode::FINISHED);
  }

  void RunPipeline(size_t dop, SourceOp* source, const std::vector<PipeOp*>& pipes,
                   size_t pipe_start, SinkOp* sink) {
    auto source_be = source->Backend();
    auto source_be_tgs = Scheduler::MakeTaskGroups(source_be);
    auto source_be_handle = scheduler_->ScheduleTaskGroups(source_be_tgs);

    auto source_fe = source->Frontend();
    auto source_fe_tgs = Scheduler::MakeTaskGroups(source_fe);
    auto source_fe_status = scheduler_->ScheduleTaskGroups(source_fe_tgs).wait().value();
    ARRA_DCHECK(source_fe_status.code == OperatorStatusCode::FINISHED);

    auto source_source = source->Source();
    std::vector<std::pair<PipelineTaskPipe, std::optional<PipelineTaskPipe>>>
        pipe_and_drains;
    for (size_t i = pipe_start; i < pipes.size(); ++i) {
      auto pipe_pipe = pipes[i]->Pipe();
      auto pipe_drain = pipes[i]->Drain();
      pipe_and_drains.emplace_back(std::move(pipe_pipe), std::move(pipe_drain));
    }
    auto sink_pipe = sink->Pipe();
    auto sink_drain = sink->Drain();
    pipe_and_drains.emplace_back(std::move(sink_pipe), std::move(sink_drain));
    PipelineTask pipeline_task(dop, source_source, pipe_and_drains);
    TaskGroup pipeline{[&](ThreadId thread_id, OperatorStatus& status) {
                         return pipeline_task.Run(thread_id, status);
                       },
                       dop, std::nullopt};
    auto pipeline_tg = Scheduler::MakeTaskGroup(pipeline);
    auto pipeline_status = scheduler_->ScheduleTaskGroup(pipeline_tg).wait().value();
    ARRA_DCHECK(pipeline_status.code == OperatorStatusCode::FINISHED);

    auto source_be_status = source_be_handle.wait().value();
    ARRA_DCHECK(source_be_status.code == OperatorStatusCode::FINISHED);
  }

 private:
  std::vector<Pipeline> pipelines_;
  Scheduler* scheduler_;
};

class FollyFutureScheduler {
 private:
  using SchedulerTask = std::pair<OperatorStatus, std::function<OperatorStatus()>>;
  using SchedulerTaskCont = TaskCont;
  using SchedulerTaskHandle = folly::SemiFuture<folly::Unit>;

 public:
  using SchedulerTaskGroup =
      std::pair<std::vector<SchedulerTask>, std::optional<SchedulerTaskCont>>;
  using SchedulerTaskGroups = std::vector<SchedulerTaskGroup>;

  using SchedulerTaskGroupHandle = folly::Future<OperatorStatus>;
  using SchedulerTaskGroupsHandle = folly::Future<OperatorStatus>;

  static SchedulerTaskGroup MakeTaskGroup(const TaskGroup& group);

  static SchedulerTaskGroups MakeTaskGroups(const TaskGroups& groups);

  FollyFutureScheduler(size_t num_threads);

  SchedulerTaskGroupHandle ScheduleTaskGroup(SchedulerTaskGroup& group);

  SchedulerTaskGroupsHandle ScheduleTaskGroups(SchedulerTaskGroups& groups);

 private:
  static SchedulerTask MakeTask(const Task& task, TaskId task_id);

  static SchedulerTaskCont MakeTaskCont(const TaskCont& cont);

  SchedulerTaskHandle ScheduleTask(SchedulerTask& task);

 private:
  folly::CPUThreadPoolExecutor executor_;
};

}  // namespace detail

using HashJoin = detail::HashJoin;
using HashJoinBuildSink = detail::HashJoinBuildSink;
using HashJoinScanSource = detail::HashJoinScanSource;

using MemorySource = detail::MemorySource;
using MemorySink = detail::MemorySink;

using PipelineTask = detail::PipelineTask;

template <typename Scheduler>
using Driver = detail::Driver<Scheduler>;

using FollyFutureScheduler = detail::FollyFutureScheduler;

}  // namespace arra::sketch

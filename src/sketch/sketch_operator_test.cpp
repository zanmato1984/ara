/// NOTE: How to implement hash join probe operator in a parallel-agnostic and
/// future-agnostic fashion.  That is:
/// 1. How to invoke and chain the `Push()` methods of all the operators in this
/// pipeline.
/// 2. How to invoke the `Finish()` methods of all the operators in this pipeline.
/// This is critical for cases like a pipeline having two hash right outer join probe
/// operators, the `Finish()` method of each will scan the hash table and produce
/// non-joined rows.
/// 3. The operator should be sync/async-agnostic, that is, the concern of how to invoke
/// them should be hidden from the operator implementation.
/// 4. The operator should be parallel-agnostic, that is, the concern of how to parallel
/// them should be hidden from the operator implementation.
/// 5. The operator should be backpressure-agnostic, that is, the concern of how to handle
/// backpressure should be hidden from the operator implementation. Try to encapsulate the
/// backpressure handling logic only among source, sink, and optionally the pipeline
/// runner itself.
/// 6. The operator should be cancel-agnostic, that is, the concern of how to cancel
/// all the corresponding tasks when one of them meets error, should be hidden from the
/// operator implementation.

#include <arrow/acero/hash_join.h>
#include <arrow/acero/hash_join_node.h>
#include <arrow/api.h>
#include <arrow/util/logging_internal.h>
#include <arrow/util/future.h>
#include <gtest/gtest.h>

#include "arrow/acero/test_util_internal.h"

enum class OperatorStatusCode : char {
  RUNNING = 0,
  FINISHED = 1,
  SPILLING = 2,
  CANCELLED = 3,
  ERROR = 4,
};

struct OperatorStatus {
  OperatorStatusCode code;
  arrow::Status status;

  static OperatorStatus RUNNING() {
    return OperatorStatus{OperatorStatusCode::RUNNING, arrow::Status::OK()};
  }
  static OperatorStatus FINISHED() {
    return OperatorStatus{OperatorStatusCode::RUNNING, arrow::Status::OK()};
  }
  static OperatorStatus SPILLING() {
    return OperatorStatus{OperatorStatusCode::RUNNING, arrow::Status::OK()};
  }
  static OperatorStatus CANCELLED() {
    return OperatorStatus{OperatorStatusCode::RUNNING, arrow::Status::OK()};
  }
  static OperatorStatus ERROR(arrow::Status status) {
    return OperatorStatus{OperatorStatusCode::RUNNING, std::move(status)};
  }
};

struct Batch {
  int value = 0;
};
using Batches = std::vector<Batch>;

struct OperatorResult {
  OperatorStatus status;
  std::optional<Batches> batches;
};

using LoopTask = std::function<OperatorStatus(size_t /*round*/)>;
using LoopTaskGroup = std::vector<std::pair<LoopTask, size_t /*rounds to loop*/>>;

using PipelineTask =
    std::function<OperatorResult(size_t /*thread id*/, size_t /*task id*/, const Batch&)>;
using PipelineTaskGroup = std::pair<PipelineTask, size_t /*tasks*/>;

template <typename T>
class TaskGroups {
 public:
  virtual ~TaskGroups() = default;

  virtual size_t Size() = 0;
  virtual arrow::Result<T> Next() = 0;
};

using LoopTaskGroups = TaskGroups<LoopTaskGroup>;
using PipelineTaskGroups = TaskGroups<PipelineTaskGroup>;

class TaskGroupsExecutor {
 public:
  virtual ~TaskGroupsExecutor() = default;

  virtual arrow::Status Execute(LoopTaskGroups& groups) = 0;

  virtual arrow::Status Execute(PipelineTaskGroups& groups, size_t dop) = 0;
};

class Operator {
 public:
  virtual ~Operator() = default;

  virtual OperatorResult Push(size_t thread_id, const Batch& batch) {
    return {OperatorStatus::RUNNING(), {}};
  }

  virtual arrow::Result<std::unique_ptr<LoopTaskGroups>> Break(size_t thread_id) {
    return nullptr;
  }

  virtual arrow::Result<std::unique_ptr<PipelineTaskGroups>> Finish() { return nullptr; }
};

class FutureTaskGroupsExecutor : public TaskGroupsExecutor {
 public:
  FutureTaskGroupsExecutor(arrow::internal::Executor* exec) : exec(exec) {}

  arrow::Status Execute(LoopTaskGroups& groups) override {
    arrow::CallbackOptions options{arrow::ShouldSchedule::Always, exec};
    auto fut = arrow::Future<>::MakeFinished();
    for (size_t i = 0; i < groups.Size(); i++) {
      auto next_fut = std::move(fut).Then(
          [&groups]() -> arrow::Result<LoopTaskGroup> {
            ARROW_ASSIGN_OR_RAISE(auto group, groups.Next());
            return group;
          },
          {}, options);
      auto group_fut = std::move(next_fut).Then(
          [options](const LoopTaskGroup& group) {
            std::vector<arrow::Future<OperatorStatus>> task_futs;
            for (auto& task_loop : group) {
              task_futs.push_back(
                  arrow::Future<OperatorStatus>::MakeFinished(OperatorStatus::RUNNING()));
              for (size_t round = 0; round < task_loop.second; round++) {
                task_futs.back() =
                    std::move(task_futs.back())
                        .Then(
                            [task = task_loop.first,
                             round](const OperatorStatus& op_status)
                                -> arrow::Result<OperatorStatus> {
                              if (op_status.code == OperatorStatusCode::CANCELLED) {
                                return arrow::Result<OperatorStatus>(
                                    arrow::Status::Cancelled("Cancelled"));
                              }
                              if (op_status.code == OperatorStatusCode::ERROR) {
                                return arrow::Result<OperatorStatus>(op_status.status);
                              }
                              return arrow::Result<OperatorStatus>(task(round));
                            },
                            {}, options);
              }
            }
            return arrow::All(task_futs);
          },
          {}, options);
      fut = std::move(group_fut).Then(
          [](const std::vector<arrow::Result<OperatorStatus>>& op_statuses) {
            for (const auto& op_status_res : op_statuses) {
              ARROW_ASSIGN_OR_RAISE(auto op_status, op_status_res);
              if (op_status.code == OperatorStatusCode::CANCELLED) {
                return arrow::Status::Cancelled("Cancelled");
              }
              if (op_status.code == OperatorStatusCode::ERROR) {
                return op_status.status;
              }
            }
            return arrow::Status::OK();
          },
          {}, options);
    }
    return fut.status();
  }

  arrow::Status Execute(PipelineTaskGroups& groups, size_t dop) override {
    arrow::CallbackOptions options{arrow::ShouldSchedule::Always, exec};
    std::atomic<size_t> num_rows{0};
    auto fut = arrow::Future<>::MakeFinished();
    for (size_t i = 0; i < groups.Size(); i++) {
      auto next_fut = std::move(fut).Then(
          [&groups]() -> arrow::Result<PipelineTaskGroup> {
            ARROW_ASSIGN_OR_RAISE(auto group, groups.Next());
            return group;
          },
          {}, options);
      auto group_fut = std::move(next_fut).Then(
          [options, &num_rows, dop](const PipelineTaskGroup& group) {
            auto task = group.first;
            auto num_tasks = group.second;
            std::vector<arrow::Future<OperatorStatus>> task_futs;
            for (size_t task_id = 0; task_id < num_tasks;) {
              for (size_t thread_id = 0; thread_id < dop && task_id < num_tasks;
                   thread_id++, task_id++) {
                if (task_id == thread_id) {
                  task_futs.push_back(arrow::Future<OperatorStatus>::MakeFinished(
                      OperatorStatus::RUNNING()));
                }
                task_futs.back() =
                    std::move(task_futs.back())
                        .Then(
                            [&num_rows, task, task_id,
                             thread_id](const OperatorStatus& op_status)
                                -> arrow::Result<OperatorStatus> {
                              if (op_status.code == OperatorStatusCode::CANCELLED) {
                                return arrow::Result<OperatorStatus>(
                                    arrow::Status::Cancelled("Cancelled"));
                              }
                              if (op_status.code == OperatorStatusCode::ERROR) {
                                return arrow::Result<OperatorStatus>(op_status.status);
                              }
                              auto batches = task(thread_id, task_id, {});
                              if (batches.status.code == OperatorStatusCode::RUNNING) {
                                auto sum = std::accumulate(
                                    batches.batches.value().begin(),
                                    batches.batches.value().end(), size_t(0),
                                    [](size_t acc, const Batch& batch) {
                                      return acc + batch.value;
                                    });
                                num_rows += sum;
                              }
                              return batches.status;
                            },
                            {}, options);
              }
            }
            return arrow::All(task_futs);
          },
          {}, options);
      fut = std::move(group_fut).Then(
          [](const std::vector<arrow::Result<OperatorStatus>>& op_statuses) {
            for (const auto& op_status_res : op_statuses) {
              ARROW_ASSIGN_OR_RAISE(auto op_status, op_status_res);
              if (op_status.code == OperatorStatusCode::CANCELLED) {
                return arrow::Status::Cancelled("Cancelled");
              }
              if (op_status.code == OperatorStatusCode::ERROR) {
                return op_status.status;
              }
            }
            return arrow::Status::OK();
          },
          {}, options);
    }
    auto status = fut.status();
    std::cout << "Row count: " << num_rows << std::endl;
    return status;
  }

 private:
  arrow::internal::Executor* exec;
};

TEST(OperatorTest, HashJoinBreakAndFinish) {
  struct HashJoinCase {
    arrow::acero::JoinType join_type;
    size_t dop;
    std::vector<arrow::acero::JoinKeyCmp> key_cmp;
    arrow::compute::Expression filter;
    std::unique_ptr<arrow::acero::HashJoinSchema> schema_mgr;
    std::unique_ptr<arrow::acero::QueryContext> ctx;

    arrow::acero::AccumulationQueue l_batches;
    arrow::acero::AccumulationQueue r_batches;

    HashJoinCase(int batch_size, int num_build_batches, int num_probe_batches,
                 arrow::acero::JoinType join_type, size_t dop)
        : join_type(join_type), dop(dop) {
      std::vector<std::shared_ptr<arrow::DataType>> key_types = {arrow::int32()};
      std::vector<std::shared_ptr<arrow::DataType>> build_payload_types = {
          arrow::int64(), arrow::decimal256(15, 2)};
      std::vector<std::shared_ptr<arrow::DataType>> probe_payload_types = {arrow::int64(),
                                                                           arrow::utf8()};
      double null_percentage = 0.0;
      double cardinality = 1.0;  // Proportion of distinct keys in build side
      double selectivity = 1.0;  // Probability of a match for a given row

      arrow::SchemaBuilder l_schema_builder, r_schema_builder;
      std::vector<arrow::FieldRef> left_keys, right_keys;
      std::vector<arrow::acero::JoinKeyCmp> key_cmp;
      for (size_t i = 0; i < key_types.size(); i++) {
        std::string l_name = "lk" + std::to_string(i);
        std::string r_name = "rk" + std::to_string(i);

        // For integers, selectivity is the proportion of the build interval that
        // overlaps with the probe interval
        uint64_t num_build_rows = num_build_batches * batch_size;

        uint64_t min_build_value = 0;
        uint64_t max_build_value = static_cast<uint64_t>(num_build_rows * cardinality);

        uint64_t min_probe_value =
            static_cast<uint64_t>((1.0 - selectivity) * max_build_value);
        uint64_t max_probe_value = min_probe_value + max_build_value;

        std::unordered_map<std::string, std::string> build_metadata;
        build_metadata["null_probability"] = std::to_string(null_percentage);
        build_metadata["min"] = std::to_string(min_build_value);
        build_metadata["max"] = std::to_string(max_build_value);
        build_metadata["min_length"] = "2";
        build_metadata["max_length"] = "20";

        std::unordered_map<std::string, std::string> probe_metadata;
        probe_metadata["null_probability"] = std::to_string(null_percentage);
        probe_metadata["min"] = std::to_string(min_probe_value);
        probe_metadata["max"] = std::to_string(max_probe_value);

        auto l_field =
            field(l_name, key_types[i], arrow::key_value_metadata(probe_metadata));
        auto r_field =
            field(r_name, key_types[i], arrow::key_value_metadata(build_metadata));

        DCHECK_OK(l_schema_builder.AddField(l_field));
        DCHECK_OK(r_schema_builder.AddField(r_field));

        left_keys.push_back(arrow::FieldRef(l_name));
        right_keys.push_back(arrow::FieldRef(r_name));
        key_cmp.push_back(arrow::acero::JoinKeyCmp::EQ);
      }

      for (size_t i = 0; i < build_payload_types.size(); i++) {
        std::string name = "lp" + std::to_string(i);
        DCHECK_OK(l_schema_builder.AddField(field(name, probe_payload_types[i])));
      }

      for (size_t i = 0; i < build_payload_types.size(); i++) {
        std::string name = "rp" + std::to_string(i);
        DCHECK_OK(r_schema_builder.AddField(field(name, build_payload_types[i])));
      }

      auto l_schema = *l_schema_builder.Finish();
      auto r_schema = *r_schema_builder.Finish();

      arrow::acero::BatchesWithSchema l_batches_with_schema =
          arrow::acero::MakeRandomBatches(l_schema, num_probe_batches, batch_size);
      arrow::acero::BatchesWithSchema r_batches_with_schema =
          arrow::acero::MakeRandomBatches(r_schema, num_build_batches, batch_size);

      for (arrow::compute::ExecBatch& batch : l_batches_with_schema.batches)
        l_batches.InsertBatch(std::move(batch));
      for (arrow::compute::ExecBatch& batch : r_batches_with_schema.batches)
        r_batches.InsertBatch(std::move(batch));

      filter = arrow::compute::literal(true);
      schema_mgr = std::make_unique<arrow::acero::HashJoinSchema>();
      DCHECK_OK(schema_mgr->Init(join_type, *l_batches_with_schema.schema, left_keys,
                                 *r_batches_with_schema.schema, right_keys, filter, "l_",
                                 "r_"));

      auto* memory_pool = arrow::default_memory_pool();
      ctx = std::make_unique<arrow::acero::QueryContext>(
          arrow::acero::QueryOptions{},
          arrow::compute::ExecContext(memory_pool, NULLPTR, NULLPTR));
      DCHECK_OK(ctx->Init(NULLPTR));
    }
  };

  using Task = std::function<arrow::Status(size_t, int64_t)>;
  using TaskCont = std::function<arrow::Status(size_t)>;
  using TaskGroup = std::pair<Task, TaskCont>;

  // TODO: These two task groups and the subsequent dispatcher need a re-design.
  class HashJoinBuildTaskGroups : public LoopTaskGroups {
   public:
    HashJoinBuildTaskGroups(TaskCont entry,
                            std::unordered_map<int, TaskGroup> task_groups, size_t dop)
        : task_groups(std::move(task_groups)),
          dop(dop),
          current_entry(std::move(entry)),
          current_task_group(-1),
          current_num_tasks(0),
          pos(0) {}

    size_t Size() override { return task_groups.size() + 1; }

    arrow::Result<LoopTaskGroup> Next() override {
      ARROW_RETURN_NOT_OK(current_entry(0));
      if (pos == task_groups.size()) {
        return arrow::Result<LoopTaskGroup>(LoopTaskGroup{});
      }
      pos++;
      auto it = task_groups.find(current_task_group);
      ARROW_RETURN_IF(it == task_groups.end(),
                      arrow::Status::Invalid("Build task group not found"));
      auto& task = it->second.first;
      current_entry = it->second.second;

      LoopTaskGroup task_group;
      // Spread tasks across threads.
      auto num_tasks_per_thread = (current_num_tasks + dop - 1) / dop;
      for (size_t task_id = 0; task_id < current_num_tasks;
           task_id += num_tasks_per_thread) {
        auto thread_id = task_group.size();
        auto num_tasks = std::min(num_tasks_per_thread, current_num_tasks - task_id);
        task_group.push_back(std::pair(
            [task, num_tasks_per_thread, task_id, thread_id, num_tasks](size_t round) {
              auto status = task(thread_id, thread_id * num_tasks_per_thread + round);
              if (status.ok()) {
                return OperatorStatus::RUNNING();
              }
              if (status.IsCancelled()) {
                return OperatorStatus::CANCELLED();
              }
              return OperatorStatus::ERROR(std::move(status));
            },
            num_tasks));
      }

      return arrow::Result(std::move(task_group));
    }

    arrow::Status StartTaskGroup(int task_group, int64_t num_tasks) {
      current_task_group = task_group;
      current_num_tasks = num_tasks;
      return arrow::Status::OK();
    }

   private:
    std::unordered_map<int, TaskGroup> task_groups;
    const size_t dop;

    TaskCont current_entry;
    int current_task_group;
    int64_t current_num_tasks;
    size_t pos;
  };

  class HashJoinProbeResultHolder {
   public:
    HashJoinProbeResultHolder(size_t dop) : holder(dop) {}

    void Emplace(size_t thread_id, arrow::compute::ExecBatch batch) {
      // std::cout << "Thread " << thread_id << ": " << batch.ToString() << std::endl;
      holder[thread_id].push_back(Batch{static_cast<int>(batch.length)});
    }

    Batches GetBatches(size_t thread_id) { return std::move(holder[thread_id]); }

   private:
    std::vector<Batches> holder;
  };

  class HashJoinProbeTaskGroups : public PipelineTaskGroups {
   public:
    HashJoinProbeTaskGroups(TaskCont entry,
                            std::unordered_map<int, TaskGroup> task_groups, size_t dop,
                            HashJoinProbeResultHolder& holder)
        : task_groups(std::move(task_groups)),
          dop(dop),
          holder(holder),
          current_entry(std::move(entry)),
          current_task_group(-1),
          current_num_tasks(0),
          pos(0) {}

    size_t Size() override { return task_groups.size() + 1; }

    arrow::Result<PipelineTaskGroup> Next() override {
      ARROW_RETURN_NOT_OK(current_entry(0));
      if (pos == task_groups.size()) {
        return arrow::Result<PipelineTaskGroup>(PipelineTaskGroup{
            [&holder = this->holder](size_t thread_id, size_t task_id,
                                     const Batch&) -> OperatorResult {
              return {OperatorStatus::RUNNING(), holder.GetBatches(thread_id)};
            },
            1});
      }
      pos++;
      auto it = task_groups.find(current_task_group);
      ARROW_RETURN_IF(it == task_groups.end(),
                      arrow::Status::Invalid("Probe task group not found"));
      auto& task = it->second.first;
      current_entry = it->second.second;

      auto pipeline_task = [task, &holder = this->holder](
                               size_t thread_id, size_t task_id,
                               const Batch&) -> OperatorResult {
        auto status = task(thread_id, task_id);
        if (status.ok()) {
          return {OperatorStatus::RUNNING(), holder.GetBatches(thread_id)};
        }
        if (status.IsCancelled()) {
          return {OperatorStatus::CANCELLED(), {}};
        }
        return {OperatorStatus::ERROR(std::move(status)), {}};
      };
      PipelineTaskGroup task_group{std::move(pipeline_task), current_num_tasks};

      return arrow::Result(std::move(task_group));
    }

    arrow::Status StartTaskGroup(int task_group, int64_t num_tasks) {
      current_task_group = task_group;
      current_num_tasks = num_tasks;
      return arrow::Status::OK();
    }

   private:
    std::unordered_map<int, TaskGroup> task_groups;
    const size_t dop;
    HashJoinProbeResultHolder& holder;

    TaskCont current_entry;
    int current_task_group;
    int64_t current_num_tasks;
    size_t pos;
  };

  class HashJoinTaskGroupDispatcher {
   public:
    int RegisterTaskGroup(Task task, TaskCont task_cont) {
      auto cur = n++;
      auto task_group = std::make_pair(std::move(task), std::move(task_cont));
      if (cur == 0) {
        build_task_group = std::move(task_group);
        return BUILD;
      }
      if (cur == 1) {
        merge_task_group = std::move(task_group);
        return MERGE;
      }
      if (cur == 2) {
        scan_task_group = std::move(task_group);
        return SCAN;
      }
      return -1;
    }

    arrow::Status StartTaskGroup(int task_group, int64_t num_tasks) {
      if (task_group == BUILD || task_group == MERGE) {
        return build_task_groups->StartTaskGroup(task_group, num_tasks);
      }
      if (task_group == SCAN) {
        return probe_task_groups->StartTaskGroup(task_group, num_tasks);
      }
      return arrow::Status::UnknownError("Unknown task group");
    }

    void Seal(TaskCont build_entry, TaskCont probe_entry, size_t dop,
              HashJoinProbeResultHolder& holder) {
      {
        std::unordered_map<int, TaskGroup> task_groups;
        task_groups.emplace(BUILD, std::move(build_task_group));
        task_groups.emplace(MERGE, std::move(merge_task_group));
        build_task_groups_unique = std::make_unique<HashJoinBuildTaskGroups>(
            std::move(build_entry), std::move(task_groups), dop);
        build_task_groups = build_task_groups_unique.get();
      }
      {
        std::unordered_map<int, TaskGroup> task_groups;
        task_groups.emplace(SCAN, std::move(scan_task_group));
        probe_task_groups_unique = std::make_unique<HashJoinProbeTaskGroups>(
            std::move(probe_entry), std::move(task_groups), dop, holder);
        probe_task_groups = probe_task_groups_unique.get();
      }
    }

    std::unique_ptr<LoopTaskGroups> BuilTaskGroups() {
      return std::move(build_task_groups_unique);
    }

    std::unique_ptr<PipelineTaskGroups> ProbeTaskGroups() {
      return std::move(probe_task_groups_unique);
    }

   private:
    enum HashJoinTaskGroupType : int {
      BUILD,
      MERGE,
      SCAN,
    };

    size_t n = 0;
    TaskGroup build_task_group;
    TaskGroup merge_task_group;
    TaskGroup scan_task_group;

    std::unique_ptr<HashJoinBuildTaskGroups> build_task_groups_unique = nullptr;
    std::unique_ptr<HashJoinProbeTaskGroups> probe_task_groups_unique = nullptr;
    HashJoinBuildTaskGroups* build_task_groups = nullptr;
    HashJoinProbeTaskGroups* probe_task_groups = nullptr;
  };

  class HashJoinOperator : public Operator {
   public:
    HashJoinOperator(std::shared_ptr<HashJoinTaskGroupDispatcher> dispatcher)
        : dispatcher(std::move(dispatcher)) {}

    ~HashJoinOperator() override = default;

    arrow::Result<std::unique_ptr<LoopTaskGroups>> Break(size_t thread_id) override {
      return dispatcher->BuilTaskGroups();
    }

    arrow::Result<std::unique_ptr<PipelineTaskGroups>> Finish() override {
      return dispatcher->ProbeTaskGroups();
    }

   private:
    std::shared_ptr<HashJoinTaskGroupDispatcher> dispatcher;
  };

  {
    int batch_size = 32769;
    int num_build_batches = 1;
    int num_probe_batches = 1;
    arrow::acero::JoinType join_type = arrow::acero::JoinType::RIGHT_OUTER;
    size_t dop = 16;
    size_t num_threads = 7;

    HashJoinCase join_case(batch_size, num_build_batches, num_probe_batches, join_type,
                           dop);

    std::cout << "Build row count: " << join_case.r_batches.row_count() << std::endl;
    std::cout << "Probe row count: " << join_case.l_batches.row_count() << std::endl;

    auto dispatcher = std::make_shared<HashJoinTaskGroupDispatcher>();
    HashJoinProbeResultHolder holder(dop);

    auto join_impl = []() {
      auto join_impl = arrow::acero::HashJoinImpl::MakeSwiss();
      ARROW_DCHECK_OK(join_impl.status());
      return std::move(*join_impl);
    }();
    ARROW_DCHECK_OK(join_impl->Init(
        join_case.ctx.get(), join_case.join_type, join_case.dop,
        &(join_case.schema_mgr->proj_maps[0]), &(join_case.schema_mgr->proj_maps[1]),
        std::move(join_case.key_cmp), std::move(join_case.filter),
        [&](Task task, TaskCont task_cont) {
          return dispatcher->RegisterTaskGroup(std::move(task), std::move(task_cont));
        },
        [&](int task_group_id, int64_t num_tasks) {
          return dispatcher->StartTaskGroup(task_group_id, num_tasks);
        },
        [&](int64_t thread_id, arrow::compute::ExecBatch batch) {
          holder.Emplace(thread_id, std::move(batch));
          return arrow::Status::OK();
        },
        [](int64_t x) { return arrow::Status::OK(); }));

    dispatcher->Seal(
        [&](int64_t thread_id) {
          return join_impl->BuildHashTable(thread_id, std::move(join_case.r_batches),
                                           [&](size_t) { return arrow::Status::OK(); });
        },
        [&](int64_t thread_id) { return join_impl->ProbingFinished(thread_id); }, dop,
        holder);

    HashJoinOperator join(dispatcher);

    auto thread_pool = [&]() {
      auto thread_pool = arrow::internal::ThreadPool::Make(num_threads);
      ARROW_DCHECK_OK(thread_pool.status());
      return *thread_pool;
    }();
    FutureTaskGroupsExecutor executor(thread_pool.get());

    {
      auto build_task_groups = [&]() {
        auto build_task_groups = join.Break(0);
        ARROW_DCHECK_OK(build_task_groups.status());
        return std::move(*build_task_groups);
      }();
      ARROW_DCHECK_OK(executor.Execute(*build_task_groups));
    }

    // for (size_t i = 0; i < join_case.l_batches.batch_count(); i++) {
    //   ARROW_DCHECK_OK(join_impl->ProbeSingleBatch(0, join_case.l_batches[i]));
    // }

    {
      auto probe_task_groups = [&]() {
        auto probe_task_groups = join.Finish();
        ARROW_DCHECK_OK(probe_task_groups.status());
        return std::move(*probe_task_groups);
      }();
      ARROW_DCHECK_OK(executor.Execute(*probe_task_groups, dop));
    }
  }
}

TEST(OperatorTest, PushAndFinish) {
  // class PushAndFinishOperator : public Operator {
  //  public:
  //   ~PushAndFinishOperator() override = default;

  //  OperatorResult Push(size_t thread_id, const Batch& batch) override {
  //   return {OperatorStatus::RUNNING(), {}};
  // }

  //   arrow::Result<std::shared_ptr<LoopTaskGroups>> Finish() override {
  //     return nullptr;
  //   }
  // };

  // {
  //   PushAndFinishOperator op;
  //   auto task_groups = op.Break(0);
  //   ASSERT_TRUE(task_groups.ok());
  //   ASSERT_EQ(task_groups.ValueOrDie(), nullptr);
  //   task_groups = op.Finish(0);
  //   ASSERT_TRUE(task_groups.ok());
  //   ASSERT_EQ(task_groups.ValueOrDie(), nullptr);
  // }
}

// TODO: Case about operator ping-pong between RUNNING and SPILLING states.

// TODO: Case about backpressure from sink to source.

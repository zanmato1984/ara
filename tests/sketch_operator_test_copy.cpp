#include <arrow/api.h>
#include <arrow/compute/exec/hash_join.h>
#include <gtest/gtest.h>

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

struct OperatorResult {
  OperatorStatus status;
  std::optional<Batch> batch;
};

class TaskGroups {
 public:
  using Task = std::function<OperatorResult(size_t /*round*/)>;
  using TaskGroup = std::vector<std::pair<Task, size_t /*num_rounds*/>>;

  virtual ~TaskGroups() = default;

  virtual bool HasNext() = 0;
  virtual arrow::Result<TaskGroup> Next() = 0;
};

class Operator {
 public:
  virtual ~Operator() = default;

  virtual OperatorResult Push(size_t thread_id, const Batch& batch) {
    return {OperatorStatus::RUNNING(), {}};
  }
  virtual arrow::Result<std::unique_ptr<TaskGroups>> Break(size_t dop) { return nullptr; }
  virtual OperatorResult Finish(size_t thread_id) {
    return {OperatorStatus::RUNNING(), {}};
  }
};

TEST(OperatorTest, HashJoinBuild) {
  class BuildTaskGroups : public TaskGroups {
   public:
    explicit BuildTaskGroups(arrow::compute::HashJoinImpl* join_impl, size_t dop)
        : join_impl(join_impl),
          dop(dop),
          count(0),
          current_build_task_group(-1),
          current_num_build_tasks(0),
          next_build_task_cont(
              [this](size_t) { return this->join_impl->BuildHashTable(0, {}, {}); }) {}

    bool HasNext() override { return count < build_task_groups.size(); }

    arrow::Result<TaskGroup> Next() override {
      ARROW_RETURN_NOT_OK(next_build_task_cont(0));
      auto it = build_task_groups.find(current_build_task_group);
      ARROW_RETURN_IF(it == build_task_groups.end(),
                      arrow::Status::Invalid("Build task group not found"));
      auto& build_task = it->second.first;
      auto& build_task_cont = it->second.second;

      TaskGroup task_group;
      task_group.reserve(current_num_build_tasks);

      // Spread tasks across threads.
      auto num_tasks_per_thread = (current_num_build_tasks + dop - 1) / dop;
      for (size_t build_task_id = 0; build_task_id < current_num_build_tasks;
           build_task_id += num_tasks_per_thread) {
        auto thread_id = task_group.size();
        auto num_tasks =
            std::min(num_tasks_per_thread, current_num_build_tasks - build_task_id);
        task_group.emplace_back(
            [build_task, build_task_cont, num_tasks_per_thread, build_task_id, thread_id,
             num_tasks](size_t round) {
              auto status =
                  build_task(thread_id, thread_id * num_tasks_per_thread + round);
              if (status.ok()) {
                return OperatorResult{OperatorStatus::RUNNING(), {}};
              } else {
                return OperatorResult{OperatorStatus::ERROR(std::move(status)), {}};
              }
            },
            num_tasks);
      }

      count++;
      next_build_task_cont = build_task_cont;
      return arrow::Result(std::move(task_group));
    }

   private:
    arrow::compute::HashJoinImpl* join_impl;

   private:
    using BuildTask = std::function<arrow::Status(size_t, int64_t)>;
    using BuildTaskCont = std::function<arrow::Status(size_t)>;
    using BuildTaskGroup = std::pair<BuildTask, BuildTaskCont>;

    int RegisterTaskGroup(BuildTask build_task, BuildTaskCont build_task_cont) {
      int id = build_task_groups.size();
      build_task_groups.emplace(
          id, std::make_pair(std::move(build_task), std::move(build_task_cont)));
      return id;
    }

    arrow::Status StartTaskGroup(int build_task_group, int64_t num_build_tasks) {
      current_build_task_group = build_task_group;
      current_num_build_tasks = num_build_tasks;
      return arrow::Status::OK();
    }

   private:
    const size_t dop;
    std::unordered_map<int, BuildTaskGroup> build_task_groups;
    size_t count;
    int current_build_task_group;
    int64_t current_num_build_tasks;
    BuildTaskCont next_build_task_cont;
  };

  class HashJoinBuildOperator : public Operator {
   public:
    ~HashJoinBuildOperator() override = default;

    arrow::Result<std::unique_ptr<TaskGroups>> Break(size_t dop) override {
      auto join_impl_ = arrow::compute::HashJoinImpl::MakeSwiss();
      // TODO: Init join_impl_.
      ARROW_RETURN_NOT_OK(join_impl_);
      join_impl = std::move(*join_impl_);
      return std::make_unique<BuildTaskGroups>(join_impl.get(), dop);
    }

   private:
    std::shared_ptr<arrow::compute::HashJoinImpl> join_impl = nullptr;
  };
}
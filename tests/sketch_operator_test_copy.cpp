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

class TasksIterator {
 public:
  using Task = std::function<arrow::Status(size_t)>;
  using Tasks = std::vector<Task>;

  virtual ~TasksIterator() = default;

  virtual bool HasNext() = 0;
  virtual arrow::Result<Tasks> Next() = 0;
};

class Operator {
 public:
  virtual ~Operator() = default;

  virtual OperatorResult Push(size_t thread_id, const Batch & batch) {
    return {OperatorStatus::RUNNING(), {}};
  }
  virtual std::unique_ptr<TasksIterator> Break(size_t dop) { return nullptr; }
  virtual OperatorResult Finish(size_t thread_id) { return {OperatorStatus::RUNNING(), {}}; }
};

TEST(OperatorTest, HashJoinBuild) {
  class HashJoinBuildTasksIterator : public TasksIterator {
   public:
    explicit HashJoinBuildTasksIterator(
        std::shared_ptr<arrow::compute::HashJoinImpl>& join_impl, size_t dop)
        : join_impl(join_impl),
          dop(dop),
          count(0),
          current_task_group(-1),
          current_num_tasks(0),
          next_task_cont(
              [this](size_t) { return this->join_impl->BuildHashTable(0, {}, {}); }) {}

    bool HasNext() override { return count < task_groups.size(); }

    arrow::Result<Tasks> Next() override {
      ARROW_RETURN_NOT_OK(next_task_cont(0));
      auto it = task_groups.find(current_task_group);
      ARROW_RETURN_IF(it == task_groups.end(), arrow::Status::Invalid("Task group not found"));
      auto& task = it->second.first;
      auto& task_cont = it->second.second;

      Tasks tasks(dop);
      // Spread tasks across threads.

      count++;
      next_task_cont = task_cont;
      return arrow::Result(std::move(tasks));
    }

   private:
    std::shared_ptr<arrow::compute::HashJoinImpl> join_impl;

   private:
    using Task = std::function<arrow::Status(size_t, int64_t)>;
    using TaskCont = std::function<arrow::Status(size_t)>;
    using TaskGroup = std::pair<Task, TaskCont>;

    int RegisterTaskGroup(Task task, TaskCont task_cont) {
      int id = task_groups.size();
      task_groups.emplace(id, std::make_pair(std::move(task), std::move(task_cont)));
      return id;
    }

    arrow::Status StartTaskGroup(int task_group, int64_t num_tasks) {
      current_task_group = task_group;
      current_num_tasks = num_tasks;
      return arrow::Status::OK();
    }

    private:
    const size_t dop;
    std::unordered_map<int, TaskGroup> task_groups;
    size_t count;
    int current_task_group;
    int64_t current_num_tasks;
    TaskCont next_task_cont;
  };

  class HashJoinBuild : public Operator {
   public:
    explicit HashJoinBuild(std::shared_ptr<arrow::compute::HashJoinImpl>& join_impl, size_t dop)
        : join_impl(join_impl), task_iterator(std::make_unique<HashJoinBuildTasksIterator>(join_impl, dop)) {}
    ~HashJoinBuild() override = default;

    std::unique_ptr<TasksIterator> Break(size_t dop) override {
      return std::move(task_iterator);
    }

   private:
    std::shared_ptr<arrow::compute::HashJoinImpl> join_impl;
    std::unique_ptr<TasksIterator> task_iterator;
  };
}
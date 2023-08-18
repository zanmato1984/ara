#include "task.h"
#include "ara/util/util.h"
#include "task_status.h"

#include <gtest/gtest.h>

using namespace ara;
using namespace ara::task;

TEST(TaskTest, BasicTask) {
  auto task_impl = [](const TaskContext& context, TaskId task_id) -> TaskResult {
    return TaskStatus::Finished();
  };

  Task task(task_impl, "BasicTask", "Do nothing but finish directly");
  TaskContext context;
  auto res = task(context, 0);
  ASSERT_TRUE(res.ok());
  ASSERT_TRUE(res->IsFinished());
}

TEST(TaskTest, BasicContinuation) {
  auto cont_impl = [](const TaskContext& context) -> TaskResult {
    return TaskStatus::Finished();
  };

  Continuation cont(cont_impl, "BasicContinuation", "Do nothing but finish directly");
  TaskContext context;
  auto res = cont(context);
  ASSERT_TRUE(res.ok());
  ASSERT_TRUE(res->IsFinished());
}

struct TaskTrace {
  std::string name;
  std::string desc;
  TaskId id;
  std::optional<TaskStatus> status;
};

bool operator==(const TaskTrace& t1, const TaskTrace& t2) {
  return t1.name == t2.name && t1.desc == t2.desc && t1.id == t2.id &&
         t1.status == t2.status;
}

TEST(TaskTest, TaskObserver) {
  auto task_continue_impl = [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Continue();
  };
  Task task_continue(task_continue_impl, "TaskContinue", "Always continue");
  auto task_continue_trace = [](TaskId task_id, bool end) -> TaskTrace {
    return {"TaskContinue", "Always continue", task_id,
            end ? std::optional<TaskStatus>(TaskStatus::Continue()) : std::nullopt};
  };

  auto task_backpressure_impl = [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Backpressure(42);
  };
  Task task_backpressure(task_backpressure_impl, "TaskBackpressure",
                         "Always backpressure");
  auto task_backpressure_trace = [](TaskId task_id, bool end) -> TaskTrace {
    return {"TaskBackpressure", "Always backpressure", task_id,
            end ? std::optional<TaskStatus>(TaskStatus::Backpressure(42)) : std::nullopt};
  };

  auto task_finished_impl = [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  };
  Task task_finished(task_finished_impl, "TaskFinished", "Always finish");
  auto task_finished_trace = [](TaskId task_id, bool end) -> TaskTrace {
    return {"TaskFinished", "Always finish", task_id,
            end ? std::optional<TaskStatus>(TaskStatus::Finished()) : std::nullopt};
  };

  struct TestTaskObserver : public TaskObserver {
    std::vector<TaskTrace> traces;

    Status OnTaskBegin(const Task& task, const TaskContext&, TaskId task_id) override {
      traces.emplace_back(
          TaskTrace{task.GetName(), task.GetDesc(), task_id, std::nullopt});
      return Status::OK();
    }

    Status OnTaskEnd(const Task& task, const TaskContext&, TaskId task_id,
                     const TaskResult& result) override {
      ARA_DCHECK(result.ok());
      traces.emplace_back(TaskTrace{task.GetName(), task.GetDesc(), task_id, *result});
      return Status::OK();
    }
  };

  TaskContext context;
  context.task_observer = std::make_unique<TestTaskObserver>();
  auto task_observer = dynamic_cast<TestTaskObserver*>(context.task_observer.get());

  std::ignore = task_continue(context, 0);
  std::ignore = task_continue(context, 1);
  std::ignore = task_finished(context, 0);
  std::ignore = task_backpressure(context, 1);
  std::ignore = task_backpressure(context, 0);
  std::ignore = task_finished(context, 1);

  ASSERT_EQ(task_observer->traces.size(), 12);
  ASSERT_EQ(task_observer->traces[0], task_continue_trace(0, false));
  ASSERT_EQ(task_observer->traces[1], task_continue_trace(0, true));
  ASSERT_EQ(task_observer->traces[2], task_continue_trace(1, false));
  ASSERT_EQ(task_observer->traces[3], task_continue_trace(1, true));
  ASSERT_EQ(task_observer->traces[4], task_finished_trace(0, false));
  ASSERT_EQ(task_observer->traces[5], task_finished_trace(0, true));
  ASSERT_EQ(task_observer->traces[6], task_backpressure_trace(1, false));
  ASSERT_EQ(task_observer->traces[7], task_backpressure_trace(1, true));
  ASSERT_EQ(task_observer->traces[8], task_backpressure_trace(0, false));
  ASSERT_EQ(task_observer->traces[9], task_backpressure_trace(0, true));
  ASSERT_EQ(task_observer->traces[10], task_finished_trace(1, false));
  ASSERT_EQ(task_observer->traces[11], task_finished_trace(1, true));
}

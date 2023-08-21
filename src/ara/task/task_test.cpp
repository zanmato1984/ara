#include "task.h"
#include "task_group.h"
#include "task_observer.h"
#include "task_status.h"

#include <ara/util/util.h>
#include <gtest/gtest.h>

using namespace ara;
using namespace ara::task;

TEST(TaskTest, BasicTask) {
  auto task_impl = [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  };

  Task task("BasicTask", "Do nothing but finish directly", task_impl);
  TaskContext context;
  auto res = task(context, 0);
  ASSERT_TRUE(res.ok());
  ASSERT_TRUE(res->IsFinished());
}

TEST(TaskTest, BasicContinuation) {
  auto cont_impl = [](const TaskContext&) -> TaskResult {
    return TaskStatus::Finished();
  };

  Continuation cont("BasicContinuation", "Do nothing but finish directly", cont_impl);
  TaskContext context;
  auto res = cont(context);
  ASSERT_TRUE(res.ok());
  ASSERT_TRUE(res->IsFinished());
}

struct TaskTrace {
  std::string name;
  std::string desc;
  std::optional<TaskId> id;
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
  Task task_continue("TaskContinue", "Always continue", task_continue_impl);
  auto task_continue_trace = [](TaskId task_id, bool end) -> TaskTrace {
    return {"TaskContinue", "Always continue", task_id,
            end ? std::optional<TaskStatus>(TaskStatus::Continue()) : std::nullopt};
  };

  auto task_backpressure_impl = [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Backpressure(42);
  };
  Task task_backpressure("TaskBackpressure", "Always backpressure",
                         task_backpressure_impl);
  auto task_backpressure_trace = [](TaskId task_id, bool end) -> TaskTrace {
    return {"TaskBackpressure", "Always backpressure", task_id,
            end ? std::optional<TaskStatus>(TaskStatus::Backpressure(42)) : std::nullopt};
  };

  auto task_finished_impl = [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  };
  Task task_finished("TaskFinished", "Always finish", task_finished_impl);
  auto task_finished_trace = [](TaskId task_id, bool end) -> TaskTrace {
    return {"TaskFinished", "Always finish", task_id,
            end ? std::optional<TaskStatus>(TaskStatus::Finished()) : std::nullopt};
  };

  auto cont_cancelled_impl = [](const TaskContext&) -> TaskResult {
    return TaskStatus::Cancelled();
  };
  Continuation cont_cancelled("ContinuationCancelled", "Always cancel",
                              cont_cancelled_impl);
  auto cont_cancelled_trace = [](bool end) -> TaskTrace {
    return {"ContinuationCancelled", "Always cancel", std::nullopt,
            end ? std::optional<TaskStatus>(TaskStatus::Cancelled()) : std::nullopt};
  };

  auto cont_yield_impl = [](const TaskContext&) -> TaskResult {
    return TaskStatus::Yield();
  };
  Continuation cont_yield("ContinuationYield", "Always yield", cont_yield_impl);
  auto cont_yield_trace = [](bool end) -> TaskTrace {
    return {"ContinuationYield", "Always yield", std::nullopt,
            end ? std::optional<TaskStatus>(TaskStatus::Yield()) : std::nullopt};
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

    Status OnContinuationBegin(const Continuation& cont, const TaskContext&) override {
      traces.emplace_back(
          TaskTrace{cont.GetName(), cont.GetDesc(), std::nullopt, std::nullopt});
      return Status::OK();
    }

    Status OnContinuationEnd(const Continuation& cont, const TaskContext&,
                             const TaskResult& result) override {
      ARA_DCHECK(result.ok());
      traces.emplace_back(
          TaskTrace{cont.GetName(), cont.GetDesc(), std::nullopt, *result});
      return Status::OK();
    }
  };

  TestTaskObserver task_observer;
  TaskContext context;
  context.task_observer = &task_observer;

  std::ignore = task_continue(context, 0);
  std::ignore = task_continue(context, 1);
  std::ignore = task_finished(context, 0);
  std::ignore = task_backpressure(context, 1);
  std::ignore = task_backpressure(context, 0);
  std::ignore = task_finished(context, 1);
  std::ignore = cont_cancelled(context);
  std::ignore = cont_yield(context);

  ASSERT_EQ(task_observer.traces.size(), 16);
  ASSERT_EQ(task_observer.traces[0], task_continue_trace(0, false));
  ASSERT_EQ(task_observer.traces[1], task_continue_trace(0, true));
  ASSERT_EQ(task_observer.traces[2], task_continue_trace(1, false));
  ASSERT_EQ(task_observer.traces[3], task_continue_trace(1, true));
  ASSERT_EQ(task_observer.traces[4], task_finished_trace(0, false));
  ASSERT_EQ(task_observer.traces[5], task_finished_trace(0, true));
  ASSERT_EQ(task_observer.traces[6], task_backpressure_trace(1, false));
  ASSERT_EQ(task_observer.traces[7], task_backpressure_trace(1, true));
  ASSERT_EQ(task_observer.traces[8], task_backpressure_trace(0, false));
  ASSERT_EQ(task_observer.traces[9], task_backpressure_trace(0, true));
  ASSERT_EQ(task_observer.traces[10], task_finished_trace(1, false));
  ASSERT_EQ(task_observer.traces[11], task_finished_trace(1, true));
  ASSERT_EQ(task_observer.traces[12], cont_cancelled_trace(false));
  ASSERT_EQ(task_observer.traces[13], cont_cancelled_trace(true));
  ASSERT_EQ(task_observer.traces[14], cont_yield_trace(false));
  ASSERT_EQ(task_observer.traces[15], cont_yield_trace(true));
}

TEST(TaskTest, BasicTaskGroup) {
  auto task_impl = [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  };
  auto cont_impl = [](const TaskContext&) -> TaskResult {
    return TaskStatus::Finished();
  };
  auto notify = [](const TaskContext&) -> Status { return Status::OK(); };

  Task task("BasicTask", "Do nothing but finish directly", task_impl);
  Continuation cont("BasicCont", "Do nothing but finish directly", cont_impl);
  TaskGroup tg("BasicTaskGroup", "Do nothing", std::move(task), 1, std::move(cont),
               std::move(notify));
  auto task_ref = tg.GetTask();
  auto num_tasks = tg.NumTasks();
  auto cont_ref = tg.GetContinuation();

  ASSERT_EQ(num_tasks, 1);
  ASSERT_TRUE(cont_ref.has_value());

  TaskContext context;
  {
    auto res = task_ref(context, 0);
    ASSERT_TRUE(res.ok());
    ASSERT_TRUE(res->IsFinished());
  }

  {
    auto res = cont_ref.value()(context);
    ASSERT_TRUE(res.ok());
    ASSERT_TRUE(res->IsFinished());
  }

  {
    auto status = tg.OnBegin(context);
    ASSERT_TRUE(status.ok());
  }

  {
    auto status = tg.OnEnd(context, TaskStatus::Finished());
    ASSERT_TRUE(status.ok());
  }

  {
    auto status = tg.NotifyFinish(context);
    ASSERT_TRUE(status.ok());
  }
}

struct TaskGroupTrace {
  std::string name;
  std::string desc;
  std::variant<TaskResult, Status> result;
};

bool operator==(const TaskGroupTrace& t1, const TaskGroupTrace& t2) {
  return t1.name == t2.name && t1.desc == t2.desc && t1.result == t2.result;
}

TEST(TaskTest, TaskGroupObserver) {
  auto task_impl = [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  };
  auto cont_impl = [](const TaskContext&) -> TaskResult {
    return TaskStatus::Finished();
  };
  auto notify = [](const TaskContext&) -> Status {
    return Status::UnknownError("Notify");
  };

  std::string name = "TaskGroup";
  std::string desc = "Do nothing";
  Task task("", "", task_impl);
  Continuation cont("", "", cont_impl);

  TaskGroup tg(name, desc, task, 1, cont, notify);

  struct TestTaskObserver : public TaskObserver {
    std::vector<TaskGroupTrace> traces;

    Status OnTaskGroupBegin(const TaskGroup& tg, const TaskContext& context) {
      traces.emplace_back(
          TaskGroupTrace{tg.GetName(), tg.GetDesc(), Status::UnknownError("Begin")});
      return Status::OK();
    }

    Status OnTaskGroupEnd(const TaskGroup& tg, const TaskContext& context,
                          const TaskResult& result) {
      traces.emplace_back(TaskGroupTrace{tg.GetName(), tg.GetDesc(), result});
      return Status::OK();
    }

    Status OnNotifyFinishBegin(const TaskGroup& tg, const TaskContext& context) {
      traces.emplace_back(TaskGroupTrace{"Notify" + tg.GetName(), tg.GetDesc(),
                                         Status::UnknownError("NotifyBegin")});
      return Status::OK();
    }

    Status OnNotifyFinishEnd(const TaskGroup& tg, const TaskContext& context,
                             const Status& status) {
      traces.emplace_back(TaskGroupTrace{"Notify" + tg.GetName(), tg.GetDesc(), status});
      return Status::OK();
    }
  };

  TestTaskObserver task_observer;
  TaskContext context;
  context.task_observer = &task_observer;

  std::ignore = tg.OnBegin(context);
  std::ignore = tg.NotifyFinish(context);
  std::ignore = tg.OnEnd(context, TaskStatus::Cancelled());

  ASSERT_EQ(task_observer.traces.size(), 4);
  ASSERT_EQ(task_observer.traces[0],
            (TaskGroupTrace{name, desc, Status::UnknownError("Begin")}));
  ASSERT_EQ(task_observer.traces[1],
            (TaskGroupTrace{"Notify" + name, desc, Status::UnknownError("NotifyBegin")}));
  ASSERT_EQ(task_observer.traces[2],
            (TaskGroupTrace{"Notify" + name, desc, Status::UnknownError("Notify")}));
  ASSERT_EQ(task_observer.traces[3],
            (TaskGroupTrace{name, desc, TaskStatus::Cancelled()}));
}

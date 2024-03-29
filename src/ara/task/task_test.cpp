#include "task.h"
#include "task_group.h"
#include "task_observer.h"
#include "task_status.h"

#include <ara/common/observer.h>
#include <ara/util/util.h>

#include <gtest/gtest.h>

using namespace ara;
using namespace ara::task;

TEST(TaskTest, BasicTask) {
  auto task_impl = [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  };

  Task task("BasicTask", "Do nothing but finish directly", task_impl);
  TaskContext ctx;
  auto res = task(ctx, 0);
  ASSERT_TRUE(res.ok());
  ASSERT_TRUE(res->IsFinished()) << res->ToString();
}

TEST(TaskTest, BasicContinuation) {
  auto cont_impl = [](const TaskContext&) -> TaskResult {
    return TaskStatus::Finished();
  };

  Continuation cont("BasicContinuation", "Do nothing but finish directly", cont_impl);
  TaskContext ctx;
  auto res = cont(ctx);
  ASSERT_TRUE(res.ok());
  ASSERT_TRUE(res->IsFinished()) << res->ToString();
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

template <typename TestTaskObserver>
std::pair<std::unique_ptr<ChainedObserver<TaskObserver>>, TestTaskObserver&>
MakeTestTaskObserver() {
  auto task_observer_ptr = std::make_unique<TestTaskObserver>();
  auto& task_observer = *dynamic_cast<TestTaskObserver*>(task_observer_ptr.get());
  std::vector<std::unique_ptr<TaskObserver>> observers;
  observers.push_back(std::move(task_observer_ptr));
  auto chained_observer =
      std::make_unique<ChainedObserver<TaskObserver>>(std::move(observers));
  return {std::move(chained_observer), task_observer};
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

  auto task_blocked_impl = [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Blocked(nullptr);
  };
  Task task_blocked("TaskBlocked", "Always blocked", task_blocked_impl);
  auto task_blocked_trace = [](TaskId task_id, bool end) -> TaskTrace {
    return {"TaskBlocked", "Always blocked", task_id,
            end ? std::optional<TaskStatus>(TaskStatus::Blocked(nullptr)) : std::nullopt};
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
      traces.emplace_back(TaskTrace{task.Name(), task.Desc(), task_id, std::nullopt});
      return Status::OK();
    }

    Status OnTaskEnd(const Task& task, const TaskContext&, TaskId task_id,
                     const TaskResult& result) override {
      ARA_DCHECK(result.ok());
      traces.emplace_back(TaskTrace{task.Name(), task.Desc(), task_id, *result});
      return Status::OK();
    }

    Status OnContinuationBegin(const Continuation& cont, const TaskContext&) override {
      traces.emplace_back(
          TaskTrace{cont.Name(), cont.Desc(), std::nullopt, std::nullopt});
      return Status::OK();
    }

    Status OnContinuationEnd(const Continuation& cont, const TaskContext&,
                             const TaskResult& result) override {
      ARA_DCHECK(result.ok());
      traces.emplace_back(TaskTrace{cont.Name(), cont.Desc(), std::nullopt, *result});
      return Status::OK();
    }
  };

  TaskContext ctx;
  auto [chained_observer, task_observer] = MakeTestTaskObserver<TestTaskObserver>();
  ctx.task_observer = std::move(chained_observer);

  std::ignore = task_continue(ctx, 0);
  std::ignore = task_continue(ctx, 1);
  std::ignore = task_finished(ctx, 0);
  std::ignore = task_blocked(ctx, 1);
  std::ignore = task_blocked(ctx, 0);
  std::ignore = task_finished(ctx, 1);
  std::ignore = cont_cancelled(ctx);
  std::ignore = cont_yield(ctx);

  ASSERT_EQ(task_observer.traces.size(), 16);
  ASSERT_EQ(task_observer.traces[0], task_continue_trace(0, false));
  ASSERT_EQ(task_observer.traces[1], task_continue_trace(0, true));
  ASSERT_EQ(task_observer.traces[2], task_continue_trace(1, false));
  ASSERT_EQ(task_observer.traces[3], task_continue_trace(1, true));
  ASSERT_EQ(task_observer.traces[4], task_finished_trace(0, false));
  ASSERT_EQ(task_observer.traces[5], task_finished_trace(0, true));
  ASSERT_EQ(task_observer.traces[6], task_blocked_trace(1, false));
  ASSERT_EQ(task_observer.traces[7], task_blocked_trace(1, true));
  ASSERT_EQ(task_observer.traces[8], task_blocked_trace(0, false));
  ASSERT_EQ(task_observer.traces[9], task_blocked_trace(0, true));
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

  TaskContext ctx;
  {
    auto res = task_ref(ctx, 0);
    ASSERT_TRUE(res.ok());
    ASSERT_TRUE(res->IsFinished()) << res->ToString();
  }

  {
    auto res = cont_ref.value()(ctx);
    ASSERT_TRUE(res.ok());
    ASSERT_TRUE(res->IsFinished()) << res->ToString();
  }

  {
    auto status = tg.NotifyFinish(ctx);
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

    Status OnTaskGroupBegin(const TaskGroup& tg, const TaskContext& ctx) {
      traces.emplace_back(
          TaskGroupTrace{tg.Name(), tg.Desc(), Status::UnknownError("Begin")});
      return Status::OK();
    }

    Status OnTaskGroupEnd(const TaskGroup& tg, const TaskContext& ctx,
                          const TaskResult& result) {
      traces.emplace_back(TaskGroupTrace{tg.Name(), tg.Desc(), result});
      return Status::OK();
    }

    Status OnNotifyFinishBegin(const TaskGroup& tg, const TaskContext& ctx) {
      traces.emplace_back(TaskGroupTrace{"Notify" + tg.Name(), tg.Desc(),
                                         Status::UnknownError("NotifyBegin")});
      return Status::OK();
    }

    Status OnNotifyFinishEnd(const TaskGroup& tg, const TaskContext& ctx,
                             const Status& status) {
      traces.emplace_back(TaskGroupTrace{"Notify" + tg.Name(), tg.Desc(), status});
      return Status::OK();
    }
  };

  TaskContext ctx;
  auto [chained_observer, task_observer] = MakeTestTaskObserver<TestTaskObserver>();
  ctx.task_observer = std::move(chained_observer);

  std::ignore = tg.NotifyFinish(ctx);

  ASSERT_EQ(task_observer.traces.size(), 2);
  ASSERT_EQ(task_observer.traces[0],
            (TaskGroupTrace{"Notify" + name, desc, Status::UnknownError("NotifyBegin")}));
  ASSERT_EQ(task_observer.traces[1],
            (TaskGroupTrace{"Notify" + name, desc, Status::UnknownError("Notify")}));
}

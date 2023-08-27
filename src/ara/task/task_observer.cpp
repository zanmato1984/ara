#include "task_observer.h"
#include "task.h"
#include "task_group.h"
#include "task_status.h"

#include <ara/util/util.h>

namespace ara::task {

using util::TaskResultToString;

class TaskLogger : public TaskObserver {
 public:
  Status OnTaskBegin(const Task& task, const TaskContext& context,
                     TaskId task_id) override {
    ARA_LOG(INFO) << "Task " << task.Name() << "(" << task.Desc() << ") " << task_id
                  << " begin";
    return Status::OK();
  }

  Status OnTaskEnd(const Task& task, const TaskContext& context, TaskId task_id,
                   const TaskResult& result) override {
    ARA_LOG(INFO) << "Task " << task.Name() << "(" << task.Desc() << ") " << task_id
                  << " end with " << TaskResultToString(result);
    return Status::OK();
  }

  Status OnContinuationBegin(const Continuation& cont,
                             const TaskContext& context) override {
    ARA_LOG(INFO) << "Continuation " << cont.Name() << "(" << cont.Desc() << ") "
                  << " begin";
    return Status::OK();
  }

  Status OnContinuationEnd(const Continuation& cont, const TaskContext& context,
                           const TaskResult& result) override {
    ARA_LOG(INFO) << "Continuation " << cont.Name() << "(" << cont.Desc() << ") "
                  << " end with " << TaskResultToString(result);
    return Status::OK();
  }

  Status OnNotifyFinishBegin(const TaskGroup& task_group,
                             const TaskContext& context) override {
    ARA_LOG(INFO) << "Notify " << task_group.Name() << "(" << task_group.Desc() << ") "
                  << " finish begin";
    return Status::OK();
  }

  virtual Status OnNotifyFinishEnd(const TaskGroup& task_group,
                                   const TaskContext& context,
                                   const Status& status) override {
    ARA_LOG(INFO) << "Notify " << task_group.Name() << "(" << task_group.Desc() << ") "
                  << " finish end with " << status.ToString();
    return Status::OK();
  }
};

std::unique_ptr<ChainedObserver<TaskObserver>> TaskObserver::Make(const QueryContext&) {
  auto logger = std::make_unique<TaskLogger>();
  std::vector<std::unique_ptr<TaskObserver>> observers;
  observers.push_back(std::move(logger));
  return std::make_unique<ChainedObserver<TaskObserver>>(std::move(observers));
}

}  // namespace ara::task

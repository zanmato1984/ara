#include "task_observer.h"
#include "task.h"
#include "task_status.h"

#include <ara/util/util.h>

namespace ara::task {

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

 private:
  std::string TaskResultToString(const TaskResult& result) {
    if (!result.ok()) {
      return result.status().ToString();
    }
    return result->ToString();
  }
};

std::unique_ptr<TaskObserver> TaskObserver::Make(const QueryContext&) {
  return std::make_unique<TaskLogger>();
}

}  // namespace ara::task

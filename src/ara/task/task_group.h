#pragma once

#include <ara/common/meta.h>
#include <ara/task/task.h>

namespace ara::task {

class TaskGroup : public internal::Meta {
 public:
  using NotifyFinishFunc = std::function<Status(const TaskContext&)>;

  TaskGroup(std::string name, std::string desc, Task task, size_t num_tasks,
            std::optional<Continuation> cont, std::optional<NotifyFinishFunc> notify)
      : Meta(std::move(name), std::move(desc)),
        task_(std::move(task)),
        cont_(std::move(cont)),
        num_tasks_(num_tasks),
        notify_(std::move(notify)) {}

  const Task& GetTask() const { return task_; }

  size_t NumTasks() const { return num_tasks_; }

  const std::optional<Continuation>& GetContinuation() const { return cont_; }

  Status NotifyFinish(const TaskContext&) const;

 private:
  Task task_;
  size_t num_tasks_;
  std::optional<Continuation> cont_;
  std::optional<NotifyFinishFunc> notify_;
};

using TaskGroups = std::vector<TaskGroup>;

}  // namespace ara::task

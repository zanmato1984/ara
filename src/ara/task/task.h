#pragma once

#include <ara/common/defines.h>
#include <ara/task/task_context.h>
#include <ara/task/task_observer.h>
#include <ara/task/task_status.h>

namespace ara::task {

using TaskId = size_t;
using TaskResult = Result<TaskStatus>;

namespace detail {

template <typename... Args>
class Task {
 private:
  using Impl = std::function<TaskResult(Args...)>;

 public:
  Task(Impl impl, std::string name, std::string desc)
      : impl_(std::move(impl)), name_(std::move(name)), desc_(std::move(desc)) {}

  TaskResult operator()(Args... args) const { return impl_(std::forward<Args>(args)...); }

 private:
  Impl impl_;
  std::string name_;
  std::string desc_;

  friend class TaskObserver;
};

}  // namespace detail

using Task = detail::Task<std::function<TaskResult(const TaskContext&, TaskId)>>;
using Cont = detail::Task<std::function<TaskResult(const TaskContext&)>>;

};  // namespace ara::task

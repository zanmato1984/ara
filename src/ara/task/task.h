#pragma once

#include <ara/task/defines.h>
#include <ara/task/task_status.h>

namespace ara::task {

namespace detail {

template <typename... Args>
class Task {
 private:
  using Impl = std::function<TaskResult(const TaskContext&, Args...)>;

 public:
  Task(Impl impl, std::string name, std::string desc)
      : impl_(std::move(impl)), name_(std::move(name)), desc_(std::move(desc)) {}

  TaskResult operator()(const TaskContext &, Args... args) const;
  //   TaskResult operator()(Args... args) const { return
  //   impl_(std::forward<Args>(args)...); }

 private:
  Impl impl_;
  std::string name_;
  std::string desc_;

  friend class TaskObserver;
};

}  // namespace detail

using Task = detail::Task<TaskId>;
using Cont = detail::Task<>;

};  // namespace ara::task

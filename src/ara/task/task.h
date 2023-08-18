#pragma once

#include <ara/task/defines.h>
#include <ara/task/task_context.h>
#include <ara/util/util.h>

namespace ara::task {

namespace detail {

template <typename T>
struct TaskTraits;

template <typename Impl>
class InternalTask {
 public:
  using Signature = typename TaskTraits<Impl>::Signature;
  using ReturnType = typename TaskTraits<Impl>::ReturnType;

  InternalTask(Signature f, std::string name, std::string desc)
      : f_(std::move(f)), name_(std::move(name)), desc_(std::move(desc)) {}

  template <typename... Args>
  ReturnType operator()(const TaskContext& context, Args... args) const {
    auto observer = context.task_observer.get();
    if (observer != nullptr) {
      ARA_RETURN_NOT_OK(
          impl().ObserverBegin(observer, context, std::forward<Args>(args)...));
    }
    auto result = f_(context, std::forward<Args>(args)...);
    if (observer != nullptr) {
      ARA_RETURN_NOT_OK(
          impl().ObserverEnd(observer, context, std::forward<Args>(args)..., result));
    }
    return result;
  }

  std::string GetName() const { return name_; }
  std::string GetDesc() const { return desc_; }

 private:
  friend Impl;
  Impl& impl() { return *static_cast<Impl*>(this); }
  const Impl& impl() const { return *static_cast<const Impl*>(this); }

  Signature f_;
  std::string name_;
  std::string desc_;
};

}  // namespace detail

class Task;
class Continuation;

namespace detail {

template <>
struct TaskTraits<ara::task::Task> {
  using Signature = std::function<TaskResult(const TaskContext&, TaskId)>;
  using ReturnType = TaskResult;
};

template <>
struct TaskTraits<ara::task::Continuation> {
  using Signature = std::function<TaskResult(const TaskContext&)>;
  using ReturnType = TaskResult;
};

}  // namespace detail

class TaskObserver;

class Task : public detail::InternalTask<Task> {
 public:
  using detail::InternalTask<Task>::InternalTask;

  Status ObserverBegin(TaskObserver*, const TaskContext&, TaskId) const;
  Status ObserverEnd(TaskObserver*, const TaskContext&, TaskId, const TaskResult&) const;
};

class Continuation : public detail::InternalTask<Continuation> {
 public:
  using detail::InternalTask<Continuation>::InternalTask;

  Status ObserverBegin(TaskObserver*, const TaskContext&) const;
  Status ObserverEnd(TaskObserver*, const TaskContext&, const TaskResult&) const;
};

};  // namespace ara::task

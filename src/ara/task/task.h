#pragma once

#include <ara/common/meta.h>
#include <ara/task/defines.h>
#include <ara/task/task_context.h>
#include <ara/util/defines.h>

namespace ara::task {

struct TaskHint {
  enum class Type {
    CPU,
    IO,
  } type;
};

namespace internal {

template <typename T>
struct TaskTraits;

template <typename Impl>
class InternalTask : public ara::internal::Meta {
 public:
  using ContextType = typename TaskTraits<Impl>::ContextType;
  using Signature = typename TaskTraits<Impl>::Signature;
  using ReturnType = typename TaskTraits<Impl>::ReturnType;

  InternalTask(std::string name, std::string desc, Signature f, TaskHint hint = {})
      : Meta(std::move(name), std::move(desc)),
        f_(std::move(f)),
        hint_(std::move(hint)) {}

  template <typename... Args>
  ReturnType operator()(const ContextType& context, Args... args) const {
    auto observer = TaskTraits<Impl>::GetObserver(context).get();
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

  const TaskHint& Hint() const { return hint_; }

 private:
  friend Impl;
  Impl& impl() { return *static_cast<Impl*>(this); }
  const Impl& impl() const { return *static_cast<const Impl*>(this); }

  Signature f_;
  TaskHint hint_;
};

}  // namespace internal

class Task;
class Continuation;

namespace internal {

template <>
struct TaskTraits<ara::task::Task> {
  using ContextType = TaskContext;
  using Signature = std::function<TaskResult(const TaskContext&, TaskId)>;
  using ReturnType = TaskResult;
  static const auto& GetObserver(const TaskContext& task_context) {
    return task_context.task_observer;
  }
};

template <>
struct TaskTraits<ara::task::Continuation> {
  using ContextType = TaskContext;
  using Signature = std::function<TaskResult(const TaskContext&)>;
  using ReturnType = TaskResult;
  static const auto& GetObserver(const TaskContext& task_context) {
    return task_context.task_observer;
  }
};

}  // namespace internal

class Task : public internal::InternalTask<Task> {
 public:
  using internal::InternalTask<Task>::InternalTask;

 private:
  Status ObserverBegin(ChainedObserver<TaskObserver>*, const TaskContext&, TaskId) const;
  Status ObserverEnd(ChainedObserver<TaskObserver>*, const TaskContext&, TaskId,
                     const TaskResult&) const;

  friend internal::InternalTask<Task>;
};

class Continuation : public internal::InternalTask<Continuation> {
 public:
  using internal::InternalTask<Continuation>::InternalTask;

 private:
  Status ObserverBegin(ChainedObserver<TaskObserver>*, const TaskContext&) const;
  Status ObserverEnd(ChainedObserver<TaskObserver>*, const TaskContext&,
                     const TaskResult&) const;

  friend internal::InternalTask<Continuation>;
};

};  // namespace ara::task

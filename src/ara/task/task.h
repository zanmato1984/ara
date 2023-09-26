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

namespace detail {

template <typename T>
struct TaskTraits;

template <typename Impl>
class InternalTask : public internal::Meta {
 public:
  using Signature = typename TaskTraits<Impl>::Signature;
  using ReturnType = typename TaskTraits<Impl>::ReturnType;

  InternalTask(std::string name, std::string desc, Signature f, TaskHint hint = {})
      : Meta(std::move(name), std::move(desc)),
        f_(std::move(f)),
        hint_(std::move(hint)) {}

  template <typename... Args>
  ReturnType operator()(const TaskContext& ctx, Args... args) const {
    auto observer = ctx.task_observer.get();
    if (observer != nullptr) {
      ARA_RETURN_NOT_OK(impl().ObserverBegin(observer, ctx, std::forward<Args>(args)...));
    }

    auto result = f_(ctx, std::forward<Args>(args)...);

    if (observer != nullptr) {
      ARA_RETURN_NOT_OK(
          impl().ObserverEnd(observer, ctx, std::forward<Args>(args)..., result));
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

class Task : public detail::InternalTask<Task> {
 public:
  using detail::InternalTask<Task>::InternalTask;

 private:
  Status ObserverBegin(ChainedObserver<TaskObserver>*, const TaskContext&, TaskId) const;
  Status ObserverEnd(ChainedObserver<TaskObserver>*, const TaskContext&, TaskId,
                     const TaskResult&) const;

  friend detail::InternalTask<Task>;
};

class Continuation : public detail::InternalTask<Continuation> {
 public:
  using detail::InternalTask<Continuation>::InternalTask;

 private:
  Status ObserverBegin(ChainedObserver<TaskObserver>*, const TaskContext&) const;
  Status ObserverEnd(ChainedObserver<TaskObserver>*, const TaskContext&,
                     const TaskResult&) const;

  friend detail::InternalTask<Continuation>;
};

};  // namespace ara::task

#include "ara/task/task.h"
#include "ara/task/task_context.h"
#include "ara/task/task_observer.h"

namespace ara::task {

template <typename... Args>
TaskResult detail::Task<Args...>::operator()(const TaskContext& context,
                                             Args... args) const {
  auto observer = context.task_observer.get();
  if (observer != nullptr) {
    ARA_RETURN_NOT_OK(observer->OnTaskBegin(*this, context, std::forward<Args>(args)...));
  }
  auto result = impl_(std::forward<Args>(args)...);
  if (observer != nullptr) {
    ARA_RETURN_NOT_OK(
        observer->OnTaskEnd(*this, context, std::forward<Args>(args)..., result));
  }
  return result;
}

}  // namespace ara::task

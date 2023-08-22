#include <ara/task/task_status.h>

namespace ara::util {

using task::TaskResult;

std::string TaskResultToString(const TaskResult& result) {
  if (!result.ok()) {
    return result.status().ToString();
  }
  return result->ToString();
}

}  // namespace ara::util

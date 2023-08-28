#include <ara/task/task_status.h>
#include <ara/util/util.h>

namespace ara::util {

using pipeline::OpResult;
using task::TaskResult;

std::string TaskResultToString(const TaskResult& result) {
  if (!result.ok()) {
    return result.status().ToString();
  }
  return result->ToString();
}

std::string OpResultToString(const OpResult& result) {
  if (!result.ok()) {
    return result.status().ToString();
  }
  return result->ToString();
}

}  // namespace ara::util

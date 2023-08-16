#include <ara/common/foo.h>

#include "ara/task/task.h"
#include "ara/task/task_context.h"
#include "ara/task/task_status.h"

namespace ara::task {

Batch foo() { return ara::foo(); }

}  // namespace ara::task

#pragma once

#include <ara/common/defines.h>
#include <ara/task/resumer.h>

namespace ara::task {

class Awaiter {
 public:
  virtual ~Awaiter() = default;
};
using AwaiterPtr = std::shared_ptr<Awaiter>;
using SingleAwaiterFactory = std::function<Result<AwaiterPtr>(ResumerPtr&)>;
using AnyAwaiterFactory = std::function<Result<AwaiterPtr>(Resumers&)>;
using AllAwaiterFactory = std::function<Result<AwaiterPtr>(Resumers&)>;

}  // namespace ara::task

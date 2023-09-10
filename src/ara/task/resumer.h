#pragma once

#include <ara/common/defines.h>

namespace ara::task {

class Resumer {
 public:
  virtual ~Resumer() = default;
  virtual void Resume() = 0;
  virtual bool IsResumed() = 0;
};
using ResumerPtr = std::shared_ptr<Resumer>;
using Resumers = std::vector<ResumerPtr>;
using ResumerFactory = std::function<Result<ResumerPtr>()>;

}  // namespace ara::task

#pragma once

#include "ara/common/types.h"

#include <memory>

namespace arrow {
class MemoryPool;
}

namespace ara::driver {
struct Option;
} // namespace ara::driver

namespace ara::execution {

using ara::driver::Option;

struct MemoryResource {
  enum class Mode : int8_t {
    ARENA = 0,
    ARENA_PER_THREAD,
    POOL,
    POOL_PER_THREAD,
    MANAGED,
    CUDA,
  };

  virtual ~MemoryResource() = default;

  using Underlying = arrow::MemoryPool;

  virtual Underlying *preConcatenate(ThreadId thread_id) const = 0;
  virtual Underlying *concatenate() const = 0;
  virtual Underlying *converge() const = 0;

protected:
  virtual void allocatePreConcatenate() = 0;
  virtual void reclaimPreConcatenate() = 0;
  virtual void allocateConcatenate() = 0;
  virtual void reclaimConcatenate() = 0;
  virtual void allocateConverge() = 0;
  virtual void reclaimConverge() = 0;

  friend struct Executor;
};

std::unique_ptr<MemoryResource> createMemoryResource(const Option &option);

} // namespace ara::execution

#pragma once

#include "ara/common/types.h"
#include "ara/driver/option.h"
#include "ara/execution/context.h"
#include "ara/execution/executor.h"

#include <list>

namespace ara::data {
struct Fragment;
} // namespace ara::data

namespace ara::relational {
struct Rel;
} // namespace ara::relational

namespace ara::driver {

using ara::data::Fragment;
using ara::execution::Context;
using ara::execution::Executor;
using ara::relational::Rel;

/// Entry of ARA CPP API.
/// Users can compose their own ARA plans using classes under namespace
/// `ara::relational`, or assemble plans into JSON format string like ARA C
/// API does, and pass them into the corresponding `compile` methods. Data
/// passed around is represented using arrow class `arrow::Array` and the life
/// cycle is fully managed using CPP shared pointer.
class Driver {
public:
  /// Explain APIs.
  std::vector<std::string> explain(const std::string &json, bool extended);
  std::vector<std::string> explain(std::shared_ptr<const Rel> rel,
                                   bool extended);

  /// Compile APIs.
  void compile(const std::string &json);
  void compile(std::shared_ptr<const Rel> rel);

  /// Setting APIs.
  void setMemoryResource(int8_t memory_resouce);
  void setExclusiveDefaultMemoryResource(bool exclusive);
  void setMemoryResourceSize(size_t size);
  void setThreadsPerPipeline(size_t threads);
  void setMemoryResourceSizePerThread(size_t size);
  void setBucketAggregate(bool enable);
  void setBucketAggregateBuckets(size_t buckets);

  /// Execution APIs.
  bool hasNextPipeline() const;
  void preparePipeline();
  bool isPipelineFinal() const;
  void finishPipeline();

  bool pipelineHasNextSource() const;
  SourceId pipelineNextSource();

  void pipelinePush(ThreadId thread_id, SourceId source_id,
                    std::shared_ptr<const Fragment> fragment);
  std::shared_ptr<const Fragment>
  pipelineStream(ThreadId thread_id, SourceId source_id,
                 std::shared_ptr<const Fragment> fragment, size_t rows) const;

private:
  Option option;
  std::unique_ptr<Executor> executor;
};

} // namespace ara::driver

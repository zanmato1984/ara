#pragma once

#include <ara/common/observer.h>

namespace ara {

class QueryContext;

namespace pipeline {

class PipelineObserver;

struct PipelineContext {
  const QueryContext* query_context;
  std::unique_ptr<ChainedObserver<PipelineObserver>> pipeline_observer = nullptr;
};

}  // namespace pipeline

}  // namespace ara

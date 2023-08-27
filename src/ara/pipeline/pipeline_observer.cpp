#include "pipeline_observer.h"

namespace ara::pipeline {

std::unique_ptr<PipelineObserver> PipelineObserver::Make(
    const QueryContext& query_context) {
  return nullptr;
}

}  // namespace ara::pipeline

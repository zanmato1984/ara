#pragma once

#include <ara/common/observer.h>

namespace ara {

class QueryContext;

namespace pipeline {

class PipelineObserver : public Observer {
 public:
  static std::unique_ptr<PipelineObserver> Make(const QueryContext&);
};

}  // namespace pipeline

}  // namespace ara

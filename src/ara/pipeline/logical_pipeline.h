#pragma once

#include <ara/pipeline/op/op.h>

namespace ara::pipeline {

struct LogicalPipeline {
  struct Plex {
    SourceOp* source;
    std::vector<PipeOp*> pipes;
  };

  std::vector<Plex> plexes;
  SinkOp* sink;
};

}  // namespace ara::pipeline

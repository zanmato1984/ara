#pragma once

#include <ara/pipeline/op/op.h>

namespace ara {

class QueryContext;

namespace pipeline {

class LogicalPipeline;

struct PhysicalPipelineStage {
  struct Plex {
    PhysicalSource source;
    std::vector<std::pair<PhysicalPipe, std::optional<PhysicalDrain>>> pipes;
    PhysicalSink sink;
  };

  std::vector<Plex> plexes;
  std::vector<std::unique_ptr<SourceOp>> sources;
};

struct PhysicalPipeline {
  std::vector<PhysicalPipelineStage> stages;
  SinkOp* sink_op;

  static PhysicalPipeline Make(const QueryContext&, const LogicalPipeline&);
};

}  // namespace pipeline

}  // namespace ara

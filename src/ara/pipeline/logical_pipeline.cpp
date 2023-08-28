#include "logical_pipeline.h"

#include <sstream>

namespace ara::pipeline {

std::string LogicalPipeline::Explain(const std::vector<Plex>& plexes,
                                     class SinkOp* sink) {
  std::stringstream ss;
  for (size_t i = 0; i < plexes.size(); ++i) {
    if (i > 0) {
      ss << std::endl;
    }
    ss << "Plex" << i << ": " << plexes[i].source_op->Name() << " -> ";
    for (size_t j = 0; j < plexes[i].pipe_ops.size(); ++j) {
      ss << plexes[i].pipe_ops[j]->Name() << " -> ";
    }
    ss << sink->Name();
  }
  return ss.str();
}

}  // namespace ara::pipeline

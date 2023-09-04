#include "logical_pipeline.h"
#include "op/op.h"

#include <sstream>

namespace ara::pipeline {

std::string LogicalPipeline::Explain(const std::vector<Channel>& channels,
                                     class SinkOp* sink) {
  std::stringstream ss;
  for (size_t i = 0; i < channels.size(); ++i) {
    if (i > 0) {
      ss << std::endl;
    }
    ss << "Channel" << i << ": " << channels[i].source_op->Name() << " -> ";
    for (size_t j = 0; j < channels[i].pipe_ops.size(); ++j) {
      ss << channels[i].pipe_ops[j]->Name() << " -> ";
    }
    ss << sink->Name();
  }
  return ss.str();
}

}  // namespace ara::pipeline

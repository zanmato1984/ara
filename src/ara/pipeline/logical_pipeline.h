#pragma once

#include <ara/common/meta.h>

namespace ara::pipeline {

class SourceOp;
class PipeOp;
class SinkOp;

class LogicalPipeline : public internal::Meta {
 public:
  struct Channel {
    SourceOp* source_op;
    std::vector<PipeOp*> pipe_ops;
  };

  LogicalPipeline(std::string name, std::vector<Channel> channels, SinkOp* sink_op)
      : Meta(std::move(name), Explain(channels, sink_op)),
        channels_(std::move(channels)),
        sink_op_(sink_op) {}

  const std::vector<Channel>& Channels() const { return channels_; }

  SinkOp* SinkOp() const { return sink_op_; }

 private:
  static std::string Explain(const std::vector<Channel>&, class SinkOp*);

 private:
  std::vector<Channel> channels_;
  class SinkOp* sink_op_;
};

}  // namespace ara::pipeline

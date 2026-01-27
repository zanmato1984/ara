#pragma once

#include <ara/common/meta.h>

#include <memory>
#include <string>
#include <vector>

namespace ara::pipeline {

class LogicalPipeline;
class PipelineContext;

class SourceOp;
class PipeOp;
class SinkOp;

class PhysicalPipeline : public internal::Meta {
 public:
  struct Channel {
    SourceOp* source_op;
    std::vector<PipeOp*> pipe_ops;
    SinkOp* sink_op;
  };

  PhysicalPipeline(std::string name, std::vector<Channel> channels,
                   std::vector<std::unique_ptr<SourceOp>> implicit_sources)
      : Meta(std::move(name), Explain(channels)),
        channels_(std::move(channels)),
        implicit_sources_(std::move(implicit_sources)) {}

  const std::vector<Channel>& Channels() const { return channels_; }

  const std::vector<std::unique_ptr<SourceOp>>& ImplicitSources() const {
    return implicit_sources_;
  }

 private:
  static std::string Explain(const std::vector<Channel>&);

 private:
  std::vector<Channel> channels_;
  std::vector<std::unique_ptr<SourceOp>> implicit_sources_;
};

using PhysicalPipelines = std::vector<PhysicalPipeline>;

PhysicalPipelines CompilePipeline(const PipelineContext&, const LogicalPipeline&);

}  // namespace ara::pipeline

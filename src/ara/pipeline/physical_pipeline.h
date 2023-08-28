#pragma once

#include <ara/common/meta.h>
#include <ara/pipeline/op/op.h>

namespace ara::pipeline {

class LogicalPipeline;
class PipelineContext;

class PhysicalPipeline : public internal::Meta {
 public:
  struct Plex {
    SourceOp* source_op;
    std::vector<PipeOp*> pipe_ops;
    SinkOp* sink_op;
  };

  PhysicalPipeline(std::string name, std::vector<Plex> plexes,
                   std::vector<std::unique_ptr<SourceOp>> implicit_sources)
      : Meta(std::move(name), Explain(plexes)),
        plexes_(std::move(plexes)),
        implicit_sources_(std::move(implicit_sources)) {}

  const std::vector<Plex>& Plexes() const { return plexes_; }

  const std::vector<std::unique_ptr<SourceOp>>& ImplicitSources() const {
    return implicit_sources_;
  }

 private:
  static std::string Explain(const std::vector<Plex>&);

 private:
  std::vector<Plex> plexes_;
  std::vector<std::unique_ptr<SourceOp>> implicit_sources_;
};

using PhysicalPipelines = std::vector<PhysicalPipeline>;

PhysicalPipelines CompilePipeline(const PipelineContext&, const LogicalPipeline&);

}  // namespace ara::pipeline

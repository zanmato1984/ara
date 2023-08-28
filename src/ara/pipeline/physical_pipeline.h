#pragma once

#include <ara/common/meta.h>
#include <ara/pipeline/op/op.h>

namespace ara {

class QueryContext;

namespace pipeline {

namespace detail {
class PipelineCompiler;
}  // namespace detail

class LogicalPipeline;

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

 private:
  static std::string Explain(const std::vector<Plex>&);

 private:
  std::vector<Plex> plexes_;
  std::vector<std::unique_ptr<SourceOp>> implicit_sources_;
};

using PhysicalPipelines = std::vector<PhysicalPipeline>;

PhysicalPipelines CompilePipeline(const QueryContext&, const LogicalPipeline&);

}  // namespace pipeline

}  // namespace ara

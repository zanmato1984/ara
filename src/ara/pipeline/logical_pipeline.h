#pragma once

#include <ara/common/meta.h>

namespace ara::pipeline {

class SourceOp;
class PipeOp;
class SinkOp;

class LogicalPipeline : public internal::Meta {
 public:
  struct Plex {
    SourceOp* source_op;
    std::vector<PipeOp*> pipe_ops;
  };

  LogicalPipeline(std::string name, std::vector<Plex> plexes, SinkOp* sink_op)
      : Meta(std::move(name), Explain(plexes, sink_op)),
        plexes_(std::move(plexes)),
        sink_op_(sink_op) {}

  const std::vector<Plex>& Plexes() const { return plexes_; }

  SinkOp* SinkOp() const { return sink_op_; }

 private:
  static std::string Explain(const std::vector<Plex>&, class SinkOp*);

 private:
  std::vector<Plex> plexes_;
  class SinkOp* sink_op_;
};

}  // namespace ara::pipeline

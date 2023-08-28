#include "physical_pipeline.h"
#include "logical_pipeline.h"

#include <map>

namespace ara::pipeline {

std::string PhysicalPipeline::Explain(const std::vector<PhysicalPipeline::Plex>& plexes) {
  return "";
}

namespace {

class PipelineCompiler {
 public:
  PipelineCompiler(const LogicalPipeline& logical_pipeline)
      : logical_pipeline_(logical_pipeline) {}

  PhysicalPipelines Compile(const PipelineContext& context) && {
    ExtractTopology(context);
    SortTopology(context);
    return BuildPhysicalPipelines(context);
  }

 private:
  void ExtractTopology(const PipelineContext&) {
    std::unordered_map<PipeOp*, SourceOp*> pipe_source_map;
    auto sink = logical_pipeline_.SinkOp();
    for (auto& plex : logical_pipeline_.Plexes()) {
      size_t id = 0;
      topology_.emplace(plex.source_op,
                        std::pair<size_t, LogicalPipeline::Plex>{id++, plex});
      for (size_t i = 0; i < plex.pipe_ops.size(); ++i) {
        auto pipe = plex.pipe_ops[i];
        if (pipe_source_map.count(pipe) == 0) {
          if (auto pipe_source_up = pipe->ImplicitSource(); pipe_source_up != nullptr) {
            auto pipe_source = pipe_source_up.get();
            pipe_source_map.emplace(pipe, pipe_source);
            LogicalPipeline::Plex new_plex{
                pipe_source,
                std::vector<PipeOp*>(plex.pipe_ops.begin() + i + 1, plex.pipe_ops.end())};
            topology_.emplace(pipe_source, std::pair<size_t, LogicalPipeline::Plex>{
                                               id++, std::move(new_plex)});
            pipe_sources_keepalive_.emplace(pipe_source, std::move(pipe_source_up));
          }
        } else {
          auto pipe_source = pipe_source_map[pipe];
          if (topology_[pipe_source].first < id) {
            topology_[pipe_source].first = id++;
          }
        }
      }
    }
  }

  void SortTopology(const PipelineContext&) {
    for (auto& [source, physical_info] : topology_) {
      if (pipe_sources_keepalive_.count(source) > 0) {
        physical_pipelines_[physical_info.first].first.push_back(
            std::move(pipe_sources_keepalive_[source]));
      }
      physical_pipelines_[physical_info.first].second.push_back(
          std::move(physical_info.second));
    }
  }

  PhysicalPipelines BuildPhysicalPipelines(const PipelineContext& context) {
    std::vector<PhysicalPipeline> physical_pipelines;
    for (auto& [id, physical_info] : physical_pipelines_) {
      auto sources_keepalive = std::move(physical_info.first);
      auto logical_plexes = std::move(physical_info.second);
      std::vector<PhysicalPipeline::Plex> physical_plexes(logical_plexes.size());
      std::transform(
          logical_plexes.begin(), logical_plexes.end(), physical_plexes.begin(),
          [&](auto& plex) -> PhysicalPipeline::Plex {
            return {plex.source_op, std::move(plex.pipe_ops), logical_pipeline_.SinkOp()};
          });
      auto name =
          "PhysicalPipeline" + std::to_string(id) + "(" + logical_pipeline_.Name() + ")";
      physical_pipelines.emplace_back(std::move(name), std::move(physical_plexes),
                                      std::move(sources_keepalive));
    }
    return physical_pipelines;
  }

  const LogicalPipeline& logical_pipeline_;

  std::unordered_map<SourceOp*, std::pair<size_t, LogicalPipeline::Plex>> topology_;
  std::unordered_map<SourceOp*, std::unique_ptr<SourceOp>> pipe_sources_keepalive_;
  std::map<size_t, std::pair<std::vector<std::unique_ptr<SourceOp>>,
                             std::vector<LogicalPipeline::Plex>>>
      physical_pipelines_;
};

}  // namespace

PhysicalPipelines CompilePipeline(const PipelineContext& context,
                                  const LogicalPipeline& logical_pipeline) {
  return PipelineCompiler(logical_pipeline).Compile(context);
}

}  // namespace ara::pipeline

#include "physical_pipeline.h"
#include "logical_pipeline.h"

#include <map>

namespace ara::pipeline {

namespace {

class PhysicalPipelineBuilder {
 public:
  PhysicalPipelineBuilder(const LogicalPipeline& logical_pipeline)
      : logical_pipeline_(logical_pipeline) {}

  PhysicalPipeline Build(const QueryContext&) && {
    ExtractTopology();
    SortTopology();
    return BuildPhysicalPipeline();
  }

 private:
  void ExtractTopology() {
    std::unordered_map<PipeOp*, SourceOp*> pipe_source_map;
    auto sink = logical_pipeline_.sink;
    for (auto& plex : logical_pipeline_.plexes) {
      size_t stage = 0;
      topology_.emplace(plex.source,
                        std::pair<size_t, LogicalPipeline::Plex>{stage++, plex});
      for (size_t i = 0; i < plex.pipes.size(); ++i) {
        auto pipe = plex.pipes[i];
        if (pipe_source_map.count(pipe) == 0) {
          if (auto pipe_source_up = pipe->ImplicitSource(); pipe_source_up != nullptr) {
            auto pipe_source = pipe_source_up.get();
            pipe_source_map.emplace(pipe, pipe_source);
            LogicalPipeline::Plex new_plex{
                pipe_source,
                std::vector<PipeOp*>(plex.pipes.begin() + i + 1, plex.pipes.end())};
            topology_.emplace(pipe_source, std::pair<size_t, LogicalPipeline::Plex>{
                                               stage++, std::move(new_plex)});
            pipe_sources_keepalive_.emplace(pipe_source, std::move(pipe_source_up));
          }
        } else {
          auto pipe_source = pipe_source_map[pipe];
          if (topology_[pipe_source].first < stage) {
            topology_[pipe_source].first = stage++;
          }
        }
      }
    }
  }

  void SortTopology() {
    for (auto& [source, stage_info] : topology_) {
      if (pipe_sources_keepalive_.count(source) > 0) {
        stages_[stage_info.first].first.push_back(
            std::move(pipe_sources_keepalive_[source]));
      }
      stages_[stage_info.first].second.push_back(std::move(stage_info.second));
    }
  }

  PhysicalPipelineStage::Plex LogicalPlexToPhysicalPlex(const LogicalPipeline::Plex& plex,
                                                        SinkOp* sink) {
    std::vector<std::pair<PhysicalPipe, std::optional<PhysicalDrain>>> pipes(
        plex.pipes.size());
    std::transform(plex.pipes.begin(), plex.pipes.end(), pipes.begin(), [&](auto* pipe) {
      return std::make_pair(pipe->Pipe(), pipe->Drain());
    });
    return {plex.source->Source(), std::move(pipes), sink->Sink()};
  }

  PhysicalPipeline BuildPhysicalPipeline() {
    std::vector<PhysicalPipelineStage> stages;
    for (auto& [stage, stage_info] : stages_) {
      auto sources_keepalive = std::move(stage_info.first);
      auto logical_plexes = std::move(stage_info.second);
      std::vector<PhysicalPipelineStage::Plex> physical_plexes(logical_plexes.size());
      std::transform(logical_plexes.begin(), logical_plexes.end(),
                     physical_plexes.begin(), [&](auto& plex) {
                       return LogicalPlexToPhysicalPlex(plex, logical_pipeline_.sink);
                     });
      stages.push_back({std::move(physical_plexes), std::move(sources_keepalive)});
    }
    return {std::move(stages), logical_pipeline_.sink};
  }

  const LogicalPipeline& logical_pipeline_;

  std::unordered_map<SourceOp*, std::pair<size_t, LogicalPipeline::Plex>> topology_;
  std::unordered_map<SourceOp*, std::unique_ptr<SourceOp>> pipe_sources_keepalive_;
  std::map<size_t, std::pair<std::vector<std::unique_ptr<SourceOp>>,
                             std::vector<LogicalPipeline::Plex>>>
      stages_;
};

}  // namespace

PhysicalPipeline PhysicalPipeline::Make(const QueryContext& query_context,
                                        const LogicalPipeline& logical_pipeline) {
  return PhysicalPipelineBuilder(logical_pipeline).Build(query_context);
}

}  // namespace ara::pipeline

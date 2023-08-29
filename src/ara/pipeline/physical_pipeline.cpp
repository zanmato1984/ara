#include "physical_pipeline.h"
#include "logical_pipeline.h"

#include <map>
#include <sstream>

namespace ara::pipeline {

std::string PhysicalPipeline::Explain(const std::vector<PhysicalPipeline::Plex>& plexes) {
  std::stringstream ss;
  for (size_t i = 0; i < plexes.size(); ++i) {
    if (i > 0) {
      ss << std::endl;
    }
    ss << "Plex" << i << ": " << plexes[i].source_op->Name() << " -> ";
    for (size_t j = 0; j < plexes[i].pipe_ops.size(); ++j) {
      ss << plexes[i].pipe_ops[j]->Name() << " -> ";
    }
    ss << plexes[i].sink_op->Name();
  }
  return ss.str();
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
      sources_keep_order_.push_back(plex.source_op);
      for (size_t i = 0; i < plex.pipe_ops.size(); ++i) {
        auto pipe = plex.pipe_ops[i];
        if (pipe_source_map.count(pipe) == 0) {
          if (auto implicit_source_up = pipe->ImplicitSource();
              implicit_source_up != nullptr) {
            auto implicit_source = implicit_source_up.get();
            pipe_source_map.emplace(pipe, implicit_source);
            LogicalPipeline::Plex new_plex{
                implicit_source,
                std::vector<PipeOp*>(plex.pipe_ops.begin() + i + 1, plex.pipe_ops.end())};
            topology_.emplace(implicit_source, std::pair<size_t, LogicalPipeline::Plex>{
                                                   id++, std::move(new_plex)});
            sources_keep_order_.push_back(implicit_source);
            implicit_sources_keepalive_.emplace(implicit_source,
                                                std::move(implicit_source_up));
          }
        } else {
          auto implicit_source = pipe_source_map[pipe];
          if (topology_[implicit_source].first < id) {
            topology_[implicit_source].first = id++;
          }
        }
      }
    }
  }

  void SortTopology(const PipelineContext&) {
    for (auto& source : sources_keep_order_) {
      auto& physical_info = topology_[source];
      if (implicit_sources_keepalive_.count(source) > 0) {
        physical_pipelines_[physical_info.first].first.push_back(
            std::move(implicit_sources_keepalive_[source]));
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
  std::vector<SourceOp*> sources_keep_order_;
  std::unordered_map<SourceOp*, std::unique_ptr<SourceOp>> implicit_sources_keepalive_;
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

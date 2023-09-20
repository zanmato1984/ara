#include "physical_pipeline.h"
#include "logical_pipeline.h"
#include "op/op.h"

#include <map>
#include <sstream>

namespace ara::pipeline {

std::string PhysicalPipeline::Explain(
    const std::vector<PhysicalPipeline::Channel>& channels) {
  std::stringstream ss;
  for (size_t i = 0; i < channels.size(); ++i) {
    if (i > 0) {
      ss << std::endl;
    }
    ss << "Channel" << i << ": " << channels[i].source_op->Name() << " -> ";
    for (size_t j = 0; j < channels[i].pipe_ops.size(); ++j) {
      ss << channels[i].pipe_ops[j]->Name() << " -> ";
    }
    ss << channels[i].sink_op->Name();
  }
  return ss.str();
}

namespace {

class PipelineCompiler {
 public:
  PipelineCompiler(const LogicalPipeline& logical_pipeline)
      : logical_pipeline_(logical_pipeline) {}

  PhysicalPipelines Compile(const PipelineContext& ctx) && {
    ExtractTopology(ctx);
    SortTopology(ctx);
    return BuildPhysicalPipelines(ctx);
  }

 private:
  void ExtractTopology(const PipelineContext& pipeline_context) {
    std::unordered_map<PipeOp*, SourceOp*> pipe_source_map;
    auto sink = logical_pipeline_.SinkOp();
    for (auto& channel : logical_pipeline_.Channels()) {
      size_t id = 0;
      topology_.emplace(channel.source_op,
                        std::pair<size_t, LogicalPipeline::Channel>{id++, channel});
      sources_keep_order_.push_back(channel.source_op);
      for (size_t i = 0; i < channel.pipe_ops.size(); ++i) {
        auto pipe = channel.pipe_ops[i];
        if (pipe_source_map.count(pipe) == 0) {
          if (auto implicit_source_up = pipe->ImplicitSource(pipeline_context);
              implicit_source_up != nullptr) {
            auto implicit_source = implicit_source_up.get();
            pipe_source_map.emplace(pipe, implicit_source);
            LogicalPipeline::Channel new_channel{
                implicit_source, std::vector<PipeOp*>(channel.pipe_ops.begin() + i + 1,
                                                      channel.pipe_ops.end())};
            topology_.emplace(implicit_source,
                              std::pair<size_t, LogicalPipeline::Channel>{
                                  id++, std::move(new_channel)});
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

  PhysicalPipelines BuildPhysicalPipelines(const PipelineContext& ctx) {
    std::vector<PhysicalPipeline> physical_pipelines;
    for (auto& [id, physical_info] : physical_pipelines_) {
      auto sources_keepalive = std::move(physical_info.first);
      auto logical_channels = std::move(physical_info.second);
      std::vector<PhysicalPipeline::Channel> physical_channels(logical_channels.size());
      std::transform(logical_channels.begin(), logical_channels.end(),
                     physical_channels.begin(),
                     [&](auto& channel) -> PhysicalPipeline::Channel {
                       return {channel.source_op, std::move(channel.pipe_ops),
                               logical_pipeline_.SinkOp()};
                     });
      auto name =
          "PhysicalPipeline" + std::to_string(id) + "(" + logical_pipeline_.Name() + ")";
      physical_pipelines.emplace_back(std::move(name), std::move(physical_channels),
                                      std::move(sources_keepalive));
    }
    return physical_pipelines;
  }

  const LogicalPipeline& logical_pipeline_;

  std::unordered_map<SourceOp*, std::pair<size_t, LogicalPipeline::Channel>> topology_;
  std::vector<SourceOp*> sources_keep_order_;
  std::unordered_map<SourceOp*, std::unique_ptr<SourceOp>> implicit_sources_keepalive_;
  std::map<size_t, std::pair<std::vector<std::unique_ptr<SourceOp>>,
                             std::vector<LogicalPipeline::Channel>>>
      physical_pipelines_;
};

}  // namespace

PhysicalPipelines CompilePipeline(const PipelineContext& ctx,
                                  const LogicalPipeline& logical_pipeline) {
  return PipelineCompiler(logical_pipeline).Compile(ctx);
}

}  // namespace ara::pipeline

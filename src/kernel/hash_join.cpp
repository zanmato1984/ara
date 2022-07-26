#include "ara/kernel/hash_join.h"
#include "ara/data/column_factories.h"
#include "ara/data/column_vector.h"
#include "ara/data/fragment.h"
#include "helper.h"

#include <arrow/compute/api.h>

namespace ara::kernel {

using ara::data::ColumnVector;
using ara::data::createArrowColumnVector;
using ara::type::DataType;
using ara::type::Schema;
using ara::type::TypeId;

// TODO: Rewrite these shit in a mature arrow fashion, i.e., array data visitors
// or so.
namespace detail {

using HashTable = std::unordered_multimap<Key, size_t, RowHash, RowEqual>;

struct HashJoinImpl {
  void build(const Context &ctx,
             std::shared_ptr<const Fragment> build_fragment_,
             const std::vector<ColumnIdx> &build_keys) {
    ARA_ASSERT(!build_fragment, "Re-entering hash join build");

    build_fragment = build_fragment_;
    auto rows = build_fragment->size();
    for (size_t i = 0; i < rows; i++) {
      Key key_values;
      for (auto key : build_keys) {
        const auto &key_col = build_fragment->column(key);
        const auto &value =
            ARA_GET_ARROW_RESULT(key_col->arrow()->GetScalar(i));
        key_values.emplace_back(value);
      }
      hash_table.emplace(std::move(key_values), i);
    }
  }

  std::shared_ptr<Fragment> join(const Context &ctx, ThreadId thread_id,
                                 const Schema &schema, JoinType join_type,
                                 std::shared_ptr<const Fragment> probe_fragment,
                                 const std::vector<ColumnIdx> &probe_keys,
                                 BuildSide build_side) const {
    if (!build_fragment) {
      return nullptr;
    }

    /// Probe hash table and calculate join indices.
    std::vector<std::shared_ptr<const ColumnVector>> key_columns(
        probe_keys.size());
    std::transform(
        probe_keys.begin(), probe_keys.end(), key_columns.begin(),
        [&](const auto &key) { return probe_fragment->column(key); });
    auto indices = probe(ctx, thread_id, key_columns, join_type);

    /// Compose joined table by gathering from both sides and combining them.
    auto rb = [&]() {
      arrow::compute::ExecContext context(
          ctx.memory_resource->preConcatenate(thread_id));
      const auto &probe_datum = ARA_GET_ARROW_RESULT(arrow::compute::Take(
          probe_fragment->arrow(), indices.first,
          arrow::compute::TakeOptions::Defaults(), &context));
      ARA_ASSERT(probe_datum.kind() == arrow::Datum::RECORD_BATCH,
                 "Invalid result for join take");
      const auto &probe_rb = probe_datum.record_batch();
      const auto &build_datum = ARA_GET_ARROW_RESULT(arrow::compute::Take(
          build_fragment->arrow(), indices.second,
          arrow::compute::TakeOptions::Defaults(), &context));
      ARA_ASSERT(build_datum.kind() == arrow::Datum::RECORD_BATCH,
                 "Invalid result for join take");
      const auto &build_rb = build_datum.record_batch();
      arrow::SchemaBuilder builder;
      for (const auto &data_type : schema) {
        auto field = std::make_shared<arrow::Field>("", data_type.arrow(),
                                                    data_type.nullable);
        ARA_ASSERT_ARROW_OK(builder.AddField(field), "Add arrow field failed");
      }
      const auto &arrow_schema = ARA_GET_ARROW_RESULT(builder.Finish());
      std::vector<std::shared_ptr<arrow::Array>> arrow_columns;
      const auto &probe_columns = probe_rb->columns();
      const auto &build_columns = build_rb->columns();
      if (build_side == BuildSide::LEFT) {
        arrow_columns.insert(arrow_columns.end(), build_columns.begin(),
                             build_columns.end());
        arrow_columns.insert(arrow_columns.end(), probe_columns.begin(),
                             probe_columns.end());
      } else {
        arrow_columns.insert(arrow_columns.end(), probe_columns.begin(),
                             probe_columns.end());
        arrow_columns.insert(arrow_columns.end(), build_columns.begin(),
                             build_columns.end());
      }
      return arrow::RecordBatch::Make(arrow_schema, probe_rb->num_rows(),
                                      std::move(arrow_columns));
    }();

    return std::make_shared<Fragment>(rb);
  }

private:
  std::pair<std::shared_ptr<arrow::Array>, std::shared_ptr<arrow::Array>>
  probe(const Context &ctx, ThreadId thread_id,
        const std::vector<std::shared_ptr<const ColumnVector>> &probe_keys,
        JoinType join_type) const {
    auto pool = ctx.memory_resource->preConcatenate(thread_id);
    std::unique_ptr<arrow::ArrayBuilder> probe_builder, build_builder;
    ARA_ASSERT_ARROW_OK(
        arrow::MakeBuilder(pool, arrow::uint64(), &probe_builder),
        "Create probe indices builder failed");
    ARA_ASSERT_ARROW_OK(
        arrow::MakeBuilder(pool, arrow::uint64(), &build_builder),
        "Create build indices builder failed");
    auto probe_indices_builder =
        dynamic_cast<arrow::UInt64Builder *>(probe_builder.get());
    auto build_indices_builder =
        dynamic_cast<arrow::UInt64Builder *>(build_builder.get());
    ARA_ASSERT(probe_indices_builder,
               "Dynamic cast of probe indices builder failed");
    ARA_ASSERT(build_indices_builder,
               "Dynamic cast of build indices builder failed");

    auto rows = probe_keys.front()->size();
    for (size_t i = 0; i < rows; i++) {
      Key key;
      for (const auto &cv : probe_keys) {
        const auto &value = ARA_GET_ARROW_RESULT(cv->arrow()->GetScalar(i));
        key.emplace_back(value);
      }
      auto pair = hash_table.equal_range(key);
      if (pair.first == pair.second && join_type == JoinType::LEFT) {
        ARA_ASSERT_ARROW_OK(probe_indices_builder->Append(i),
                            "Append probe indices failed");
        ARA_ASSERT_ARROW_OK(build_indices_builder->AppendNull(),
                            "Append build indices failed");
      } else {
        while (pair.first != pair.second) {
          ARA_ASSERT_ARROW_OK(probe_indices_builder->Append(i),
                              "Append probe indices failed");
          ARA_ASSERT_ARROW_OK(build_indices_builder->Append(pair.first->second),
                              "Append build indices failed");
          pair.first++;
        }
      }
    }

    std::shared_ptr<arrow::Array> probe_indices, build_indices;
    ARA_ASSERT_ARROW_OK(probe_indices_builder->Finish(&probe_indices),
                        "Finish probe indices failed");
    ARA_ASSERT_ARROW_OK(build_indices_builder->Finish(&build_indices),
                        "Finish build indices failed");

    return std::make_pair(probe_indices, build_indices);
  }

  HashTable hash_table;
  std::shared_ptr<const Fragment> build_fragment;
};

} // namespace detail

HashJoinBuild::HashJoinBuild(KernelId id, Schema schema_,
                             std::vector<ColumnIdx> keys_)
    : NonStreamKernel(id), schema(std::move(schema_)), keys(std::move(keys_)),
      impl(std::make_shared<detail::HashJoinImpl>()) {
  ARA_ASSERT(!keys.empty(), "Empty build keys for HashJoinBuild");
}

void HashJoinBuild::push(const Context &ctx, ThreadId thread_id,
                         KernelId upstream,
                         std::shared_ptr<const Fragment> fragment) const {
  std::lock_guard<std::mutex> lock(push_mutex);
  pushed_fragments.emplace_back(fragment);
}

void HashJoinBuild::concatenate(const Context &ctx) const {
  if (pushed_fragments.empty()) {
    return;
  }

  auto fragments = std::move(pushed_fragments);
  concatenated_fragment = detail::concatFragments(
      ctx.memory_resource->concatenate(), schema, fragments);
}

void HashJoinBuild::converge(const Context &ctx) const {
  if (!concatenated_fragment) {
    return;
  }

  auto concatenated = std::move(concatenated_fragment);

  impl->build(ctx, concatenated, keys);
}

std::string HashJoinProbe::toString() const {
  std::stringstream ss;
  ss << Kernel::toString() << "(type: " << joinTypeToString(join_type)
     << ", build: " << std::to_string(build_kernel_id);
  if (join_type == JoinType::INNER) {
    ss << ", build_side: " << buildSideToString(build_side);
  }
  ss << ", keys: ["
     << std::accumulate(keys.begin() + 1, keys.end(), std::to_string(keys[0]),
                        [](const auto &all, const auto &key) {
                          return all + ", " + std::to_string(key);
                        })
     << "])";
  return ss.str();
}

std::shared_ptr<const Fragment>
HashJoinProbe::streamImpl(const Context &ctx, ThreadId thread_id,
                          KernelId upstream,
                          std::shared_ptr<const Fragment> fragment) const {
  return impl->join(ctx, thread_id, schema, join_type, fragment, keys,
                    build_side);
}

} // namespace ara::kernel

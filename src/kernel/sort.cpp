#include "ara/kernel/sort.h"
#include "ara/data/column_vector.h"
#include "ara/type/data_type.h"
#include "helper.h"

#include <arrow/compute/api.h>
#include <arrow/visitor.h>
#include <map>

namespace ara::kernel {

using ara::relational::SortInfo;

void Sort::push(const Context &ctx, ThreadId thread_id, KernelId upstream,
                std::shared_ptr<const Fragment> fragment) const {
  std::lock_guard<std::mutex> lock(push_mutex);
  pushed_fragments.emplace_back(fragment);
}

namespace detail {

struct SortTypeVisitor : public arrow::TypeVisitor {
  SortTypeVisitor(bool desc_, bool null_last_,
                  std::shared_ptr<arrow::Scalar> left_,
                  std::shared_ptr<arrow::Scalar> right_)
      : desc(desc_), null_last(null_last_), left(left_), right(right_) {}

  template <typename T,
            typename arrow::enable_if_primitive_ctype<T> * = nullptr>
  arrow::Status Visit(const T &type) {
    using ScalarType = typename arrow::TypeTraits<T>::ScalarType;
    auto l = std::dynamic_pointer_cast<ScalarType>(left);
    auto r = std::dynamic_pointer_cast<ScalarType>(right);
    if (!l->is_valid && !r->is_valid) {
      result = 0;
      return arrow::Status::OK();
    }
    if (!l->is_valid) {
      result = null_last ? 1 : -1;
      return arrow::Status::OK();
    }
    if (!r->is_valid) {
      result = null_last ? -1 : 1;
      return arrow::Status::OK();
    }
    result = desc ? r->value - l->value : l->value - r->value;
    return arrow::Status::OK();
  }

  template <typename T,
            std::enable_if_t<std::is_same_v<T, arrow::StringType>> * = nullptr>
  arrow::Status Visit(const T &type) {
    auto l = std::dynamic_pointer_cast<arrow::StringScalar>(left);
    auto r = std::dynamic_pointer_cast<arrow::StringScalar>(right);
    if (!l->is_valid && !r->is_valid) {
      result = 0;
      return arrow::Status::OK();
    }
    if (!l->is_valid) {
      result = null_last ? 1 : -1;
      return arrow::Status::OK();
    }
    if (!r->is_valid) {
      result = null_last ? -1 : 1;
      return arrow::Status::OK();
    }
    result = desc ? std::strcmp(r->value->ToString().data(),
                                l->value->ToString().data())
                  : std::strcmp(l->value->ToString().data(),
                                r->value->ToString().data());
    return arrow::Status::OK();
  }

  template <typename T, typename std::enable_if_t<
                            !arrow::is_primitive_ctype<T>::value &&
                            !std::is_same_v<T, arrow::StringType>> * = nullptr>
  arrow::Status Visit(const T &type) {
    ARA_FAIL("Unsupported arrow scalar");
  }

  bool desc;
  bool null_last;
  std::shared_ptr<arrow::Scalar> left, right;
  int result;
};

struct RowCompare {
  bool operator()(const Key &l, const Key &r) const {
    ARA_ASSERT(l.size() == r.size(), "Mismatched type");
    for (size_t i = 0; i < l.size(); i++) {
      ARA_ASSERT(l[i]->type->Equals(r[i]->type), "Mismatched type");
      SortTypeVisitor visitor(desc[i], null_last[i], l[i], r[i]);
      ARA_ASSERT_ARROW_OK(arrow::VisitTypeInline(*l[i]->type, &visitor),
                          "Compare row failed");
      if (visitor.result < 0) {
        return true;
      } else if (visitor.result > 0) {
        return false;
      }
    }
    return false;
  }

  std::vector<bool> desc;
  std::vector<bool> null_last;
};

using Map = std::multimap<Key, size_t, RowCompare>;

std::shared_ptr<Fragment>
doSort(const Context &ctx, const Schema &schema,
       const std::vector<PhysicalSortInfo> &sort_infos,
       std::shared_ptr<const Fragment> fragment) {
  RowCompare row_cmp{std::vector<bool>(sort_infos.size()),
                     std::vector<bool>(sort_infos.size())};
  std::transform(sort_infos.begin(), sort_infos.end(), row_cmp.desc.begin(),
                 [](const auto &sort_info) {
                   return sort_info.order == SortInfo::Order::DESCENDING;
                 });
  std::vector<bool> null_last(sort_infos.size());
  std::transform(sort_infos.begin(), sort_infos.end(),
                 row_cmp.null_last.begin(), [](const auto &sort_info) {
                   return sort_info.null_order == SortInfo::NullOrder::LAST;
                 });
  detail::Map map(row_cmp);

  /// Sort based on keys and store indices of sorted rows.
  for (size_t row = 0; row < fragment->size(); row++) {
    std::vector<std::shared_ptr<arrow::Scalar>> key(sort_infos.size());
    std::transform(
        sort_infos.begin(), sort_infos.end(), key.begin(),
        [&](const auto &sort_info) {
          return ARA_GET_ARROW_RESULT(
              fragment->column(sort_info.idx)->arrow()->GetScalar(row));
        });
    map.emplace(std::move(key), row);
  }

  /// Gather fragment based on indices.
  std::shared_ptr<arrow::Array> indices;
  auto pool = ctx.memory_resource->converge();
  arrow::compute::ExecContext context(pool);
  {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    ARA_ASSERT_ARROW_OK(arrow::MakeBuilder(pool, arrow::uint64(), &builder),
                        "Create indices builder failed");
    auto indices_builder = dynamic_cast<arrow::UInt64Builder *>(builder.get());
    ARA_ASSERT(indices_builder, "Dynamic cast of indices builder failed");

    for (const auto &entry : map) {
      ARA_ASSERT_ARROW_OK(indices_builder->Append(entry.second),
                          "Append indices failed");
    }

    ARA_ASSERT_ARROW_OK(builder->Finish(&indices), "Finish indices failed");
  }
  const auto &datum = ARA_GET_ARROW_RESULT(
      arrow::compute::Take(fragment->arrow(), indices,
                           arrow::compute::TakeOptions::Defaults(), &context));

  return std::make_shared<Fragment>(datum.record_batch());
}

} // namespace detail

void Sort::concatenate(const Context &ctx) const {
  if (pushed_fragments.empty()) {
    return;
  }

  auto fragments = std::move(pushed_fragments);
  concatenated_fragment = detail::concatFragments(
      ctx.memory_resource->concatenate(), schema, fragments);
}

void Sort::converge(const Context &ctx) const {
  if (!concatenated_fragment) {
    return;
  }

  auto concatenated = std::move(concatenated_fragment);

  converged_fragment = detail::doSort(ctx, schema, sort_infos, concatenated);
}

} // namespace ara::kernel

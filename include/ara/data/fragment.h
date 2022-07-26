#pragma once

#include "ara/expression/expressions.h"

#include <algorithm>

namespace ara::data {

struct ColumnScalar;
struct ColumnVector;

using ara::expression::ColumnIdx;
using ara::type::Schema;

struct Fragment {
  explicit Fragment(std::vector<std::shared_ptr<const Column>> columns_);

  explicit Fragment(std::shared_ptr<arrow::RecordBatch> record_batch);

  size_t size() const;

  size_t numColumns() const { return columns.size(); }

  template <typename T = ColumnVector,
            std::enable_if_t<std::is_same_v<T, ColumnVector> ||
                             std::is_same_v<T, ColumnScalar>> * = nullptr>
  std::shared_ptr<const T> column(ColumnIdx idx) const {
    if (auto cc = std::dynamic_pointer_cast<const T>(columns[idx]); cc) {
      return cc;
    }
    ARA_FAIL("Required concrete column is invalid");
  }

  template <typename T, std::enable_if_t<std::is_same_v<T, Column>> * = nullptr>
  std::shared_ptr<const Column> column(ColumnIdx idx) const {
    return columns[idx];
  }

  auto begin() { return std::begin(columns); }
  auto begin() const { return std::cbegin(columns); }
  auto end() { return std::end(columns); }
  auto end() const { return std::cend(columns); }

  std::shared_ptr<arrow::RecordBatch> arrow() const;

private:
  std::vector<std::shared_ptr<const Column>> columns;
};

} // namespace ara::data

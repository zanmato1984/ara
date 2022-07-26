#pragma once

#include "ara/data/column_vector.h"
#include "ara/execution/context.h"
#include "ara/type/data_type.h"

namespace ara::expression::detail {

using ara::data::Column;
using ara::data::ColumnVector;
using ara::execution::Context;
using ara::type::DataType;

constexpr uint64_t YEAR_BIT_FIELD_OFFSET = 50;
constexpr uint64_t YEAR_BIT_FIELD_WIDTH = 14;
constexpr uint64_t YEAR_BIT_FIELD_MASK = ((1ULL << YEAR_BIT_FIELD_WIDTH) - 1ULL)
                                         << YEAR_BIT_FIELD_OFFSET;

std::shared_ptr<const Column>
extractYear(const Context &ctx, ThreadId thread_id,
            std::shared_ptr<const ColumnVector> cv,
            const DataType &result_type);

} // namespace ara::expression::detail
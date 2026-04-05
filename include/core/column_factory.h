#pragma once

#include <core/column.h>
#include <core/columns/bool_column.h>
#include <core/columns/numeric_column.h>
#include <core/columns/string_column.h>

#include <cstdint>
#include <memory>

namespace columnar::core {
using Int64Column = NumericColumn<int64_t, DataType::Int64>;
using DoubleColumn = NumericColumn<double, DataType::Double>;

std::unique_ptr<Column> MakeColumn(DataType id, bool nullable);
}  // namespace columnar::core

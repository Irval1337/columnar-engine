#pragma once

#include <core/column.h>
#include <core/columns/bool_column.h>
#include <core/columns/char_column.h>
#include <core/columns/numeric_column.h>
#include <core/columns/string_column.h>

#include <memory>

namespace columnar::core {
std::unique_ptr<Column> MakeColumn(DataType type, bool nullable);
}  // namespace columnar::core

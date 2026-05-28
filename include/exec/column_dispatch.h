#pragma once

#include <core/column.h>
#include <core/columns/bool_column.h>
#include <core/columns/char_column.h>
#include <core/columns/date_column.h>
#include <core/columns/numeric_column.h>
#include <core/columns/timestamp_column.h>
#include <core/datatype.h>
#include <util/macro.h>

#include <type_traits>

namespace columnar::exec {
template <typename V>
auto VisitIntegerCol(const core::Column& col, V&& v) {
    switch (col.GetDataType()) {
        case core::DataType::Int16:
            return v(static_cast<const core::Int16Column&>(col));
        case core::DataType::Int32:
            return v(static_cast<const core::Int32Column&>(col));
        case core::DataType::Int64:
            return v(static_cast<const core::Int64Column&>(col));
        case core::DataType::Date:
            return v(static_cast<const core::DateColumn&>(col));
        case core::DataType::Timestamp:
            return v(static_cast<const core::TimestampColumn&>(col));
        case core::DataType::Bool:
            return v(static_cast<const core::BoolColumn&>(col));
        case core::DataType::Char:
            return v(static_cast<const core::CharColumn&>(col));
        default:
            break;
    }
    THROW_RUNTIME_ERROR("Column is not integer-typed");
}

template <typename Col>
size_t TypedColumnSize(const Col& col) {
    if constexpr (std::is_same_v<std::remove_cvref_t<Col>, core::BoolColumn>) {
        return col.Size();
    } else {
        return col.GetData().size();
    }
}

template <typename Col>
auto ReadTypedValue(const Col& col, size_t row) {
    if constexpr (std::is_same_v<std::remove_cvref_t<Col>, core::BoolColumn>) {
        return col.Get(row);
    } else {
        return col.GetData()[row];
    }
}

inline bool HasIntegerValue(core::DataType type) {
    auto physical_type = core::DataTypeToPhysical(type);
    return physical_type == core::PhysicalType::Int16 ||
           physical_type == core::PhysicalType::Int32 ||
           physical_type == core::PhysicalType::Int64 ||
           physical_type == core::PhysicalType::Bool || physical_type == core::PhysicalType::Char;
}
}  // namespace columnar::exec

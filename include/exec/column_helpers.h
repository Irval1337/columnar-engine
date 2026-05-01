#pragma once

#include <core/column.h>
#include <core/columns/bool_column.h>
#include <core/columns/char_column.h>
#include <core/columns/date_column.h>
#include <core/columns/numeric_column.h>
#include <core/columns/timestamp_column.h>
#include <core/datatype.h>
#include <util/macro.h>

#include <cstdint>

namespace columnar::exec {
inline bool HasIntegerValue(core::DataType type) {
    auto physical_type = core::DataTypeToPhysical(type);
    return physical_type == core::PhysicalType::Int16 ||
           physical_type == core::PhysicalType::Int32 ||
           physical_type == core::PhysicalType::Int64 ||
           physical_type == core::PhysicalType::Bool || physical_type == core::PhysicalType::Char;
}

inline void AppendInteger(core::Column& out, int64_t value) {
    switch (out.GetDataType()) {
        case core::DataType::Int16:
            static_cast<core::Int16Column&>(out).Append(static_cast<int16_t>(value));
            return;
        case core::DataType::Int32:
            static_cast<core::Int32Column&>(out).Append(static_cast<int32_t>(value));
            return;
        case core::DataType::Int64:
            static_cast<core::Int64Column&>(out).Append(value);
            return;
        case core::DataType::Date:
            static_cast<core::DateColumn&>(out).Append(static_cast<int32_t>(value));
            return;
        case core::DataType::Timestamp:
            static_cast<core::TimestampColumn&>(out).Append(value);
            return;
        case core::DataType::Bool:
            static_cast<core::BoolColumn&>(out).Append(value != 0);
            return;
        case core::DataType::Char:
            static_cast<core::CharColumn&>(out).Append(static_cast<char>(value));
            return;
        default:
            break;
    }
    THROW_RUNTIME_ERROR("Cannot append integer to output column");
}

inline void AppendDouble(core::Column& out, double value) {
    if (out.GetDataType() != core::DataType::Double) {
        THROW_RUNTIME_ERROR("Cannot append double to output column");
    }
    static_cast<core::DoubleColumn&>(out).Append(value);
}
}  // namespace columnar::exec

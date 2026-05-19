#pragma once

#include <core/column.h>
#include <core/columns/bool_column.h>
#include <core/columns/char_column.h>
#include <core/columns/date_column.h>
#include <core/columns/dictionary_string_column.h>
#include <core/columns/numeric_column.h>
#include <core/columns/string_column.h>
#include <core/columns/timestamp_column.h>
#include <core/datatype.h>
#include <util/macro.h>

#include <cstdint>
#include <string_view>
#include <type_traits>

namespace columnar::exec {
template <typename V>
decltype(auto) VisitIntegerCol(const core::Column& col, V&& v) {
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

inline int64_t ReadIntegerRow(const core::Column& col, size_t row) {
    switch (col.GetDataType()) {
        case core::DataType::Int16:
            return static_cast<const core::Int16Column&>(col).Get(row);
        case core::DataType::Int32:
            return static_cast<const core::Int32Column&>(col).Get(row);
        case core::DataType::Int64:
            return static_cast<const core::Int64Column&>(col).Get(row);
        case core::DataType::Date:
            return static_cast<const core::DateColumn&>(col).Get(row);
        case core::DataType::Timestamp:
            return static_cast<const core::TimestampColumn&>(col).Get(row);
        case core::DataType::Bool:
            return static_cast<const core::BoolColumn&>(col).Get(row) ? 1 : 0;
        case core::DataType::Char:
            return static_cast<const core::CharColumn&>(col).Get(row);
        default:
            break;
    }
    THROW_RUNTIME_ERROR("Cannot read column row as int64");
}

inline double ReadDoubleRow(const core::Column& col, size_t row) {
    if (col.GetDataType() == core::DataType::Double) {
        return static_cast<const core::DoubleColumn&>(col).Get(row);
    }
    return static_cast<double>(ReadIntegerRow(col, row));
}

inline std::string_view ReadStringRow(const core::Column& col, size_t row) {
    if (auto* dict = dynamic_cast<const core::DictionaryStringColumn*>(&col)) {
        return dict->Get(row);
    }
    return static_cast<const core::StringColumn&>(col).Get(row);
}
inline void AppendRow(core::Column& out, const core::Column& src, size_t row) {
    if (src.IsNull(row)) {
        out.AppendNull();
        return;
    }
    auto type = src.GetDataType();
    if (type == core::DataType::String) {
        out.AppendFromString(ReadStringRow(src, row));
        return;
    }
    if (type == core::DataType::Double) {
        AppendDouble(out, ReadDoubleRow(src, row));
        return;
    }
    AppendInteger(out, ReadIntegerRow(src, row));
}
}  // namespace columnar::exec

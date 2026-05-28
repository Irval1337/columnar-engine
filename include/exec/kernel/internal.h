#pragma once

#include <core/column.h>
#include <core/columns/bool_column.h>
#include <core/columns/dictionary_string_column.h>
#include <core/columns/numeric_column.h>
#include <exec/column_dispatch.h>
#include <exec/selection.h>
#include <exec/kernel.h>

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

namespace columnar::exec::kernel {
template <typename V>
auto VisitNumericCol(const core::Column& col, V&& v) {
    if (col.GetDataType() == core::DataType::Double) {
        return v(static_cast<const core::DoubleColumn&>(col));
    }
    return VisitIntegerCol(col, std::forward<V>(v));
}

inline std::unique_ptr<core::BoolColumn> MakeBoolColumn(size_t rows) {
    auto out = std::make_unique<core::BoolColumn>(false);
    out->Reserve(rows);
    return out;
}

template <typename Col, typename F>
void ForEachNonNullCol(const Col& col, const std::vector<uint32_t>* selection, F&& f) {
    const auto& data = col.GetData();
    if (col.IsNullable()) {
        const auto& mask = col.GetNullMask();
        ForSelectedRows(selection, data.size(), [&](size_t i) {
            if (!mask.Get(i)) {
                f(data[i]);
            }
        });
    } else {
        ForSelectedRows(selection, data.size(), [&](size_t i) { f(data[i]); });
    }
}

template <typename F>
void ForEachNonNullCol(const core::BoolColumn& col, const std::vector<uint32_t>* selection, F&& f) {
    if (col.IsNullable()) {
        const auto& mask = col.GetNullMask();
        ForSelectedRows(selection, col.Size(), [&](size_t i) {
            if (!mask.Get(i)) {
                f(col.Get(i));
            }
        });
    } else {
        ForSelectedRows(selection, col.Size(), [&](size_t i) { f(col.Get(i)); });
    }
}
}  // namespace columnar::exec::kernel

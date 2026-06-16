#pragma once

#include <core/column.h>
#include <core/datatype.h>
#include <exec/column_row_access.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace columnar::exec {
template <typename T>
int Compare3(const T& a, const T& b) {
    if (a < b) {
        return -1;
    }
    if (a > b) {
        return 1;
    }
    return 0;
}

inline int CompareRowRefs(const core::Column& col_a, size_t row_a, const core::Column& col_b,
                          size_t row_b) {
    bool a_null = col_a.IsNull(row_a);
    bool b_null = col_b.IsNull(row_b);
    if (a_null != b_null) {
        return a_null ? -1 : 1;
    }
    if (a_null) {
        return 0;
    }
    switch (col_a.GetDataType()) {
        case core::DataType::String:
            return Compare3(ReadStringRow(col_a, row_a), ReadStringRow(col_b, row_b));
        case core::DataType::Double:
            return Compare3(ReadDoubleRow(col_a, row_a), ReadDoubleRow(col_b, row_b));
        default:
            return Compare3(ReadIntegerRow(col_a, row_a), ReadIntegerRow(col_b, row_b));
    }
}

struct SortValue {
    core::DataType type = core::DataType::Int64;
    bool is_null = false;
    int64_t int_value = 0;
    double double_value = 0.0;
    std::string string_value;
};

inline SortValue ReadSortValue(const core::Column& col, size_t row) {
    SortValue value;
    value.type = col.GetDataType();
    value.is_null = col.IsNull(row);
    if (value.is_null) {
        return value;
    }
    switch (value.type) {
        case core::DataType::String:
            value.string_value = std::string(ReadStringRow(col, row));
            break;
        case core::DataType::Double:
            value.double_value = ReadDoubleRow(col, row);
            break;
        default:
            value.int_value = ReadIntegerRow(col, row);
            break;
    }
    return value;
}

inline int CompareSortValues(const SortValue& lhs, const SortValue& rhs) {
    if (lhs.is_null != rhs.is_null) {
        return lhs.is_null ? -1 : 1;
    }
    if (lhs.is_null) {
        return 0;
    }
    switch (lhs.type) {
        case core::DataType::String:
            return Compare3(lhs.string_value, rhs.string_value);
        case core::DataType::Double:
            return Compare3(lhs.double_value, rhs.double_value);
        default:
            return Compare3(lhs.int_value, rhs.int_value);
    }
}

template <typename Ref, typename Less>
void OfferToTopN(std::vector<Ref>& refs, size_t prefix, const Ref& candidate, const Less& less) {
    if (refs.size() < prefix) {
        refs.push_back(candidate);
        std::push_heap(refs.begin(), refs.end(), less);
    } else if (prefix > 0 && less(candidate, refs.front())) {
        std::pop_heap(refs.begin(), refs.end(), less);
        refs.back() = candidate;
        std::push_heap(refs.begin(), refs.end(), less);
    }
}

template <typename Ref, typename Less>
void FinishTopN(std::vector<Ref>& refs, size_t offset, const Less& less) {
    std::sort_heap(refs.begin(), refs.end(), less);
    if (offset >= refs.size()) {
        refs.clear();
    } else if (offset > 0) {
        refs.erase(refs.begin(), refs.begin() + offset);
    }
}
}  // namespace columnar::exec

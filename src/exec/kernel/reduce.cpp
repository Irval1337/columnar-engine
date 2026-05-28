#include <core/column.h>
#include <exec/column_dispatch.h>
#include <exec/column_row_access.h>
#include <exec/selection.h>
#include <exec/kernel.h>
#include <exec/kernel/internal.h>
#include <util/macro.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace columnar::exec::kernel {
namespace {
template <typename Acc, typename Col>
ScalarReduction<Acc> SumImpl(const Col& col, const std::vector<uint32_t>* selection) {
    ScalarReduction<Acc> r;
    ForEachNonNullCol(col, selection, [&](auto v) {
        r.value += static_cast<Acc>(v);
        r.has_value = true;
    });
    return r;
}

template <typename Acc, typename Col, typename Better>
ScalarReduction<Acc> MinMaxImpl(const Col& col, const std::vector<uint32_t>* selection,
                                Better&& better) {
    ScalarReduction<Acc> r;
    ForEachNonNullCol(col, selection, [&](auto v) {
        Acc cv = static_cast<Acc>(v);
        if (!r.has_value || better(cv, r.value)) {
            r.value = cv;
            r.has_value = true;
        }
    });
    return r;
}

template <typename Better>
ScalarReduction<std::string> MinMaxStringImpl(const core::Column& col,
                                              const std::vector<uint32_t>* selection,
                                              Better better) {
    if (col.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("MIN/MAX string: not a string column");
    }
    ScalarReduction<std::string> r;
    ForSelectedRows(selection, col.Size(), [&](size_t i) {
        if (col.IsNull(i)) {
            return;
        }
        auto v = ReadStringRow(col, i);
        if (!r.has_value || better(v, std::string_view(r.value))) {
            r.value.assign(v.data(), v.size());
            r.has_value = true;
        }
    });
    return r;
}
}  // namespace

ScalarReduction<int64_t> SumInt(const core::Column& col, const std::vector<uint32_t>* selection) {
    return VisitIntegerCol(col,
                           [&](const auto& typed) { return SumImpl<int64_t>(typed, selection); });
}

ScalarReduction<long double> SumDouble(const core::Column& col,
                                       const std::vector<uint32_t>* selection) {
    return VisitNumericCol(
        col, [&](const auto& typed) { return SumImpl<long double>(typed, selection); });
}

ScalarReduction<int64_t> MinInt(const core::Column& col, const std::vector<uint32_t>* selection) {
    return VisitIntegerCol(col, [&](const auto& typed) {
        return MinMaxImpl<int64_t>(typed, selection, [](int64_t a, int64_t b) { return a < b; });
    });
}

ScalarReduction<int64_t> MaxInt(const core::Column& col, const std::vector<uint32_t>* selection) {
    return VisitIntegerCol(col, [&](const auto& typed) {
        return MinMaxImpl<int64_t>(typed, selection, [](int64_t a, int64_t b) { return a > b; });
    });
}

ScalarReduction<double> MinDouble(const core::Column& col, const std::vector<uint32_t>* selection) {
    return VisitNumericCol(col, [&](const auto& typed) {
        return MinMaxImpl<double>(typed, selection, [](double a, double b) { return a < b; });
    });
}

ScalarReduction<double> MaxDouble(const core::Column& col, const std::vector<uint32_t>* selection) {
    return VisitNumericCol(col, [&](const auto& typed) {
        return MinMaxImpl<double>(typed, selection, [](double a, double b) { return a > b; });
    });
}

ScalarReduction<std::string> MinString(const core::Column& col,
                                       const std::vector<uint32_t>* selection) {
    return MinMaxStringImpl(col, selection,
                            [](std::string_view a, std::string_view b) { return a < b; });
}

ScalarReduction<std::string> MaxString(const core::Column& col,
                                       const std::vector<uint32_t>* selection) {
    return MinMaxStringImpl(col, selection,
                            [](std::string_view a, std::string_view b) { return a > b; });
}

AvgPartial Avg(const core::Column& col, const std::vector<uint32_t>* selection) {
    return VisitIntegerCol(col, [&](const auto& typed) {
        AvgPartial r;
        ForEachNonNullCol(typed, selection, [&](auto v) {
            r.int_sum += static_cast<__int128>(v);
            ++r.count;
        });
        return r;
    });
}

uint64_t CountNonNull(const core::Column& col, const std::vector<uint32_t>* selection) {
    if (!col.IsNullable()) {
        return selection != nullptr ? selection->size() : col.Size();
    }
    uint64_t c = 0;
    ForSelectedRows(selection, col.Size(), [&](size_t i) {
        if (!col.IsNull(i)) {
            ++c;
        }
    });
    return c;
}

void DistinctInts(const core::Column& col, std::unordered_set<int64_t>& out,
                  const std::vector<uint32_t>* selection) {
    VisitIntegerCol(col, [&](const auto& typed) {
        ForEachNonNullCol(typed, selection, [&](auto v) { out.insert(static_cast<int64_t>(v)); });
    });
}

void DistinctStrings(const core::Column& col, std::unordered_set<std::string>& out,
                     const std::vector<uint32_t>* selection) {
    if (col.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("DistinctStrings: not a string column");
    }
    ForSelectedRows(selection, col.Size(), [&](size_t i) {
        if (!col.IsNull(i)) {
            out.emplace(ReadStringRow(col, i));
        }
    });
}
}  // namespace columnar::exec::kernel

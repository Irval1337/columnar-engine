#pragma once

#include <core/batch.h>
#include <core/column.h>
#include <core/columns/bool_column.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>

namespace columnar::exec::kernel {
std::unique_ptr<core::Column> ConstInt64(int64_t value, size_t rows);
std::unique_ptr<core::Column> ConstString(std::string_view value, size_t rows);

std::unique_ptr<core::Column> Equal(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> NotEqual(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> Less(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> LessOrEqual(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> Greater(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> GreaterOrEqual(const core::Column& lhs, const core::Column& rhs);

std::unique_ptr<core::Column> And(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> Or(const core::Column& lhs, const core::Column& rhs);

std::unique_ptr<core::Column> StrContains(const core::Column& operand, std::string_view substring,
                                          bool negated);

core::Batch ApplyFilter(const core::Batch& batch, const core::BoolColumn& mask);

template <typename T>
struct ScalarReduction {
    bool has_value = false;
    T value{};
};

ScalarReduction<int64_t> SumInt(const core::Column& col);
ScalarReduction<long double> SumDouble(const core::Column& col);

ScalarReduction<int64_t> MinInt(const core::Column& col);
ScalarReduction<int64_t> MaxInt(const core::Column& col);
ScalarReduction<double> MinDouble(const core::Column& col);
ScalarReduction<double> MaxDouble(const core::Column& col);
ScalarReduction<std::string> MinString(const core::Column& col);
ScalarReduction<std::string> MaxString(const core::Column& col);

struct AvgPartial {
    __int128 int_sum = 0;
    long double double_sum = 0;
    uint64_t count = 0;
    bool is_integer = false;
};
AvgPartial Avg(const core::Column& col);

uint64_t CountNonNull(const core::Column& col);

void DistinctInts(const core::Column& col, std::unordered_set<int64_t>& out);
void DistinctStrings(const core::Column& col, std::unordered_set<std::string>& out);
}  // namespace columnar::exec::kernel

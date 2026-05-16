#pragma once

#include <core/batch.h>
#include <core/column.h>
#include <core/columns/bool_column.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace columnar::exec::kernel {
std::unique_ptr<core::Column> ConstInt64(int64_t value, size_t rows);
std::unique_ptr<core::Column> ConstString(std::string_view value, size_t rows);

std::unique_ptr<core::Column> Equal(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> NotEqual(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> Less(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> LessOrEqual(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> Greater(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> GreaterOrEqual(const core::Column& lhs, const core::Column& rhs);

std::unique_ptr<core::Column> EqualConstInt(const core::Column& col, int64_t value);
std::unique_ptr<core::Column> NotEqualConstInt(const core::Column& col, int64_t value);
std::unique_ptr<core::Column> LessConstInt(const core::Column& col, int64_t value);
std::unique_ptr<core::Column> LessOrEqualConstInt(const core::Column& col, int64_t value);
std::unique_ptr<core::Column> GreaterConstInt(const core::Column& col, int64_t value);
std::unique_ptr<core::Column> GreaterOrEqualConstInt(const core::Column& col, int64_t value);

std::unique_ptr<core::Column> EqualConstString(const core::Column& col, std::string_view value);
std::unique_ptr<core::Column> NotEqualConstString(const core::Column& col, std::string_view value);
std::unique_ptr<core::Column> LessConstString(const core::Column& col, std::string_view value);
std::unique_ptr<core::Column> LessOrEqualConstString(const core::Column& col,
                                                     std::string_view value);
std::unique_ptr<core::Column> GreaterConstString(const core::Column& col, std::string_view value);
std::unique_ptr<core::Column> GreaterOrEqualConstString(const core::Column& col,
                                                        std::string_view value);

std::unique_ptr<core::Column> And(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> Or(const core::Column& lhs, const core::Column& rhs);

std::unique_ptr<core::Column> Add(const core::Column& lhs, const core::Column& rhs);
std::unique_ptr<core::Column> Subtract(const core::Column& lhs, const core::Column& rhs);

std::unique_ptr<core::Column> StrContains(const core::Column& operand, std::string_view substring,
                                          bool negated);
std::unique_ptr<core::Column> StrLength(const core::Column& operand);
std::unique_ptr<core::Column> ExtractMinute(const core::Column& operand);
std::unique_ptr<core::Column> TruncMinute(const core::Column& operand);
std::unique_ptr<core::Column> RegexReplace(const core::Column& operand, const std::regex& regex,
                                           const std::string& replacement);

std::unique_ptr<core::Column> CaseSelect(const core::BoolColumn& mask,
                                         const core::Column& when_true,
                                         const core::Column& when_false);

std::vector<uint32_t> MaskToSelection(const core::BoolColumn& mask);

core::Batch Materialize(const core::Batch& batch);

template <typename T>
struct ScalarReduction {
    bool has_value = false;
    T value{};
};

ScalarReduction<int64_t> SumInt(const core::Column& col,
                                const std::vector<uint32_t>* selection = nullptr);
ScalarReduction<long double> SumDouble(const core::Column& col,
                                       const std::vector<uint32_t>* selection = nullptr);

ScalarReduction<int64_t> MinInt(const core::Column& col,
                                const std::vector<uint32_t>* selection = nullptr);
ScalarReduction<int64_t> MaxInt(const core::Column& col,
                                const std::vector<uint32_t>* selection = nullptr);
ScalarReduction<double> MinDouble(const core::Column& col,
                                  const std::vector<uint32_t>* selection = nullptr);
ScalarReduction<double> MaxDouble(const core::Column& col,
                                  const std::vector<uint32_t>* selection = nullptr);
ScalarReduction<std::string> MinString(const core::Column& col,
                                       const std::vector<uint32_t>* selection = nullptr);
ScalarReduction<std::string> MaxString(const core::Column& col,
                                       const std::vector<uint32_t>* selection = nullptr);

struct AvgPartial {
    __int128 int_sum = 0;
    uint64_t count = 0;
};
AvgPartial Avg(const core::Column& col, const std::vector<uint32_t>* selection = nullptr);

uint64_t CountNonNull(const core::Column& col, const std::vector<uint32_t>* selection = nullptr);

void DistinctInts(const core::Column& col, std::unordered_set<int64_t>& out,
                  const std::vector<uint32_t>* selection = nullptr);
void DistinctStrings(const core::Column& col, std::unordered_set<std::string>& out,
                     const std::vector<uint32_t>* selection = nullptr);
}  // namespace columnar::exec::kernel

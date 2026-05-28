#include <exec/aggregation.h>

#include <core/columns/string_column.h>
#include <core/field.h>
#include <exec/column_dispatch.h>
#include <exec/column_row_access.h>
#include <exec/expression/eval.h>
#include <exec/kernel.h>
#include <util/macro.h>

#include <utility>

namespace columnar::exec {
namespace {
bool AggregationCanReturnNull(AggregationType type) {
    return type == AggregationType::Avg || type == AggregationType::Min ||
           type == AggregationType::Max;
}

core::DataType AggregationOutputType(const AggregationUnit& unit) {
    switch (unit.type) {
        case AggregationType::Count:
        case AggregationType::Distinct:
        case AggregationType::Avg:
            return core::DataType::Int64;
        case AggregationType::Sum:
            return GetExpressionType(*unit.expression) == core::DataType::Double
                       ? core::DataType::Double
                       : core::DataType::Int64;
        case AggregationType::Min:
        case AggregationType::Max:
            return GetExpressionType(*unit.expression);
    }
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}

template <typename T, typename Acc>
void AddSumPart(SumState& sum, const kernel::ScalarReduction<T>& part, Acc& acc) {
    if (!part.has_value) {
        return;
    }
    acc += part.value;
    sum.has_value = true;
}

template <typename T>
bool BetterMinMaxValue(bool is_min, const T& candidate, const T& current) {
    return is_min ? candidate < current : candidate > current;
}

template <typename T, typename Slot>
void MergeMinMaxPart(MinMaxState& min_max, bool is_min, const kernel::ScalarReduction<T>& part,
                     Slot& slot) {
    if (!part.has_value) {
        return;
    }
    if (!min_max.has_value || BetterMinMaxValue(is_min, part.value, slot)) {
        slot = part.value;
        min_max.has_value = true;
    }
}

AggregationState MakeAggregationState(const AggregationUnit& unit) {
    switch (unit.type) {
        case AggregationType::Count:
            return CountState{};
        case AggregationType::Sum:
            return SumState{};
        case AggregationType::Avg:
            return AvgState{};
        case AggregationType::Distinct:
            return DistinctState{};
        case AggregationType::Min:
        case AggregationType::Max:
            return MinMaxState{};
    }
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}
}  // namespace

core::Schema MakeAggregationSchema(const std::vector<AggregationUnit>& aggregations) {
    std::vector<core::Field> fields;
    fields.reserve(aggregations.size());
    for (auto& unit : aggregations) {
        fields.emplace_back(unit.name, AggregationOutputType(unit),
                            AggregationCanReturnNull(unit.type));
    }
    return core::Schema(std::move(fields));
}

std::vector<AggregationState> MakeAggregationStates(
    const std::vector<AggregationUnit>& aggregations) {
    std::vector<AggregationState> states;
    states.reserve(aggregations.size());
    for (auto& unit : aggregations) {
        states.push_back(MakeAggregationState(unit));
    }
    return states;
}

void UpdateAggregationState(AggregationState& state, const AggregationUnit& unit,
                            const core::Column& col, const std::vector<uint32_t>* selection) {
    auto type = col.GetDataType();
    switch (unit.type) {
        case AggregationType::Count:
            return;
        case AggregationType::Sum: {
            if (type != core::DataType::Double && !HasIntegerValue(type)) {
                THROW_RUNTIME_ERROR("SUM supports only numeric columns");
            }
            auto& sum = std::get<SumState>(state);
            if (type == core::DataType::Double) {
                AddSumPart(sum, kernel::SumDouble(col, selection), sum.double_value);
            } else {
                AddSumPart(sum, kernel::SumInt(col, selection), sum.int_value);
            }
            return;
        }
        case AggregationType::Avg: {
            if (!HasIntegerValue(type)) {
                THROW_RUNTIME_ERROR("AVG supports only integer-like columns");
            }
            auto part = kernel::Avg(col, selection);
            auto& avg = std::get<AvgState>(state);
            avg.int_sum += part.int_sum;
            avg.count += part.count;
            return;
        }
        case AggregationType::Distinct: {
            auto& distinct = std::get<DistinctState>(state);
            if (type == core::DataType::String) {
                kernel::DistinctStrings(col, distinct.strings, selection);
            } else if (HasIntegerValue(type)) {
                kernel::DistinctInts(col, distinct.ints, selection);
            } else {
                THROW_RUNTIME_ERROR("COUNT DISTINCT supports only integer-like and string columns");
            }
            return;
        }
        case AggregationType::Min:
        case AggregationType::Max: {
            bool is_min = unit.type == AggregationType::Min;
            auto& min_max = std::get<MinMaxState>(state);
            if (type == core::DataType::String) {
                auto part =
                    is_min ? kernel::MinString(col, selection) : kernel::MaxString(col, selection);
                MergeMinMaxPart(min_max, is_min, part, min_max.string_value);
            } else if (type == core::DataType::Double) {
                auto part =
                    is_min ? kernel::MinDouble(col, selection) : kernel::MaxDouble(col, selection);
                MergeMinMaxPart(min_max, is_min, part, min_max.double_value);
            } else if (HasIntegerValue(type)) {
                auto part =
                    is_min ? kernel::MinInt(col, selection) : kernel::MaxInt(col, selection);
                MergeMinMaxPart(min_max, is_min, part, min_max.int_value);
            } else {
                THROW_RUNTIME_ERROR("MIN/MAX does not support this column type");
            }
            return;
        }
    }
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}

void AppendAggregationResult(const AggregationState& state, const AggregationUnit& unit,
                             core::Column& out) {
    switch (unit.type) {
        case AggregationType::Count:
            AppendInteger(out, static_cast<int64_t>(std::get<CountState>(state).value));
            return;
        case AggregationType::Distinct: {
            auto& distinct = std::get<DistinctState>(state);
            AppendInteger(out,
                          static_cast<int64_t>(distinct.ints.size() + distinct.strings.size()));
            return;
        }
        case AggregationType::Sum: {
            auto& sum = std::get<SumState>(state);
            if (!sum.has_value) {
                out.AppendNull();
                return;
            }
            if (out.GetDataType() == core::DataType::Double) {
                AppendDouble(out, static_cast<double>(sum.double_value));
            } else {
                AppendInteger(out, sum.int_value);
            }
            return;
        }
        case AggregationType::Avg: {
            auto& avg = std::get<AvgState>(state);
            if (avg.count == 0) {
                out.AppendNull();
                return;
            }
            AppendInteger(out,
                          static_cast<int64_t>(avg.int_sum / static_cast<__int128>(avg.count)));
            return;
        }
        case AggregationType::Min:
        case AggregationType::Max: {
            auto& min_max = std::get<MinMaxState>(state);
            if (!min_max.has_value) {
                out.AppendNull();
                return;
            }
            if (out.GetDataType() == core::DataType::String) {
                out.AppendFromString(min_max.string_value);
            } else if (out.GetDataType() == core::DataType::Double) {
                AppendDouble(out, min_max.double_value);
            } else {
                AppendInteger(out, min_max.int_value);
            }
            return;
        }
    }
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}
}  // namespace columnar::exec

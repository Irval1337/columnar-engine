#include <exec/aggregation.h>

#include <core/columns/string_column.h>
#include <core/field.h>
#include <exec/column_helpers.h>
#include <exec/kernel.h>
#include <util/macro.h>

#include <utility>

namespace columnar::exec {
namespace {
core::DataType AggregationOutputType(const AggregationUnit& unit) {
    if (unit.type == AggregationType::Count || unit.type == AggregationType::Distinct) {
        return core::DataType::Int64;
    }
    if (unit.type == AggregationType::Sum) {
        return GetExpressionType(*unit.expression) == core::DataType::Double
                   ? core::DataType::Double
                   : core::DataType::Int64;
    }
    if (unit.type == AggregationType::Avg) {
        return core::DataType::Int64;
    }
    if (unit.type == AggregationType::Min || unit.type == AggregationType::Max) {
        return GetExpressionType(*unit.expression);
    }
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}

AggregationState MakeAggregationState(const AggregationUnit& unit) {
    if (unit.type == AggregationType::Count) {
        return CountState{};
    }
    if (unit.type == AggregationType::Sum) {
        return SumState{};
    }
    if (unit.type == AggregationType::Avg) {
        return AvgState{};
    }
    if (unit.type == AggregationType::Distinct) {
        return DistinctState{};
    }
    if (unit.type == AggregationType::Min || unit.type == AggregationType::Max) {
        return MinMaxState{};
    }
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}
}  // namespace

core::Schema MakeAggregationSchema(const std::vector<AggregationUnit>& aggregations) {
    std::vector<core::Field> fields;
    fields.reserve(aggregations.size());
    for (auto& unit : aggregations) {
        bool nullable = unit.type == AggregationType::Sum || unit.type == AggregationType::Avg ||
                        unit.type == AggregationType::Min || unit.type == AggregationType::Max;
        fields.emplace_back(unit.name, AggregationOutputType(unit), nullable);
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
    if (unit.type == AggregationType::Count) {
        return;
    }
    if (unit.type == AggregationType::Sum) {
        if (type != core::DataType::Double && !HasIntegerValue(type)) {
            THROW_RUNTIME_ERROR("SUM supports only numeric columns");
        }
        auto& sum = std::get<SumState>(state);
        if (type == core::DataType::Double) {
            auto part = kernel::SumDouble(col, selection);
            if (part.has_value) {
                sum.double_value += part.value;
                sum.has_value = true;
            }
        } else {
            auto part = kernel::SumInt(col, selection);
            if (part.has_value) {
                sum.int_value += part.value;
                sum.has_value = true;
            }
        }
        return;
    }
    if (unit.type == AggregationType::Avg) {
        if (!HasIntegerValue(type)) {
            THROW_RUNTIME_ERROR("AVG supports only integer-like columns");
        }
        auto part = kernel::Avg(col, selection);
        auto& avg = std::get<AvgState>(state);
        avg.int_sum += part.int_sum;
        avg.count += part.count;
        return;
    }
    if (unit.type == AggregationType::Distinct) {
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
    if (unit.type == AggregationType::Min || unit.type == AggregationType::Max) {
        bool is_min = unit.type == AggregationType::Min;
        auto& min_max = std::get<MinMaxState>(state);
        if (type == core::DataType::String) {
            auto part =
                is_min ? kernel::MinString(col, selection) : kernel::MaxString(col, selection);
            if (part.has_value &&
                (!min_max.has_value || (is_min ? part.value < min_max.string_value
                                               : part.value > min_max.string_value))) {
                min_max.string_value = part.value;
                min_max.has_value = true;
            }
        } else if (type == core::DataType::Double) {
            auto part =
                is_min ? kernel::MinDouble(col, selection) : kernel::MaxDouble(col, selection);
            if (part.has_value &&
                (!min_max.has_value || (is_min ? part.value < min_max.double_value
                                               : part.value > min_max.double_value))) {
                min_max.double_value = part.value;
                min_max.has_value = true;
            }
        } else if (HasIntegerValue(type)) {
            auto part = is_min ? kernel::MinInt(col, selection) : kernel::MaxInt(col, selection);
            if (part.has_value &&
                (!min_max.has_value ||
                 (is_min ? part.value < min_max.int_value : part.value > min_max.int_value))) {
                min_max.int_value = part.value;
                min_max.has_value = true;
            }
        } else {
            THROW_RUNTIME_ERROR("MIN/MAX does not support this column type");
        }
        return;
    }
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}

void AppendAggregationResult(const AggregationState& state, const AggregationUnit& unit,
                             core::Column& out) {
    if (unit.type == AggregationType::Count) {
        AppendInteger(out, static_cast<int64_t>(std::get<CountState>(state).value));
        return;
    }
    if (unit.type == AggregationType::Distinct) {
        auto& distinct = std::get<DistinctState>(state);
        AppendInteger(out, static_cast<int64_t>(distinct.ints.size() + distinct.strings.size()));
        return;
    }
    if (unit.type == AggregationType::Sum) {
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
    if (unit.type == AggregationType::Avg) {
        auto& avg = std::get<AvgState>(state);
        if (avg.count == 0) {
            out.AppendNull();
            return;
        }
        AppendInteger(out, static_cast<int64_t>(avg.int_sum / static_cast<__int128>(avg.count)));
        return;
    }
    if (unit.type == AggregationType::Min || unit.type == AggregationType::Max) {
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
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}
}  // namespace columnar::exec

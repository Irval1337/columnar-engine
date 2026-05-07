#pragma once

#include <core/column.h>
#include <core/schema.h>
#include <exec/expression.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

namespace columnar::exec {
enum class AggregationType {
    Count,
    Sum,
    Avg,
    Distinct,
    Min,
    Max,
};

struct AggregationUnit {
    AggregationType type;
    std::shared_ptr<Expression> expression;
    std::string name;
};

inline AggregationUnit Count(std::string name) {
    return {AggregationType::Count, nullptr, std::move(name)};
}

inline AggregationUnit Sum(std::shared_ptr<Expression> expression, std::string name) {
    return {AggregationType::Sum, std::move(expression), std::move(name)};
}

inline AggregationUnit Avg(std::shared_ptr<Expression> expression, std::string name) {
    return {AggregationType::Avg, std::move(expression), std::move(name)};
}

inline AggregationUnit Distinct(std::shared_ptr<Expression> expression, std::string name) {
    return {AggregationType::Distinct, std::move(expression), std::move(name)};
}

inline AggregationUnit Min(std::shared_ptr<Expression> expression, std::string name) {
    return {AggregationType::Min, std::move(expression), std::move(name)};
}

inline AggregationUnit Max(std::shared_ptr<Expression> expression, std::string name) {
    return {AggregationType::Max, std::move(expression), std::move(name)};
}

struct CountState {
    uint64_t value = 0;
};

struct SumState {
    bool has_value = false;
    int64_t int_value = 0;
    long double double_value = 0;
};

struct AvgState {
    __int128 int_sum = 0;
    uint64_t count = 0;
};

struct MinMaxState {
    bool has_value = false;
    int64_t int_value = 0;
    double double_value = 0;
    std::string string_value;
};

struct DistinctState {
    std::unordered_set<int64_t> ints;
    std::unordered_set<std::string> strings;
};

using AggregationState = std::variant<CountState, SumState, AvgState, MinMaxState, DistinctState>;

core::Schema MakeAggregationSchema(const std::vector<AggregationUnit>& aggregations);
std::vector<AggregationState> MakeAggregationStates(
    const std::vector<AggregationUnit>& aggregations);

void UpdateAggregationState(AggregationState& state, const AggregationUnit& unit,
                            const core::Column& evaluated);

void UpdateAggregationStateRow(AggregationState& state, const AggregationUnit& unit,
                               const core::Column& evaluated, size_t row);

void AppendAggregationResult(const AggregationState& state, const AggregationUnit& unit,
                             core::Column& out);
}  // namespace columnar::exec

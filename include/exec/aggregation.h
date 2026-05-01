#pragma once

#include <exec/expression.h>

#include <memory>
#include <string>
#include <utility>

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
}  // namespace columnar::exec

#pragma once

#include <exec/expression/eval.h>
#include <exec/expression/types.h>

#include <vector>

namespace columnar::exec {
template <typename Unit>
bool RequiresDenseBatch(const std::vector<Unit>& units) {
    for (auto& u : units) {
        if (u.expression != nullptr && !IsTrivialExpression(*u.expression)) {
            return true;
        }
    }
    return false;
}

inline bool IsComparisonFunction(BinaryFunction f) {
    return f == BinaryFunction::Equal || f == BinaryFunction::NotEqual ||
           f == BinaryFunction::Less || f == BinaryFunction::LessOrEqual ||
           f == BinaryFunction::Greater || f == BinaryFunction::GreaterOrEqual;
}

inline bool IsConstExpression(const Expression& expr) {
    return expr.type == ExpressionType::ConstInt64 || expr.type == ExpressionType::ConstString;
}

inline BinaryFunction FlipComparison(BinaryFunction f) {
    switch (f) {
        case BinaryFunction::Less:
            return BinaryFunction::Greater;
        case BinaryFunction::LessOrEqual:
            return BinaryFunction::GreaterOrEqual;
        case BinaryFunction::Greater:
            return BinaryFunction::Less;
        case BinaryFunction::GreaterOrEqual:
            return BinaryFunction::LessOrEqual;
        default:
            return f;
    }
}
}  // namespace columnar::exec

#include <exec/metadata_pruning.h>

#include <bruh/bruh_batch_reader.h>
#include <exec/expression/utils.h>

#include <algorithm>
#include <string_view>

namespace columnar::exec {
namespace {
template <typename T>
bool MinMaxMayMatch(T mn, T mx, T value, BinaryFunction function) {
    switch (function) {
        case BinaryFunction::Equal:
            return mn <= value && value <= mx;
        case BinaryFunction::NotEqual:
            return mn != value || mx != value;
        case BinaryFunction::Less:
            return mn < value;
        case BinaryFunction::LessOrEqual:
            return mn <= value;
        case BinaryFunction::Greater:
            return mx > value;
        case BinaryFunction::GreaterOrEqual:
            return mx >= value;
        default:
            return true;
    }
}

const bruh::ColumnChunkStatistics& ColumnStatistics(bruh::BruhBatchReader& reader, size_t row_group,
                                                    const ColumnExpr& column) {
    auto& metadata = reader.GetMetaData();
    size_t col = metadata.schema.GetIndex(column.name);
    return reader.GetColumnStatistics(row_group, col);
}

bool IntComparisonMayMatch(const bruh::ColumnChunkStatistics& statistics, int64_t value,
                           BinaryFunction function) {
    if (!statistics.present) {
        return true;
    }
    if (!statistics.has_min_max) {
        return false;
    }
    return MinMaxMayMatch(statistics.min_int, statistics.max_int, value, function);
}

bool DoubleComparisonMayMatch(const bruh::ColumnChunkStatistics& statistics, int64_t value,
                              BinaryFunction function) {
    if (!statistics.present) {
        return true;
    }
    if (!statistics.has_min_max) {
        return false;
    }
    return MinMaxMayMatch(statistics.min_double, statistics.max_double, static_cast<double>(value),
                          function);
}

bool StringComparisonMayMatch(const bruh::ColumnChunkStatistics& statistics, std::string_view value,
                              BinaryFunction function) {
    if (!statistics.present) {
        return true;
    }
    if (!statistics.has_min_max) {
        return false;
    }
    return MinMaxMayMatch(std::string_view(statistics.min_string),
                          std::string_view(statistics.max_string), value, function);
}

bool ColumnBoolMayMatch(bruh::BruhBatchReader& reader, size_t row_group, const ColumnExpr& column) {
    if (column.type != core::DataType::Bool) {
        return true;
    }
    auto& statistics = ColumnStatistics(reader, row_group, column);
    if (!statistics.present) {
        return true;
    }
    return statistics.has_min_max && statistics.max_int != 0;
}

bool CompareMayMatch(bruh::BruhBatchReader& reader, size_t row_group, const BinaryExpr& binary) {
    if (!IsComparisonFunction(binary.function)) {
        return true;
    }

    const Expression* column_side = binary.lhs.get();
    const Expression* const_side = binary.rhs.get();
    bool flipped = false;
    if (IsConstExpression(*column_side)) {
        std::swap(column_side, const_side);
        flipped = true;
    }
    if (column_side->type != ExpressionType::Column || !IsConstExpression(*const_side)) {
        return true;
    }

    auto function = flipped ? FlipComparison(binary.function) : binary.function;
    auto& column = static_cast<const ColumnExpr&>(*column_side);
    auto& statistics = ColumnStatistics(reader, row_group, column);

    if (const_side->type == ExpressionType::ConstInt64) {
        int64_t value = static_cast<const ConstInt64&>(*const_side).value;
        if (column.type == core::DataType::Double) {
            return DoubleComparisonMayMatch(statistics, value, function);
        }
        if (column.type == core::DataType::String) {
            return true;
        }
        return IntComparisonMayMatch(statistics, value, function);
    }

    if (const_side->type == ExpressionType::ConstString && column.type == core::DataType::String) {
        auto& value = static_cast<const ConstString&>(*const_side).value;
        return StringComparisonMayMatch(statistics, value, function);
    }
    return true;
}

bool ContainsMayMatch(bruh::BruhBatchReader& reader, size_t row_group,
                      const ContainsExpr& contains) {
    if (contains.expr->type != ExpressionType::Column) {
        return true;
    }
    auto& column = static_cast<const ColumnExpr&>(*contains.expr);
    if (column.type != core::DataType::String) {
        return true;
    }
    auto& statistics = ColumnStatistics(reader, row_group, column);
    if (!statistics.present) {
        return true;
    }
    return statistics.has_min_max;
}
}  // namespace

bool PredicateMayMatch(bruh::BruhBatchReader& reader, size_t row_group, const Expression& expr) {
    auto& metadata = reader.GetMetaData();
    if (metadata.row_groups[row_group].rows_count == 0) {
        return false;
    }
    switch (expr.type) {
        case ExpressionType::Binary: {
            auto& binary = static_cast<const BinaryExpr&>(expr);
            if (binary.function == BinaryFunction::And) {
                return PredicateMayMatch(reader, row_group, *binary.lhs) &&
                       PredicateMayMatch(reader, row_group, *binary.rhs);
            }
            if (binary.function == BinaryFunction::Or) {
                return PredicateMayMatch(reader, row_group, *binary.lhs) ||
                       PredicateMayMatch(reader, row_group, *binary.rhs);
            }
            return CompareMayMatch(reader, row_group, binary);
        }
        case ExpressionType::Column:
            return ColumnBoolMayMatch(reader, row_group, static_cast<const ColumnExpr&>(expr));
        case ExpressionType::Contains:
            return ContainsMayMatch(reader, row_group, static_cast<const ContainsExpr&>(expr));
        default:
            return true;
    }
}
}  // namespace columnar::exec

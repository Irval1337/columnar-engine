#include <exec/expression.h>

#include <core/columns/bool_column.h>
#include <exec/kernel.h>
#include <util/macro.h>

namespace columnar::exec {
namespace {
std::unique_ptr<core::Column> EvalFunction(const core::Column& arg, ScalarFunction function) {
    switch (function) {
        case ScalarFunction::Length:
            return kernel::StrLength(arg);
        case ScalarFunction::ExtractMinute:
            return kernel::ExtractMinute(arg);
        case ScalarFunction::TruncMinute:
            return kernel::TruncMinute(arg);
    }
    THROW_RUNTIME_ERROR("Unsupported scalar function");
}

std::unique_ptr<core::Column> EvalBinary(const core::Column& lhs, const core::Column& rhs,
                                         BinaryFunction function) {
    switch (function) {
        case BinaryFunction::Equal:
            return kernel::Equal(lhs, rhs);
        case BinaryFunction::NotEqual:
            return kernel::NotEqual(lhs, rhs);
        case BinaryFunction::Less:
            return kernel::Less(lhs, rhs);
        case BinaryFunction::LessOrEqual:
            return kernel::LessOrEqual(lhs, rhs);
        case BinaryFunction::Greater:
            return kernel::Greater(lhs, rhs);
        case BinaryFunction::GreaterOrEqual:
            return kernel::GreaterOrEqual(lhs, rhs);
        case BinaryFunction::And:
            return kernel::And(lhs, rhs);
        case BinaryFunction::Or:
            return kernel::Or(lhs, rhs);
        case BinaryFunction::Plus:
            return kernel::Add(lhs, rhs);
        case BinaryFunction::Minus:
            return kernel::Subtract(lhs, rhs);
    }
    THROW_RUNTIME_ERROR("Unsupported binary function");
}
}  // namespace

core::DataType GetExpressionType(const Expression& expr) {
    switch (expr.type) {
        case ExpressionType::ConstInt64:
            return core::DataType::Int64;
        case ExpressionType::ConstString:
            return core::DataType::String;
        case ExpressionType::Column:
            return static_cast<const ColumnExpr&>(expr).type;
        case ExpressionType::Binary:
            return IsArithmeticFunction(static_cast<const BinaryExpr&>(expr).function)
                       ? core::DataType::Int64
                       : core::DataType::Bool;
        case ExpressionType::Contains:
            return core::DataType::Bool;
        case ExpressionType::Function:
            return static_cast<const FunctionExpr&>(expr).function == ScalarFunction::TruncMinute
                       ? core::DataType::Timestamp
                       : core::DataType::Int64;
        case ExpressionType::Case:
            return GetExpressionType(*static_cast<const CaseExpr&>(expr).when_true);
        case ExpressionType::RegexReplace:
            return core::DataType::String;
    }
    THROW_RUNTIME_ERROR("Unsupported expression type");
}

void CollectColumns(const Expression& expr, std::vector<std::string>& columns) {
    switch (expr.type) {
        case ExpressionType::Column:
            AddColumn(columns, static_cast<const ColumnExpr&>(expr).name);
            return;
        case ExpressionType::Binary: {
            auto& binary = static_cast<const BinaryExpr&>(expr);
            CollectColumns(*binary.lhs, columns);
            CollectColumns(*binary.rhs, columns);
            return;
        }
        case ExpressionType::Contains:
            CollectColumns(*static_cast<const ContainsExpr&>(expr).expr, columns);
            return;
        case ExpressionType::Function:
            CollectColumns(*static_cast<const FunctionExpr&>(expr).arg, columns);
            return;
        case ExpressionType::Case: {
            auto& case_expr = static_cast<const CaseExpr&>(expr);
            CollectColumns(*case_expr.cond, columns);
            CollectColumns(*case_expr.when_true, columns);
            CollectColumns(*case_expr.when_false, columns);
            return;
        }
        case ExpressionType::RegexReplace:
            CollectColumns(*static_cast<const RegexReplaceExpr&>(expr).arg, columns);
            return;
        case ExpressionType::ConstInt64:
        case ExpressionType::ConstString:
            return;
    }
    THROW_RUNTIME_ERROR("Unsupported expression type");
}

EvalResult Evaluate(const core::Batch& batch, const Expression& expr) {
    size_t rows = batch.RowsCount();
    switch (expr.type) {
        case ExpressionType::ConstInt64:
            return kernel::ConstInt64(static_cast<const ConstInt64&>(expr).value, rows);
        case ExpressionType::ConstString:
            return kernel::ConstString(static_cast<const ConstString&>(expr).value, rows);
        case ExpressionType::Column: {
            auto& column_expr = static_cast<const ColumnExpr&>(expr);
            return batch.ColumnAt(batch.GetSchema().GetIndex(column_expr.name));
        }
        case ExpressionType::Binary: {
            auto& binary = static_cast<const BinaryExpr&>(expr);
            auto lhs = Evaluate(batch, *binary.lhs);
            auto rhs = Evaluate(batch, *binary.rhs);
            return EvalBinary(lhs.Get(), rhs.Get(), binary.function);
        }
        case ExpressionType::Contains: {
            auto& contains = static_cast<const ContainsExpr&>(expr);
            auto operand = Evaluate(batch, *contains.expr);
            return kernel::StrContains(operand.Get(), contains.substring, contains.negated);
        }
        case ExpressionType::Function: {
            auto& function = static_cast<const FunctionExpr&>(expr);
            auto arg = Evaluate(batch, *function.arg);
            return EvalFunction(arg.Get(), function.function);
        }
        case ExpressionType::Case: {
            auto& case_expr = static_cast<const CaseExpr&>(expr);
            auto cond = Evaluate(batch, *case_expr.cond);
            auto when_true = Evaluate(batch, *case_expr.when_true);
            auto when_false = Evaluate(batch, *case_expr.when_false);
            const auto& mask = static_cast<const core::BoolColumn&>(cond.Get());
            return kernel::CaseSelect(mask, when_true.Get(), when_false.Get());
        }
        case ExpressionType::RegexReplace: {
            auto& regex_replace = static_cast<const RegexReplaceExpr&>(expr);
            auto arg = Evaluate(batch, *regex_replace.arg);
            return kernel::RegexReplace(arg.Get(), regex_replace.regex, regex_replace.replacement);
        }
    }
    THROW_RUNTIME_ERROR("Unsupported expression type");
}

}  // namespace columnar::exec

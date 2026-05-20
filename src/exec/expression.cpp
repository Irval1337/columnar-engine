#include <exec/expression.h>
#include <core/columns/bool_column.h>
#include <core/columns/dictionary_string_column.h>
#include <core/columns/string_column.h>
#include <exec/column_helpers.h>
#include <exec/kernel.h>
#include <util/macro.h>

#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace columnar::exec {
namespace {
struct InIntTerm {
    const core::Column* col = nullptr;
    std::vector<int64_t> values;
};

struct IntCompareTerm {
    const core::Column* col = nullptr;
    int64_t value = 0;
    BinaryFunction function = BinaryFunction::Equal;
};

struct StringCompareTerm {
    const core::Column* col = nullptr;
    const core::DictionaryStringColumn* dict_col = nullptr;
    std::string_view value;
    BinaryFunction function = BinaryFunction::Equal;
    std::vector<uint8_t> dict_matches;
};

struct ContainsTerm {
    const core::Column* col = nullptr;
    const core::DictionaryStringColumn* dict_col = nullptr;
    std::string_view substring;
    bool negated = false;
    std::vector<uint8_t> dict_matches;
};

struct BoolColumnTerm {
    const core::BoolColumn* col = nullptr;
};

using PredicateTerm =
    std::variant<InIntTerm, IntCompareTerm, StringCompareTerm, ContainsTerm, BoolColumnTerm>;

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
        case BinaryFunction::Multiply:
            return kernel::Multiply(lhs, rhs);
    }
    THROW_RUNTIME_ERROR("Unsupported binary function");
}

bool IsComparisonFunction(BinaryFunction f) {
    return f == BinaryFunction::Equal || f == BinaryFunction::NotEqual ||
           f == BinaryFunction::Less || f == BinaryFunction::LessOrEqual ||
           f == BinaryFunction::Greater || f == BinaryFunction::GreaterOrEqual;
}

bool IsConstExpression(const Expression& expr) {
    return expr.type == ExpressionType::ConstInt64 || expr.type == ExpressionType::ConstString;
}

BinaryFunction FlipComparison(BinaryFunction f) {
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

std::unique_ptr<core::Column> EvalIntConstCompare(const core::Column& col, int64_t value,
                                                  BinaryFunction f) {
    switch (f) {
        case BinaryFunction::Equal:
            return kernel::EqualConstInt(col, value);
        case BinaryFunction::NotEqual:
            return kernel::NotEqualConstInt(col, value);
        case BinaryFunction::Less:
            return kernel::LessConstInt(col, value);
        case BinaryFunction::LessOrEqual:
            return kernel::LessOrEqualConstInt(col, value);
        case BinaryFunction::Greater:
            return kernel::GreaterConstInt(col, value);
        case BinaryFunction::GreaterOrEqual:
            return kernel::GreaterOrEqualConstInt(col, value);
        default:
            THROW_RUNTIME_ERROR("EvalIntConstCompare: not a comparison");
    }
}

std::unique_ptr<core::Column> EvalStringConstCompare(const core::Column& col,
                                                     std::string_view value, BinaryFunction f) {
    switch (f) {
        case BinaryFunction::Equal:
            return kernel::EqualConstString(col, value);
        case BinaryFunction::NotEqual:
            return kernel::NotEqualConstString(col, value);
        case BinaryFunction::Less:
            return kernel::LessConstString(col, value);
        case BinaryFunction::LessOrEqual:
            return kernel::LessOrEqualConstString(col, value);
        case BinaryFunction::Greater:
            return kernel::GreaterConstString(col, value);
        case BinaryFunction::GreaterOrEqual:
            return kernel::GreaterOrEqualConstString(col, value);
        default:
            THROW_RUNTIME_ERROR("EvalStringConstCompare: not a comparison");
    }
}

std::unique_ptr<core::Column> TryEvalConstCompare(const core::Batch& batch,
                                                  const BinaryExpr& binary) {
    if (!IsComparisonFunction(binary.function)) {
        return nullptr;
    }
    const Expression* col_side = binary.lhs.get();
    const Expression* const_side = binary.rhs.get();
    bool flipped = false;
    if (IsConstExpression(*col_side)) {
        std::swap(col_side, const_side);
        flipped = true;
    }
    if (!IsConstExpression(*const_side) || IsConstExpression(*col_side)) {
        return nullptr;
    }
    auto op = flipped ? FlipComparison(binary.function) : binary.function;
    auto col_eval = Evaluate(batch, *col_side);
    if (const_side->type == ExpressionType::ConstInt64) {
        if (col_eval.Get().GetDataType() == core::DataType::String) {
            return nullptr;
        }
        return EvalIntConstCompare(col_eval.Get(),
                                   static_cast<const ConstInt64&>(*const_side).value, op);
    }
    return EvalStringConstCompare(col_eval.Get(),
                                  static_cast<const ConstString&>(*const_side).value, op);
}

const core::Column& ResolveColumn(const core::Batch& batch, const ColumnExpr& expr) {
    return batch.ColumnAt(batch.GetSchema().GetIndex(expr.name));
}

template <typename T>
bool CompareValues(T lhs, T rhs, BinaryFunction function) {
    switch (function) {
        case BinaryFunction::Equal:
            return lhs == rhs;
        case BinaryFunction::NotEqual:
            return lhs != rhs;
        case BinaryFunction::Less:
            return lhs < rhs;
        case BinaryFunction::LessOrEqual:
            return lhs <= rhs;
        case BinaryFunction::Greater:
            return lhs > rhs;
        case BinaryFunction::GreaterOrEqual:
            return lhs >= rhs;
        default:
            THROW_RUNTIME_ERROR("CompareValues: not a comparison");
    }
}

void PrepareDictionaryTerm(StringCompareTerm& term) {
    term.dict_col = dynamic_cast<const core::DictionaryStringColumn*>(term.col);
    if (term.dict_col == nullptr) {
        return;
    }
    term.dict_matches.resize(term.dict_col->DictSize());
    for (uint32_t id = 0; id < term.dict_matches.size(); ++id) {
        term.dict_matches[id] =
            CompareValues(term.dict_col->DictValue(id), term.value, term.function) ? 1 : 0;
    }
}

void PrepareDictionaryTerm(ContainsTerm& term) {
    term.dict_col = dynamic_cast<const core::DictionaryStringColumn*>(term.col);
    if (term.dict_col == nullptr) {
        return;
    }
    term.dict_matches.resize(term.dict_col->DictSize());
    for (uint32_t id = 0; id < term.dict_matches.size(); ++id) {
        bool found = term.dict_col->DictValue(id).find(term.substring) != std::string_view::npos;
        term.dict_matches[id] = (found != term.negated) ? 1 : 0;
    }
}

bool TryCollectInInts(const Expression& expr, const ColumnExpr*& column,
                      std::vector<int64_t>& values) {
    if (expr.type != ExpressionType::Binary) {
        return false;
    }
    auto& binary = static_cast<const BinaryExpr&>(expr);
    if (binary.function == BinaryFunction::Or) {
        return TryCollectInInts(*binary.lhs, column, values) &&
               TryCollectInInts(*binary.rhs, column, values);
    }
    if (binary.function != BinaryFunction::Equal) {
        return false;
    }
    const Expression* lhs = binary.lhs.get();
    const Expression* rhs = binary.rhs.get();
    if (lhs->type == ExpressionType::ConstInt64) {
        std::swap(lhs, rhs);
    }
    if (lhs->type != ExpressionType::Column || rhs->type != ExpressionType::ConstInt64) {
        return false;
    }
    auto& candidate = static_cast<const ColumnExpr&>(*lhs);
    if (candidate.type == core::DataType::String || candidate.type == core::DataType::Double ||
        candidate.type == core::DataType::Bool) {
        return false;
    }
    if (column != nullptr && column->name != candidate.name) {
        return false;
    }
    column = &candidate;
    values.push_back(static_cast<const ConstInt64&>(*rhs).value);
    return true;
}

bool TryCompileSimpleTerm(const core::Batch& batch, const Expression& expr,
                          std::vector<PredicateTerm>& terms) {
    if (expr.type == ExpressionType::Column) {
        const auto& column_expr = static_cast<const ColumnExpr&>(expr);
        if (column_expr.type != core::DataType::Bool) {
            return false;
        }
        terms.push_back(BoolColumnTerm{
            &static_cast<const core::BoolColumn&>(ResolveColumn(batch, column_expr))});
        return true;
    }

    if (expr.type == ExpressionType::Contains) {
        const auto& contains = static_cast<const ContainsExpr&>(expr);
        if (contains.expr->type != ExpressionType::Column) {
            return false;
        }
        const auto& column_expr = static_cast<const ColumnExpr&>(*contains.expr);
        if (column_expr.type != core::DataType::String) {
            return false;
        }
        ContainsTerm term{
            &ResolveColumn(batch, column_expr), nullptr, contains.substring, contains.negated, {}};
        PrepareDictionaryTerm(term);
        terms.push_back(std::move(term));
        return true;
    }

    if (expr.type != ExpressionType::Binary) {
        return false;
    }
    const auto& binary = static_cast<const BinaryExpr&>(expr);
    if (!IsComparisonFunction(binary.function)) {
        return false;
    }

    const Expression* col_side = binary.lhs.get();
    const Expression* const_side = binary.rhs.get();
    bool flipped = false;
    if (IsConstExpression(*col_side)) {
        std::swap(col_side, const_side);
        flipped = true;
    }
    if (col_side->type != ExpressionType::Column) {
        return false;
    }
    const auto& column_expr = static_cast<const ColumnExpr&>(*col_side);
    auto function = flipped ? FlipComparison(binary.function) : binary.function;
    if (const_side->type == ExpressionType::ConstInt64) {
        if (column_expr.type == core::DataType::String ||
            column_expr.type == core::DataType::Double) {
            return false;
        }
        terms.push_back(IntCompareTerm{&ResolveColumn(batch, column_expr),
                                       static_cast<const ConstInt64&>(*const_side).value,
                                       function});
        return true;
    }
    if (const_side->type == ExpressionType::ConstString &&
        column_expr.type == core::DataType::String) {
        StringCompareTerm term{&ResolveColumn(batch, column_expr),
                               nullptr,
                               static_cast<const ConstString&>(*const_side).value,
                               function,
                               {}};
        PrepareDictionaryTerm(term);
        terms.push_back(std::move(term));
        return true;
    }
    return false;
}

bool TryCompileTerms(const core::Batch& batch, const Expression& expr,
                     std::vector<PredicateTerm>& terms) {
    if (expr.type == ExpressionType::Binary) {
        auto& binary = static_cast<const BinaryExpr&>(expr);
        if (binary.function == BinaryFunction::And) {
            return TryCompileTerms(batch, *binary.lhs, terms) &&
                   TryCompileTerms(batch, *binary.rhs, terms);
        }
        const ColumnExpr* in_column = nullptr;
        std::vector<int64_t> in_values;
        if (TryCollectInInts(expr, in_column, in_values) && in_values.size() > 1) {
            terms.push_back(InIntTerm{&ResolveColumn(batch, *in_column), std::move(in_values)});
            return true;
        }
    }
    if (TryCompileSimpleTerm(batch, expr, terms)) {
        return true;
    }
    return false;
}

bool MatchesTerm(const PredicateTerm& term, size_t row) {
    return std::visit(
        [&](const auto& typed) {
            using T = std::decay_t<decltype(typed)>;
            if constexpr (std::is_same_v<T, InIntTerm>) {
                if (typed.col->IsNull(row)) {
                    return false;
                }
                int64_t value = ReadIntegerRow(*typed.col, row);
                for (int64_t candidate : typed.values) {
                    if (value == candidate) {
                        return true;
                    }
                }
                return false;
            } else if constexpr (std::is_same_v<T, IntCompareTerm>) {
                return !typed.col->IsNull(row) &&
                       CompareValues(ReadIntegerRow(*typed.col, row), typed.value, typed.function);
            } else if constexpr (std::is_same_v<T, StringCompareTerm>) {
                if (typed.col->IsNull(row)) {
                    return false;
                }
                if (typed.dict_col != nullptr) {
                    return typed.dict_matches[typed.dict_col->GetId(row)] != 0;
                }
                return CompareValues(ReadStringRow(*typed.col, row), typed.value, typed.function);
            } else if constexpr (std::is_same_v<T, ContainsTerm>) {
                if (typed.col->IsNull(row)) {
                    return false;
                }
                if (typed.dict_col != nullptr) {
                    return typed.dict_matches[typed.dict_col->GetId(row)] != 0;
                }
                bool found =
                    ReadStringRow(*typed.col, row).find(typed.substring) != std::string_view::npos;
                return typed.negated ? !found : found;
            } else if constexpr (std::is_same_v<T, BoolColumnTerm>) {
                return !typed.col->IsNull(row) && typed.col->Get(row);
            } else {
                return false;
            }
        },
        term);
}

std::vector<uint32_t> SelectionFromMask(const core::Batch& batch, const core::BoolColumn& mask) {
    std::vector<uint32_t> selection;
    selection.reserve(batch.SelectedRowsCount());
    const std::vector<uint32_t>* input = batch.HasSelection() ? &batch.Selection() : nullptr;
    ForSelectedRows(input, batch.RowsCount(), [&](size_t row) {
        if (!mask.IsNull(row) && mask.Get(row)) {
            selection.push_back(static_cast<uint32_t>(row));
        }
    });
    return selection;
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
        case ExpressionType::PrefixCapture:
            return core::DataType::String;
    }
    THROW_RUNTIME_ERROR("Unsupported expression type");
}

bool IsTrivialExpression(const Expression& expr) {
    return expr.type == ExpressionType::Column || expr.type == ExpressionType::ConstInt64 ||
           expr.type == ExpressionType::ConstString;
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
        case ExpressionType::PrefixCapture:
            CollectColumns(*static_cast<const PrefixCaptureExpr&>(expr).arg, columns);
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
            if (auto fast = TryEvalConstCompare(batch, binary)) {
                return EvalResult(std::move(fast));
            }
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
        case ExpressionType::PrefixCapture: {
            auto& prefix_capture = static_cast<const PrefixCaptureExpr&>(expr);
            auto arg = Evaluate(batch, *prefix_capture.arg);
            return kernel::PrefixCapture(arg.Get(), prefix_capture.prefixes,
                                         prefix_capture.delimiter, prefix_capture.require_non_empty,
                                         prefix_capture.single_line_tail);
        }
    }
    THROW_RUNTIME_ERROR("Unsupported expression type");
}

std::vector<uint32_t> EvaluatePredicateSelection(const core::Batch& batch, const Expression& expr) {
    std::vector<PredicateTerm> terms;
    if (!TryCompileTerms(batch, expr, terms) ||
        (terms.size() == 1 && !std::holds_alternative<InIntTerm>(terms.front()))) {
        auto result = Evaluate(batch, expr);
        if (result.Get().GetDataType() != core::DataType::Bool) {
            THROW_RUNTIME_ERROR("Filter condition must produce a boolean column");
        }
        return SelectionFromMask(batch, static_cast<const core::BoolColumn&>(result.Get()));
    }

    std::vector<uint32_t> selection;
    if (batch.HasSelection()) {
        selection = batch.Selection();
    } else {
        size_t rows = batch.RowsCount();
        selection.resize(rows);
        for (size_t row = 0; row < rows; ++row) {
            selection[row] = static_cast<uint32_t>(row);
        }
    }

    size_t out = 0;
    for (uint32_t row : selection) {
        bool matched = true;
        for (auto& term : terms) {
            if (!MatchesTerm(term, row)) {
                matched = false;
                break;
            }
        }
        if (matched) {
            selection[out++] = row;
        }
    }
    selection.resize(out);
    return selection;
}
}  // namespace columnar::exec

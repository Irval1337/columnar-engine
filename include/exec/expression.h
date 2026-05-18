#pragma once

#include <core/batch.h>
#include <core/column.h>
#include <core/datatype.h>
#include <re2/re2.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace columnar::exec {
enum class ExpressionType {
    ConstInt64,
    ConstString,
    Column,
    Binary,
    Contains,
    Function,
    Case,
    RegexReplace,
    PrefixCapture,
};

struct Expression {
    explicit Expression(ExpressionType t) : type(t) {
    }

    virtual ~Expression() = default;

    ExpressionType type;
};

struct ConstInt64 final : public Expression {
    explicit ConstInt64(int64_t v) : Expression(ExpressionType::ConstInt64), value(v) {
    }

    int64_t value;
};

struct ConstString final : public Expression {
    explicit ConstString(std::string v)
        : Expression(ExpressionType::ConstString), value(std::move(v)) {
    }

    std::string value;
};

struct ColumnExpr final : public Expression {
    ColumnExpr(std::string n, core::DataType t)
        : Expression(ExpressionType::Column), name(std::move(n)), type(t) {
    }

    std::string name;
    core::DataType type;
};

enum class BinaryFunction {
    And,
    Or,
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Plus,
    Minus,
    Multiply,
};

inline bool IsArithmeticFunction(BinaryFunction function) {
    return function == BinaryFunction::Plus || function == BinaryFunction::Minus ||
           function == BinaryFunction::Multiply;
}

struct BinaryExpr final : public Expression {
    BinaryExpr(BinaryFunction f, std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
        : Expression(ExpressionType::Binary), function(f), lhs(std::move(l)), rhs(std::move(r)) {
    }

    BinaryFunction function;
    std::shared_ptr<Expression> lhs;
    std::shared_ptr<Expression> rhs;
};

struct ContainsExpr final : public Expression {
    ContainsExpr(std::shared_ptr<Expression> e, std::string s, bool n = false)
        : Expression(ExpressionType::Contains),
          expr(std::move(e)),
          substring(std::move(s)),
          negated(n) {
    }

    std::shared_ptr<Expression> expr;
    std::string substring;
    bool negated = false;
};

inline std::shared_ptr<ConstInt64> MakeConst(int64_t value) {
    return std::make_shared<ConstInt64>(value);
}

inline std::shared_ptr<ConstString> MakeConst(std::string value) {
    return std::make_shared<ConstString>(std::move(value));
}

inline std::shared_ptr<ColumnExpr> MakeColumnExpr(std::string name, core::DataType type) {
    return std::make_shared<ColumnExpr>(std::move(name), type);
}

inline std::shared_ptr<BinaryExpr> MakeBinary(BinaryFunction function,
                                              std::shared_ptr<Expression> lhs,
                                              std::shared_ptr<Expression> rhs) {
    return std::make_shared<BinaryExpr>(function, std::move(lhs), std::move(rhs));
}

inline std::shared_ptr<ContainsExpr> MakeContains(std::shared_ptr<Expression> expr,
                                                  std::string substring, bool negated = false) {
    return std::make_shared<ContainsExpr>(std::move(expr), std::move(substring), negated);
}

enum class ScalarFunction {
    Length,         // length(string) -> Int64
    ExtractMinute,  // extract(minute FROM timestamp) -> Int64
    TruncMinute,    // date_trunc('minute', timestamp) -> Timestamp
};

struct FunctionExpr final : public Expression {
    FunctionExpr(ScalarFunction f, std::shared_ptr<Expression> a)
        : Expression(ExpressionType::Function), function(f), arg(std::move(a)) {
    }

    ScalarFunction function;
    std::shared_ptr<Expression> arg;
};

struct CaseExpr final : public Expression {
    CaseExpr(std::shared_ptr<Expression> c, std::shared_ptr<Expression> t,
             std::shared_ptr<Expression> f)
        : Expression(ExpressionType::Case),
          cond(std::move(c)),
          when_true(std::move(t)),
          when_false(std::move(f)) {
    }

    std::shared_ptr<Expression> cond;
    std::shared_ptr<Expression> when_true;
    std::shared_ptr<Expression> when_false;
};

struct RegexReplaceExpr final : public Expression {
    RegexReplaceExpr(std::shared_ptr<Expression> a, const std::string& pattern,
                     std::string replacement)
        : Expression(ExpressionType::RegexReplace),
          arg(std::move(a)),
          regex(pattern),
          replacement(std::move(replacement)) {
        if (!regex.ok()) {
            throw std::runtime_error("Invalid regex pattern: " + regex.error());
        }
        std::string rewrite_error;
        if (!regex.CheckRewriteString(this->replacement, &rewrite_error)) {
            throw std::runtime_error("Invalid regex replacement: " + rewrite_error);
        }
    }

    std::shared_ptr<Expression> arg;
    RE2 regex;
    std::string replacement;
};

struct PrefixCaptureExpr final : public Expression {
    PrefixCaptureExpr(std::shared_ptr<Expression> a, std::vector<std::string> p, char d,
                      bool require_non_empty = true, bool single_line_tail = true)
        : Expression(ExpressionType::PrefixCapture),
          arg(std::move(a)),
          prefixes(std::move(p)),
          delimiter(d),
          require_non_empty(require_non_empty),
          single_line_tail(single_line_tail) {
        if (prefixes.empty()) {
            throw std::runtime_error("PrefixCapture requires at least one prefix");
        }
        if (delimiter == '\0') {
            throw std::runtime_error("PrefixCapture delimiter must not be NUL");
        }
    }

    std::shared_ptr<Expression> arg;
    std::vector<std::string> prefixes;
    char delimiter = '\0';
    bool require_non_empty = true;
    bool single_line_tail = true;
};

inline std::shared_ptr<FunctionExpr> MakeFunction(ScalarFunction function,
                                                  std::shared_ptr<Expression> arg) {
    return std::make_shared<FunctionExpr>(function, std::move(arg));
}

inline std::shared_ptr<CaseExpr> MakeCase(std::shared_ptr<Expression> cond,
                                          std::shared_ptr<Expression> when_true,
                                          std::shared_ptr<Expression> when_false) {
    return std::make_shared<CaseExpr>(std::move(cond), std::move(when_true), std::move(when_false));
}

inline std::shared_ptr<RegexReplaceExpr> MakeRegexReplace(std::shared_ptr<Expression> arg,
                                                          const std::string& pattern,
                                                          const std::string& replacement) {
    return std::make_shared<RegexReplaceExpr>(std::move(arg), pattern, replacement);
}

inline std::shared_ptr<PrefixCaptureExpr> MakePrefixCapture(std::shared_ptr<Expression> arg,
                                                            std::vector<std::string> prefixes,
                                                            char delimiter,
                                                            bool require_non_empty = true,
                                                            bool single_line_tail = true) {
    return std::make_shared<PrefixCaptureExpr>(std::move(arg), std::move(prefixes), delimiter,
                                               require_non_empty, single_line_tail);
}

class EvalResult {
public:
    EvalResult(const core::Column& borrowed) : ptr_(&borrowed) {
    }

    EvalResult(std::unique_ptr<core::Column> owned) : ptr_(owned.get()), owned_(std::move(owned)) {
    }

    const core::Column& Get() const {
        return *ptr_;
    }

private:
    const core::Column* ptr_ = nullptr;
    std::unique_ptr<core::Column> owned_;
};

inline void AddColumn(std::vector<std::string>& columns, const std::string& name) {
    for (auto& column : columns) {
        if (column == name) {
            return;
        }
    }
    columns.push_back(name);
}

core::DataType GetExpressionType(const Expression& expr);

bool IsTrivialExpression(const Expression& expr);

void CollectColumns(const Expression& expr, std::vector<std::string>& columns);

EvalResult Evaluate(const core::Batch& batch, const Expression& expr);

std::vector<uint32_t> EvaluatePredicateSelection(const core::Batch& batch, const Expression& expr);
}  // namespace columnar::exec

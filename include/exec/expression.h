#pragma once

#include <core/batch.h>
#include <core/column.h>
#include <core/datatype.h>

#include <cstdint>
#include <memory>
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
};

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

void CollectColumns(const Expression& expr, std::vector<std::string>& columns);

EvalResult Evaluate(const core::Batch& batch, const Expression& expr);
}  // namespace columnar::exec

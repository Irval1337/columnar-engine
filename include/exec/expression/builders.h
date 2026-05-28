#pragma once

#include <exec/expression/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace columnar::exec {
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
}  // namespace columnar::exec

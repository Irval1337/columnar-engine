#pragma once

#include <core/batch.h>
#include <core/column.h>
#include <core/datatype.h>
#include <exec/expression/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace columnar::exec {
class EvalResult {
public:
    explicit EvalResult(const core::Column& borrowed) : ptr_(&borrowed) {
    }

    explicit EvalResult(std::unique_ptr<core::Column> owned)
        : ptr_(owned.get()), owned_(std::move(owned)) {
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

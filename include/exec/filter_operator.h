#pragma once

#include <exec/operator.h>

#include <memory>
#include <utility>

namespace columnar::exec {
class FilterSink final : public IOperator {
public:
    FilterSink(IOperator& downstream, std::shared_ptr<Expression> condition)
        : downstream_(downstream), condition_(std::move(condition)) {
    }

    void Consume(core::Batch batch) override;

    void Finalize() override {
        downstream_.Finalize();
    }

private:
    IOperator& downstream_;
    std::shared_ptr<Expression> condition_;
};
}  // namespace columnar::exec

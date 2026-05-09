#pragma once

#include <core/batch.h>
#include <exec/operator.h>

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

namespace columnar::exec {
class TopNSink final : public IOperator {
public:
    TopNSink(IOperator& downstream, std::vector<SortUnit> sort_units, std::optional<size_t> limit);

    void Consume(core::Batch batch) override;

    void Finalize() override;

private:
    IOperator& downstream_;
    std::vector<SortUnit> sort_units_;
    std::optional<size_t> limit_;
    std::vector<core::Batch> buffer_;
};
}  // namespace columnar::exec

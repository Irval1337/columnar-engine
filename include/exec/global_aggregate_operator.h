#pragma once

#include <core/schema.h>
#include <exec/aggregation.h>
#include <exec/operator.h>

#include <vector>

namespace columnar::exec {
class GlobalAggregationSink final : public IOperator {
public:
    GlobalAggregationSink(IOperator& downstream, std::vector<AggregationUnit> aggregations);

    void Consume(core::Batch batch) override;

    void Finalize() override;

private:
    IOperator& downstream_;
    core::Schema output_schema_;
    std::vector<AggregationUnit> aggregations_;
    std::vector<AggregationState> states_;
    bool needs_dense_;
};
}  // namespace columnar::exec

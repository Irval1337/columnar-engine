#pragma once

#include <core/column.h>
#include <core/schema.h>
#include <exec/aggregation.h>
#include <exec/operator.h>

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

namespace columnar::exec {
core::Schema MakeGlobalAggregateSchema(const std::vector<AggregationUnit>& aggregations);

class GlobalAggregationSink final : public IOperator {
public:
    GlobalAggregationSink(IOperator& downstream, std::vector<AggregationUnit> aggregations);

    void Consume(core::Batch batch) override;

    void Finalize() override;

private:
    struct State {
        uint64_t count = 0;
        bool has_value = false;
        int64_t int_sum = 0;
        __int128 big_int_sum = 0;
        long double double_sum = 0;
        int64_t int_value = 0;
        double double_value = 0;
        std::string string_value;
        std::unordered_set<int64_t> ints;
        std::unordered_set<std::string> strings;
    };

    void UpdateState(const core::Batch& batch, size_t unit_index);
    void AppendResult(size_t unit_index, core::Column& out) const;

    IOperator& downstream_;
    core::Schema output_schema_;
    std::vector<AggregationUnit> aggregations_;
    std::vector<State> states_;
};
}  // namespace columnar::exec

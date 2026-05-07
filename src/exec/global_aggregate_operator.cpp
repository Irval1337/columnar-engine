#include <exec/global_aggregate_operator.h>

#include <exec/expression.h>

#include <utility>

namespace columnar::exec {
GlobalAggregationSink::GlobalAggregationSink(IOperator& downstream,
                                             std::vector<AggregationUnit> aggregations)
    : downstream_(downstream),
      output_schema_(MakeAggregationSchema(aggregations)),
      aggregations_(std::move(aggregations)),
      states_(MakeAggregationStates(aggregations_)) {
}

void GlobalAggregationSink::Consume(core::Batch batch) {
    for (size_t i = 0; i < aggregations_.size(); ++i) {
        auto& unit = aggregations_[i];
        auto& state = states_[i];
        if (unit.type == AggregationType::Count) {
            std::get<CountState>(state).value += batch.RowsCount();
            continue;
        }
        auto eval = Evaluate(batch, *unit.expression);
        UpdateAggregationState(state, unit, eval.Get());
    }
}

void GlobalAggregationSink::Finalize() {
    core::Batch result(output_schema_, 1);
    for (size_t i = 0; i < aggregations_.size(); ++i) {
        AppendAggregationResult(states_[i], aggregations_[i], result.ColumnAt(i));
    }
    downstream_.Consume(std::move(result));
    downstream_.Finalize();
}
}  // namespace columnar::exec

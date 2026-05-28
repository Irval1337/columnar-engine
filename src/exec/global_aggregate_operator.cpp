#include <exec/global_aggregate_operator.h>

#include <exec/expression/eval.h>
#include <exec/expression/utils.h>
#include <exec/kernel.h>

#include <utility>
#include <vector>

namespace columnar::exec {
GlobalAggregationSink::GlobalAggregationSink(IOperator& downstream,
                                             std::vector<AggregationUnit> aggregations)
    : downstream_(downstream),
      output_schema_(MakeAggregationSchema(aggregations)),
      aggregations_(std::move(aggregations)),
      states_(MakeAggregationStates(aggregations_)),
      needs_dense_(RequiresDenseBatch(aggregations_)) {
}

void GlobalAggregationSink::Consume(core::Batch batch) {
    if (batch.HasSelection() && needs_dense_) {
        batch = kernel::Materialize(batch);
    }
    const std::vector<uint32_t>* selection = batch.HasSelection() ? &batch.Selection() : nullptr;
    for (size_t i = 0; i < aggregations_.size(); ++i) {
        auto& unit = aggregations_[i];
        auto& state = states_[i];
        if (unit.type == AggregationType::Count) {
            std::get<CountState>(state).value += batch.SelectedRowsCount();
            continue;
        }
        auto eval = Evaluate(batch, *unit.expression);
        UpdateAggregationState(state, unit, eval.Get(), selection);
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

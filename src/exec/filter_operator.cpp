#include <exec/filter_operator.h>

#include <exec/expression/eval.h>

namespace columnar::exec {
void FilterSink::Consume(core::Batch batch) {
    if (batch.RowsCount() == 0) {
        return;
    }
    auto selection = EvaluatePredicateSelection(batch, *condition_);
    size_t selected = selection.size();
    if (selected == 0) {
        return;
    }
    if (!batch.HasSelection() && selected == batch.RowsCount()) {
        downstream_.Consume(std::move(batch));
        return;
    }

    batch.SetSelection(std::move(selection));
    downstream_.Consume(std::move(batch));
}
}  // namespace columnar::exec

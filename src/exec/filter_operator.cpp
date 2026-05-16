#include <exec/filter_operator.h>

#include <core/columns/bool_column.h>
#include <exec/expression.h>
#include <exec/kernel.h>
#include <util/macro.h>

namespace columnar::exec {
void FilterSink::Consume(core::Batch batch) {
    if (batch.RowsCount() == 0) {
        return;
    }
    if (batch.HasSelection()) {
        batch = kernel::Materialize(batch);
    }
    size_t rows = batch.RowsCount();

    auto result = Evaluate(batch, *condition_);
    if (result.Get().GetDataType() != core::DataType::Bool) {
        THROW_RUNTIME_ERROR("Filter condition must produce a boolean column");
    }
    auto& mask = static_cast<const core::BoolColumn&>(result.Get());

    size_t selected = mask.GetData().PopCount();
    if (selected == 0) {
        return;
    }
    if (selected == rows) {
        downstream_.Consume(std::move(batch));
        return;
    }

    batch.SetSelection(kernel::MaskToSelection(mask));
    downstream_.Consume(std::move(batch));
}
}  // namespace columnar::exec

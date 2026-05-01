#include <exec/filter_operator.h>

#include <core/columns/bool_column.h>
#include <exec/expression.h>
#include <exec/kernel.h>
#include <util/macro.h>

namespace columnar::exec {
void FilterSink::Consume(core::Batch batch) {
    size_t rows = batch.RowsCount();
    if (rows == 0) {
        return;
    }

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

    downstream_.Consume(kernel::ApplyFilter(batch, mask));
}
}  // namespace columnar::exec

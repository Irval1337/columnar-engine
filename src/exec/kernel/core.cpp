#include <exec/kernel.h>

#include <core/column_factory.h>
#include <core/columns/bool_column.h>
#include <core/columns/numeric_column.h>
#include <exec/column_row_access.h>
#include <util/macro.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace columnar::exec::kernel {
std::unique_ptr<core::Column> ConstInt64(int64_t value, size_t rows) {
    auto out = std::make_unique<core::Int64Column>(false);
    out->Reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        out->Append(value);
    }
    return out;
}

std::unique_ptr<core::Column> CaseSelect(const core::BoolColumn& mask,
                                         const core::Column& when_true,
                                         const core::Column& when_false) {
    size_t rows = mask.Size();
    if (when_true.Size() != rows || when_false.Size() != rows) {
        THROW_RUNTIME_ERROR("CASE: row count mismatch");
    }
    auto out = core::MakeColumn(when_true.GetDataType(),
                                when_true.IsNullable() || when_false.IsNullable());
    for (size_t i = 0; i < rows; ++i) {
        bool take_true = !mask.IsNull(i) && mask.Get(i);
        AppendRow(*out, take_true ? when_true : when_false, i);
    }
    return out;
}

std::vector<uint32_t> MaskToSelection(const core::BoolColumn& mask) {
    std::vector<uint32_t> selection;
    selection.reserve(mask.GetData().PopCount());
    size_t rows = mask.Size();
    for (size_t i = 0; i < rows; ++i) {
        if (mask.Get(i)) {
            selection.push_back(static_cast<uint32_t>(i));
        }
    }
    return selection;
}

core::Batch Materialize(const core::Batch& batch) {
    if (!batch.HasSelection()) {
        THROW_RUNTIME_ERROR("Materialize: batch has no selection");
    }
    const auto& selection = batch.Selection();
    core::Batch out(batch.GetSchema(), selection.size());
    for (size_t col = 0; col < batch.ColumnsCount(); ++col) {
        const auto& src = batch.ColumnAt(col);
        auto& dst = out.ColumnAt(col);
        for (uint32_t row : selection) {
            AppendRow(dst, src, row);
        }
    }
    return out;
}
}  // namespace columnar::exec::kernel

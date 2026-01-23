#include <csv/csv_batch_reader.h>

namespace columnar::csv {
std::optional<core::Batch> CSVBatchReader::ReadNext() {
    if (row_reader_.IsFinished()) {
        return std::nullopt;
    }

    core::Batch batch(schema_, options_.batch_rows_size);
    std::size_t rows = 0;
    while (rows < options_.batch_rows_size) {
        auto row = row_reader_.ReadRow();
        if (!row) {
            break;
        }
        if (row->size() != schema_.FieldsCount()) {
            THROW_RUNTIME_ERROR("Row has incorrect number of fields");
        }

        for (std::size_t i = 0; i < row->size(); ++i) {
            auto& col = batch.ColumnAt(i);
            auto& field = (*row)[i];
            if (field.value.empty() && !field.was_quoted && col.IsNullable()) {
                col.AppendNull();
            } else {
                col.AppendFromString(field.value);
            }
        }
        ++rows;
    }

    if (rows == 0) {
        return std::nullopt;
    }
    return batch;
}
}  // namespace columnar::csv

#include <csv/csv_batch_reader.h>

namespace columnar::csv {
std::optional<core::Batch> CSVBatchReader::ReadNext() {
    if (row_reader_.IsFinished()) {
        return std::nullopt;
    }

    auto fields_count = schema_.FieldsCount();
    core::Batch batch(schema_, options_.batch_rows_size);
    size_t rows = 0;
    while (rows < options_.batch_rows_size) {
        auto* row = row_reader_.ReadRowView();
        if (!row) {
            break;
        }
        if (row->size() != fields_count) {
            THROW_RUNTIME_ERROR("Row has incorrect number of fields: expected " +
                                std::to_string(fields_count) + ", got " +
                                std::to_string(row->size()));
        }

        for (size_t i = 0; i < fields_count; ++i) {
            const auto& field = (*row)[i];
            auto& col = batch.ColumnAt(i);
            if (field.empty() && !field.was_quoted && col.IsNullable()) {
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

#pragma once

#include <core/batch_reader.h>
#include <core/schema.h>
#include <csv/csv_options.h>
#include <csv/csv_row_reader.h>
#include <util/macro.h>

#include <fstream>
#include <optional>

namespace columnar::csv {
class CSVBatchReader : public core::BatchReader {
public:
    CSVBatchReader(std::istream& is, core::Schema schema, CSVOptions options)
        : row_reader_(is, options), schema_(std::move(schema)), options_(options) {
    }

    std::optional<core::Batch> ReadNext() override {
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

    const core::Schema& GetSchema() const override {
        return schema_;
    }

private:
    CSVRowReader row_reader_;
    core::Schema schema_;
    CSVOptions options_;
};
}  // namespace columnar::csv

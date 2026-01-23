#pragma once

#include <core/batch_reader.h>
#include <core/schema.h>
#include <csv/csv_options.h>
#include <csv/csv_row_reader.h>
#include <util/macro.h>

#include <fstream>
#include <optional>

namespace columnar::csv {
class CSVBatchReader final : public core::BatchReader {
public:
    CSVBatchReader(std::istream& is, core::Schema schema, CSVOptions options)
        : row_reader_(is, options), schema_(std::move(schema)), options_(options) {
    }

    std::optional<core::Batch> ReadNext() override;

    const core::Schema& GetSchema() const override {
        return schema_;
    }

private:
    CSVRowReader row_reader_;
    core::Schema schema_;
    CSVOptions options_;
};
}  // namespace columnar::csv

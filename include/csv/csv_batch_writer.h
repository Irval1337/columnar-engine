#pragma once

#include <core/batch_writer.h>
#include <csv/csv_options.h>
#include <util/macro.h>

#include <string>
#include <vector>

namespace columnar::csv {
class CSVBatchWriter final : public core::BatchWriter {
public:
    CSVBatchWriter(std::ostream& os, CSVOptions options) : os_(os), options_(options) {
    }

    void Write(const core::Batch& batch) override;

    void Flush() override {
        os_.flush();
    }

private:
    // Helps to skip virtual calls and other checks during the cell loop
    struct ColumnView {
        const core::Column* column;
        bool is_string;
        bool nullable;
    };

    void WriteHeader(const core::Schema& schema);
    void AppendField(std::string_view value);
    void FlushLine();

    std::ostream& os_;
    CSVOptions options_;
    bool header_written_ = false;
    std::string line_buf_;
    std::vector<ColumnView> col_views_;
};
}  // namespace columnar::csv

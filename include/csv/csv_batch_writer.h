#pragma once

#include <core/batch_writer.h>
#include <csv/csv_options.h>
#include <util/macro.h>

#include <fstream>
#include <string>

namespace columnar::csv {
class CSVBatchWriter : public core::BatchWriter {
public:
    CSVBatchWriter(std::ostream& os, CSVOptions options) : os_(os), options_(options) {
    }

    void Write(const core::Batch& batch) override {
        if (options_.has_header && !header_written_) {
            WriteHeader(batch.GetSchema());
            header_written_ = true;
        }

        for (std::size_t row = 0; row < batch.RowsCount(); ++row) {
            for (std::size_t col = 0; col < batch.ColumnsCount(); ++col) {
                if (col > 0) {
                    os_ << options_.delimiter;
                }
                if (!batch.ColumnAt(col).IsNull(row)) {
                    std::string value = batch.ColumnAt(col).GetAsString(row);
                    if (value.empty()) {
                        os_ << options_.quote_char << options_.quote_char;
                    } else {
                        WriteField(value);
                    }
                }
            }
            os_ << '\n';
        }
    }

    void Flush() override {
        os_.flush();
    }

private:
    void WriteHeader(const core::Schema& schema) {
        for (std::size_t i = 0; i < schema.FieldsCount(); ++i) {
            if (i > 0) {
                os_ << options_.delimiter;
            }
            WriteField(schema.GetFields()[i].name);
        }
        os_ << '\n';
    }

    void WriteField(const std::string& value) {
        bool need_quotes = value.find(options_.delimiter) != std::string::npos ||
                           value.find(options_.quote_char) != std::string::npos ||
                           value.find('\n') != std::string::npos;

        if (need_quotes) {
            os_ << options_.quote_char;
            for (char c : value) {
                if (c == options_.quote_char) {
                    os_ << options_.quote_char;
                }
                os_ << c;
            }
            os_ << options_.quote_char;
        } else {
            os_ << value;
        }
    }

    std::ostream& os_;
    CSVOptions options_;
    bool header_written_ = false;
};
}  // namespace columnar::csv

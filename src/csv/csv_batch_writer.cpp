#include <csv/csv_batch_writer.h>
#include "core/columns/string_column.h"
#include "core/datatype.h"

namespace columnar::csv {
void CSVBatchWriter::Write(const core::Batch& batch) {
    if (options_.has_header && !header_written_) {
        WriteHeader(batch.GetSchema());
        header_written_ = true;
    }

    for (std::size_t row = 0; row < batch.RowsCount(); ++row) {
        for (std::size_t col = 0; col < batch.ColumnsCount(); ++col) {
            if (col > 0) {
                os_ << options_.delimiter;
            }
            auto& column = batch.ColumnAt(col);
            if (!column.IsNull(row)) {
                std::string value;
                if (column.GetDataType() ==
                    core::DataType::String) {  // Optimization for string continuous columns
                    value = dynamic_cast<const core::StringColumn*>(&column)->Get(row);
                } else {
                    value = column.GetAsString(row);
                }
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

void CSVBatchWriter::WriteHeader(const core::Schema& schema) {
    for (std::size_t i = 0; i < schema.FieldsCount(); ++i) {
        if (i > 0) {
            os_ << options_.delimiter;
        }
        WriteField(schema.GetFields()[i].name);
    }
    os_ << '\n';
}

void CSVBatchWriter::WriteField(const std::string& value) {
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
}  // namespace columnar::csv

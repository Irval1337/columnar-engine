#include <core/columns/char_column.h>
#include <core/columns/string_column.h>
#include <csv/csv_batch_writer.h>

namespace columnar::csv {
void CSVBatchWriter::Write(const core::Batch& batch) {
    if (options_.has_header && !header_written_) {
        WriteHeader(batch.GetSchema());
        header_written_ = true;
    }

    const auto& cols = batch.GetColumns();
    col_views_.clear();
    col_views_.reserve(cols.size());
    for (const auto& col : cols) {
        col_views_.push_back({col.get(), col->GetDataType(), col->IsNullable()});
    }

    for (size_t row = 0; row < batch.RowsCount(); ++row) {
        line_buf_.clear();
        for (size_t col = 0; col < col_views_.size(); ++col) {
            if (col > 0) {
                line_buf_ += options_.delimiter;
            }
            auto& view = col_views_[col];
            if (view.nullable && view.column->IsNull(row)) {
                continue;
            }
            if (view.type == core::DataType::String) {
                auto value = static_cast<const core::StringColumn*>(view.column)->Get(row);
                if (value.empty()) {
                    line_buf_ += options_.quote_char;
                    line_buf_ += options_.quote_char;
                } else {
                    AppendField(value);
                }
            } else if (view.type == core::DataType::Char) {
                char value = static_cast<const core::CharColumn*>(view.column)->Get(row);
                AppendField(std::string_view(&value, 1));
            } else {
                view.column->AppendToString(row, line_buf_);
            }
        }
        FlushLine();
    }
}

void CSVBatchWriter::WriteHeader(const core::Schema& schema) {
    line_buf_.clear();
    const auto& fields = schema.GetFields();
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i > 0) {
            line_buf_ += options_.delimiter;
        }
        AppendField(fields[i].name);
    }
    FlushLine();
}

void CSVBatchWriter::AppendField(std::string_view value) {
    bool need_quotes = false;
    size_t quotes = 0;
    for (char c : value) {
        if (c == options_.quote_char) {
            ++quotes;
            need_quotes = true;
        } else if (c == options_.delimiter || c == '\n' || c == '\r') {
            need_quotes = true;
        }
    }

    if (!need_quotes) {
        line_buf_.append(value);
        return;
    }

    line_buf_.reserve(line_buf_.size() + value.size() + quotes + 2);
    line_buf_ += options_.quote_char;
    for (char c : value) {
        if (c == options_.quote_char) {
            line_buf_ += options_.quote_char;
        }
        line_buf_ += c;
    }
    line_buf_ += options_.quote_char;
}

void CSVBatchWriter::FlushLine() {
    line_buf_ += '\n';
    os_.write(line_buf_.data(), line_buf_.size());
}
}  // namespace columnar::csv

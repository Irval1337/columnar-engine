#include <csv/csv_row_reader.h>

namespace columnar::csv {
std::optional<CSVRowReader::Row> CSVRowReader::ReadRow() {
    if (IsFinished()) {
        return std::nullopt;
    }

    Row row;
    std::string field;
    bool in_quotes = false;
    bool was_quoted = false;
    char c;
    while (is_.get(c)) {
        if (in_quotes) {
            if (c == options_.quote_char) {
                if (is_.peek() == options_.quote_char) {
                    is_.get(c);
                    field += options_.quote_char;
                } else {
                    in_quotes = false;
                }
            } else {
                field += c;
            }
        } else {
            if (c == options_.delimiter) {
                row.emplace_back(Field{std::move(field), was_quoted});
                field.clear();
                was_quoted = false;
            } else if (c == options_.quote_char) {
                in_quotes = true;
                was_quoted = true;
            } else if (c == '\n') {
                row.emplace_back(Field{std::move(field), was_quoted});
                return row;
            } else {
                field += c;
            }
        }
    }

    if (!field.empty() || !row.empty()) {
        if (in_quotes) {
            THROW_RUNTIME_ERROR("Got EOF inside the quotes");
        }
        row.emplace_back(Field{std::move(field), was_quoted});
        return row;
    }
    return std::nullopt;
}
}  // namespace columnar::csv

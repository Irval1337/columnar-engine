#pragma once

#include <csv/csv_options.h>
#include <util/macro.h>

#include <fstream>
#include <optional>
#include <vector>
#include <string>

namespace columnar::csv {
class CSVRowReader {
public:
    using Row = std::vector<std::string>;

public:
    CSVRowReader(std::istream& is, CSVOptions options = {}) : is_(is), options_(options) {
        if (options_.has_header) {
            ReadRow();
        }
    }

    std::optional<Row> ReadRow() {
        if (IsFinished()) {
            return std::nullopt;
        }

        Row row;
        std::string field;
        bool in_quotes = false;
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
                    row.emplace_back(std::move(field));
                    field.clear();
                } else if (c == options_.quote_char) {
                    in_quotes = true;
                } else if (c == '\n') {
                    row.emplace_back(std::move(field));
                    return row;
                } else {
                    field += c;
                }
            }
        }

        if (!field.empty() || !row.empty()) {
            row.emplace_back(std::move(field));
            return row;
        }
        return std::nullopt;
    }

    bool IsFinished() const {
        return is_.eof() || !is_.good();
    }

private:
    std::istream& is_;
    CSVOptions options_;
};
}  // namespace columnar::csv

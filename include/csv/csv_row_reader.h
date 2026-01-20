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
    // I hope that creating such a strange struct won't be too bad
    struct Field {
        std::string value;
        bool was_quoted = false;

        operator const std::string&() const {
            return value;
        }
        operator std::string_view() const {
            return value;
        }
        bool empty() const {  // NOLINT
            return value.empty();
        }
        std::size_t size() const {  // NOLINT
            return value.size();
        }
        const char* c_str() const {  // NOLINT
            return value.c_str();
        }
        bool operator==(const std::string& other) const {
            return value == other;
        }
        bool operator==(std::string_view other) const {
            return value == other;
        }
        bool operator==(const char* other) const {
            return value == other;
        }
    };
    using Row = std::vector<Field>;

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
                    row.emplace_back(std::move(field), was_quoted);
                    return row;
                } else {
                    field += c;
                }
            }
        }

        if (!field.empty() || !row.empty()) {
            row.emplace_back(std::move(field), was_quoted);
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

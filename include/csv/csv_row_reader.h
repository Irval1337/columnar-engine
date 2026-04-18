#pragma once

#include <csv/csv_options.h>
#include <util/macro.h>

#include <istream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace columnar::csv {
class CSVRowReader {
public:
    // was_quoted helps to identify empty unquoted fields and empty strings like ""
    template <typename T>
    struct BasicField {
        T value;
        bool was_quoted = false;

        operator const T&() const {
            return value;
        }
        operator std::string_view() const {
            return value;
        }
        bool empty() const {  // NOLINT
            return value.empty();
        }
        bool operator==(std::string_view other) const {
            return std::string_view(value) == other;
        }
    };

    using FieldView = BasicField<std::string_view>;
    using Field = BasicField<std::string>;
    using Row = std::vector<Field>;
    using RowView = std::vector<FieldView>;

    CSVRowReader(std::istream& is, CSVOptions options = {}) : is_(is), options_(options) {
        if (options_.has_header) {
            ReadRowView();
        }
    }

    // Returned view stays valid until next Read call
    const RowView* ReadRowView();
    std::optional<Row> ReadRow();

    bool IsFinished() const {
        return is_.eof() || !is_.good();
    }

private:
    bool ReadRawRow();

    std::istream& is_;
    CSVOptions options_;
    std::string line_buf_;
    std::string raw_buf_;
    std::string unescape_buf_;
    RowView parsed_fields_;
};
}  // namespace columnar::csv

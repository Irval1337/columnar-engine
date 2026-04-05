#pragma once

#include <cstdint>

namespace columnar::csv {
// We are working only with LF line endings in CSV files
struct CSVOptions {
    char delimiter = ',';
    char quote_char = '"';
    bool has_header = false;
    std::size_t batch_rows_size = 9359;
};
}  // namespace columnar::csv

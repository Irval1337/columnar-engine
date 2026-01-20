#pragma once

#include <cstdint>

namespace columnar::csv {
// I believe that we are working only with LF line endings in CSV files
// No windows support :(
struct CSVOptions {
    char delimiter = ',';
    char quote_char = '"';
    bool has_header = false;
    std::size_t batch_rows_size = 9359;  // 1337 * 7, isn't is a nice magic constant?
};
}  // namespace columnar::csv

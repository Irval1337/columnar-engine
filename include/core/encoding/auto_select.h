#pragma once

#include <core/column.h>
#include <core/encoding.h>
#include <core/schema.h>

#include <cstdint>
#include <string_view>
#include <vector>

namespace columnar::core::encoding {
struct AutoEncoding {
    Encoding encoding = Encoding::Plain;
    std::vector<std::string_view> dict_values;
    std::vector<uint32_t> dict_indexes;
    bool has_int_stats = false;
    int64_t mn = 0;
    int64_t mx = 0;
    int64_t min_delta = 0;
    int64_t max_delta = 0;
};

AutoEncoding SelectEncoding(const Column& col, const Field& field);
}  // namespace columnar::core::encoding

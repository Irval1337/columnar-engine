#pragma once

#include <util/byte_buffer.h>

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace columnar::core::encoding {
constexpr uint32_t kMaxDictSize = 65535;

void EncodeStringDictionary(util::BufWriter& w, const std::vector<char>& data,
                            const std::vector<size_t>& offsets);

void EncodeStringDictionary(util::BufWriter& w, const std::vector<std::string_view>& dict_values,
                            const std::vector<uint32_t>& indexes);

struct DecodedStringDictionary {
    std::vector<char> data;
    std::vector<size_t> offsets;
};

DecodedStringDictionary DecodeStringDictionary(util::BufReader& r, size_t n);
}  // namespace columnar::core::encoding

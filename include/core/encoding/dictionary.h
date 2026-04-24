#pragma once

#include <util/stream_helper.h>

#include <cstddef>
#include <cstdint>
#include <istream>
#include <ostream>
#include <string_view>
#include <vector>

namespace columnar::core::encoding {
constexpr uint32_t kMaxDictSize = 65535;

void EncodeStringDictionary(std::ostream& os, const std::vector<char>& data,
                            const std::vector<size_t>& offsets);

void EncodeStringDictionary(std::ostream& os, const std::vector<std::string_view>& dict_values,
                            const std::vector<uint32_t>& indexes);

struct DecodedStringDictionary {
    std::vector<char> data;
    std::vector<size_t> offsets;
};

DecodedStringDictionary DecodeStringDictionary(std::istream& is, size_t n);
}  // namespace columnar::core::encoding

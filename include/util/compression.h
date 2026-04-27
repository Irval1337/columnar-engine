#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>

namespace columnar::util {
enum class Compression : uint8_t {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
};

void Compress(Compression codec, const uint8_t* src, size_t n, std::vector<uint8_t>& out);

void Decompress(Compression codec, const uint8_t* src, size_t n, uint8_t* dst,
                size_t uncompressed_size);
}  // namespace columnar::util

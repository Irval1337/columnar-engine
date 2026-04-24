#include <core/encoding/bit_packing.h>
#include <util/macro.h>

#include <cstring>

namespace columnar::core::encoding {
void BitPack(const uint64_t* src, size_t n, uint8_t bit_width, uint8_t* out) {
    if (bit_width > kBitPackingMaxWidth) {
        THROW_RUNTIME_ERROR("bit_width is too large");
    }
    if (bit_width == 0 || n == 0) {
        return;
    }
    uint64_t mask = (uint64_t{1} << bit_width) - 1;
    uint64_t res = 0;
    uint32_t res_bits = 0;
    size_t ind = 0;
    for (size_t i = 0; i < n; ++i) {
        res |= (src[i] & mask) << res_bits;
        res_bits += bit_width;
        while (res_bits >= 8) {
            out[ind++] = static_cast<uint8_t>(res);
            res >>= 8;
            res_bits -= 8;
        }
    }
    if (res_bits > 0) {
        out[ind] = static_cast<uint8_t>(res);
    }
}

void BitUnpack(const uint8_t* src, size_t src_size, size_t n, uint8_t bit_width, uint64_t* dst) {
    if (bit_width > kBitPackingMaxWidth) {
        THROW_RUNTIME_ERROR("bit_width is too large");
    }
    if (bit_width == 0) {
        std::memset(dst, 0, n * sizeof(uint64_t));
        return;
    }
    if (n == 0) {
        return;
    }
    if (src_size < BitPackedSize(n, bit_width)) {
        THROW_RUNTIME_ERROR("Input is too small");
    }
    uint64_t mask = (uint64_t{1} << bit_width) - 1;
    uint64_t res = 0;
    uint32_t res_bits = 0;
    size_t ind = 0;
    for (size_t i = 0; i < n; ++i) {
        while (res_bits < bit_width) {
            res |= static_cast<uint64_t>(src[ind++]) << res_bits;
            res_bits += 8;
        }
        dst[i] = res & mask;
        res >>= bit_width;
        res_bits -= bit_width;
    }
}
}  // namespace columnar::core::encoding

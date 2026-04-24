#include <core/encoding/rle.h>

namespace columnar::core::encoding {
void EncodeBoolRLE(std::ostream& os, const util::BitVector& bits, size_t n) {
    uint32_t run_count = 0;
    for (size_t i = 0; i < n;) {
        bool v = bits.Get(i);
        size_t j = i + 1;
        while (j < n && bits.Get(j) == v) {
            ++j;
        }
        ++run_count;
        i = j;
    }
    util::Write<uint32_t>(os, run_count);
    for (size_t i = 0; i < n;) {
        bool v = bits.Get(i);
        size_t j = i + 1;
        while (j < n && bits.Get(j) == v) {
            ++j;
        }
        util::Write<uint32_t>(os, static_cast<uint32_t>(j - i));
        util::Write<uint8_t>(os, v ? 1 : 0);
        i = j;
    }
}

util::BitVector DecodeBoolRLE(std::istream& is, size_t n) {
    auto run_count = util::Read<uint32_t>(is);
    util::BitVector bits(n);
    size_t pos = 0;
    for (uint32_t r = 0; r < run_count; ++r) {
        auto len = util::Read<uint32_t>(is);
        bool val = util::Read<uint8_t>(is) != 0;
        if (pos + len > n) {
            THROW_RUNTIME_ERROR("RLE run length invalid");
        }
        if (val) {
            for (uint32_t k = 0; k < len; ++k) {
                bits.Set(pos + k);
            }
        }
        pos += len;
    }
    if (pos != n) {
        THROW_RUNTIME_ERROR("RLE length mismatch");
    }
    return bits;
}
}  // namespace columnar::core::encoding

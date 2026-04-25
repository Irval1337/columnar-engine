#include <core/encoding/rle.h>

namespace columnar::core::encoding {
void EncodeBoolRLE(std::ostream& os, const util::BitVector& bits, size_t n) {
    std::vector<Run<uint8_t>> runs;
    for (size_t i = 0; i < n;) {
        bool v = bits.Get(i);
        size_t j = i + 1;
        while (j < n && bits.Get(j) == v) {
            ++j;
        }
        runs.push_back({static_cast<uint32_t>(j - i), v ? uint8_t{1} : uint8_t{0}});
        i = j;
    }
    util::Write<uint32_t>(os, runs.size());
    for (auto& run : runs) {
        util::Write<uint32_t>(os, run.len);
        util::Write<uint8_t>(os, run.value);
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

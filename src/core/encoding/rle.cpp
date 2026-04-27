#include <core/encoding/rle.h>

namespace columnar::core::encoding {
void EncodeBoolRLE(util::BufWriter& w, const util::BitVector& bits, size_t n) {
    std::vector<Run<uint8_t>> runs;
    for (size_t i = 0; i < n;) {
        bool v = bits.Get(i);
        size_t j = i + 1;
        while (j < n && bits.Get(j) == v) {
            ++j;
        }
        runs.push_back({static_cast<uint32_t>(j - i), static_cast<uint8_t>(v)});
        i = j;
    }
    w.Write<uint32_t>(runs.size());
    for (auto& run : runs) {
        w.Write<uint32_t>(run.len);
        w.Write<uint8_t>(run.value);
    }
}

util::BitVector DecodeBoolRLE(util::BufReader& r, size_t n) {
    auto run_count = r.Read<uint32_t>();
    util::BitVector bits(n);
    size_t pos = 0;
    for (uint32_t i = 0; i < run_count; ++i) {
        auto len = r.Read<uint32_t>();
        bool val = r.Read<uint8_t>() != 0;
        if (pos + len > n) {
            THROW_RUNTIME_ERROR("RLE run length invalid");
        }
        if (val) {
            bits.SetRange(pos, len);
        }
        pos += len;
    }
    if (pos != n) {
        THROW_RUNTIME_ERROR("RLE length mismatch");
    }
    return bits;
}
}  // namespace columnar::core::encoding

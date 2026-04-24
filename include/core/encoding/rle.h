#pragma once

#include <util/bit_vector.h>
#include <util/macro.h>
#include <util/stream_helper.h>

#include <algorithm>
#include <cstdint>
#include <istream>
#include <ostream>
#include <vector>

namespace columnar::core::encoding {
template <util::BinaryTrivial T>
void EncodeRLE(std::ostream& os, const T* data, size_t n) {
    uint32_t cnt = 0;
    for (size_t i = 0; i < n;) {
        size_t j = i + 1;
        while (j < n && data[j] == data[i]) {
            ++j;
        }
        ++cnt;
        i = j;
    }
    util::Write<uint32_t>(os, cnt);
    for (size_t i = 0; i < n;) {
        size_t j = i + 1;
        while (j < n && data[j] == data[i]) {
            ++j;
        }
        util::Write<uint32_t>(os, static_cast<uint32_t>(j - i));
        util::Write<T>(os, data[i]);
        i = j;
    }
}

template <util::BinaryTrivial T>
std::vector<T> DecodeRLE(std::istream& is, size_t n) {
    auto cnt = util::Read<uint32_t>(is);
    std::vector<T> out(n);
    size_t pos = 0;
    for (uint32_t r = 0; r < cnt; ++r) {
        auto len = util::Read<uint32_t>(is);
        auto val = util::Read<T>(is);
        if (pos + len > n) {
            THROW_RUNTIME_ERROR("RLE length invalid");
        }
        std::fill(out.data() + pos, out.data() + pos + len, val);
        pos += len;
    }
    if (pos != n) {
        THROW_RUNTIME_ERROR("RLE length mismatch");
    }
    return out;
}

void EncodeBoolRLE(std::ostream& os, const util::BitVector& bits, size_t n);

util::BitVector DecodeBoolRLE(std::istream& is, size_t n);
}  // namespace columnar::core::encoding

#pragma once

#include <cstdint>

namespace columnar::core {
enum class Encoding : uint8_t {
    Auto = 0,
    Plain = 1,
    Dictionary = 2,
    RLE = 3,
    FrameOfReference = 4,
    BitPacking = 5,
    Delta = 6,
};
}  // namespace columnar::core

#pragma once

#include <cstdint>

namespace columnar::core {
enum class Encoding : uint8_t {
    Plain = 0,
    Dictionary = 1,
    RLE = 2,
    FrameOfReference = 3,
    BitPacking = 4,
    Delta = 5,
};
}  // namespace columnar::core

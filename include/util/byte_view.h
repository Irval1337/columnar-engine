#pragma once

#include <cstdint>
#include <span>

namespace columnar::util {
using ByteView = std::span<const uint8_t>;
}  // namespace columnar::util

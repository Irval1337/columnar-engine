#pragma once

#include <cstddef>
#include <functional>
#include <string_view>

namespace columnar::util {
struct TransparentStringHash {
    using is_transparent = void;  // NOLINT(readability-identifier-naming)

    size_t operator()(std::string_view s) const noexcept {
        return std::hash<std::string_view>{}(s);
    }
};
}  // namespace columnar::util

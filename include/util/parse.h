#pragma once

#include <util/macro.h>

#include <string>
#include <string_view>
#include <type_traits>
#include <limits>
#include <charconv>
#include <cstdint>
#include <cctype>
#include <cerrno>
#include <cstdlib>

namespace columnar::util {
inline bool LowercaseEquals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) {
        return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

template <typename T>
T ParseFromString(std::string_view s) {
    if constexpr (std::is_same_v<T, std::string>) {
        return std::string(s);
    } else if constexpr (std::is_same_v<T, bool>) {
        if (s == "1" || LowercaseEquals(s, "true")) {
            return true;
        }
        if (s == "0" || LowercaseEquals(s, "false")) {
            return false;
        }
        THROW_RUNTIME_ERROR("Invalid bool value");
    } else if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
        int64_t tmp;
        auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), tmp, 10);
        if (ec != std::errc{} || ptr != s.data() + s.size()) {
            THROW_RUNTIME_ERROR("Invalid integer value");
        }
        if (tmp < std::numeric_limits<T>::min() || tmp > std::numeric_limits<T>::max()) {
            THROW_RUNTIME_ERROR("Value out of type range");
        }
        return static_cast<T>(tmp);
    } else if constexpr (std::is_integral_v<T> && !std::is_signed_v<T>) {
        uint64_t tmp;
        auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), tmp, 10);
        if (ec != std::errc{} || ptr != s.data() + s.size()) {
            THROW_RUNTIME_ERROR("Invalid integer value");
        }
        if (tmp > std::numeric_limits<T>::max()) {
            THROW_RUNTIME_ERROR("Value out of type range");
        }
        return static_cast<T>(tmp);
    } else if constexpr (std::is_floating_point_v<T>) {
        std::string ss(s);
        char* last = nullptr;
        errno = 0;
        long double val = std::strtold(ss.c_str(), &last);
        if (last != ss.c_str() + ss.size()) {
            THROW_RUNTIME_ERROR("Invalid floating value");
        }
        if (errno == ERANGE) {
            THROW_RUNTIME_ERROR("Value out of range");
        }
        return static_cast<T>(val);
    } else {
        THROW_RUNTIME_ERROR("Unsupported type");
    }
}
}  // namespace columnar::util

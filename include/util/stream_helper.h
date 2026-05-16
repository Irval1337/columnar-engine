#pragma once

#include <cstring>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

namespace columnar::util {
template <typename T>
concept BinaryTrivial = std::is_trivially_copyable_v<std::remove_cvref_t<T>> &&
                        std::is_standard_layout_v<std::remove_cvref_t<T>> &&
                        !std::is_same_v<std::remove_cvref_t<T>, bool>;

template <BinaryTrivial T>
inline void Write(std::ostream& stream, const T& value) {
    stream.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

inline void WriteString(std::ostream& stream, const std::string& value) {
    stream.write(value.data(), value.size());
}

template <BinaryTrivial T>
inline void WriteArray(std::ostream& stream, const std::vector<T>& values) {
    stream.write(reinterpret_cast<const char*>(values.data()), sizeof(T) * values.size());
}

inline void WriteRaw(std::ostream& stream, const void* src, size_t n) {
    stream.write(reinterpret_cast<const char*>(src), n);
}
}  // namespace columnar::util

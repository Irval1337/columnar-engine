#pragma once

#include <concepts>
#include <fstream>
#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

namespace columnar::util {
template <typename T>
concept BinaryTrivial = std::is_trivially_copyable_v<std::remove_cvref_t<T>> &&
                        std::is_standard_layout_v<std::remove_cvref_t<T>> &&
                        !std::is_same_v<std::remove_cvref_t<T>, bool>;

template <BinaryTrivial T>
inline T Read(std::istream& stream) {
    T value;
    stream.read(reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

inline std::string ReadString(std::istream& stream, std::size_t len) {
    std::string value;
    value.resize(len);
    stream.read(value.data(), len);
    return value;
}

inline void Skip(std::istream& stream, std::size_t len) {
    stream.seekg(len, std::ios::cur);
}

template <BinaryTrivial T>
inline std::vector<T> ReadArray(std::istream& stream, std::size_t count) {
    std::vector<T> values(count);
    stream.read(reinterpret_cast<char*>(values.data()), sizeof(T) * count);
    return values;
}

inline std::vector<uint64_t> ReadBoolArray(std::istream& stream, std::size_t bits_count) {
    auto count = (bits_count + 63) / 64;
    std::vector<uint64_t> values(count);
    stream.read(reinterpret_cast<char*>(values.data()), sizeof(uint64_t) * count);
    return values;
}

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
}  // namespace columnar::util

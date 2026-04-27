#pragma once

#include <util/stream_helper.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace columnar::util {
class BufWriter {
public:
    explicit BufWriter(std::vector<uint8_t>& buf) : buf_(buf) {
    }

    template <BinaryTrivial T>
    void Write(const T& value) {
        size_t old = buf_.size();
        buf_.resize(old + sizeof(T));
        std::memcpy(buf_.data() + old, &value, sizeof(T));
    }

    void WriteRaw(const void* src, size_t n) {
        size_t old = buf_.size();
        buf_.resize(old + n);
        std::memcpy(buf_.data() + old, src, n);
    }

    void WriteString(const std::string& value) {
        WriteRaw(value.data(), value.size());
    }

    template <BinaryTrivial T>
    void WriteArray(const std::vector<T>& values) {
        WriteRaw(values.data(), sizeof(T) * values.size());
    }

private:
    std::vector<uint8_t>& buf_;
};

class BufReader {
public:
    BufReader(const uint8_t* data, size_t size) : pos_(data), end_(data + size) {
    }

    template <BinaryTrivial T>
    T Read() {
        T value;
        std::memcpy(&value, pos_, sizeof(T));
        pos_ += sizeof(T);
        return value;
    }

    void ReadRaw(void* dst, size_t n) {
        std::memcpy(dst, pos_, n);
        pos_ += n;
    }

    std::string ReadString(size_t len) {
        std::string value(reinterpret_cast<const char*>(pos_), len);
        pos_ += len;
        return value;
    }

    template <BinaryTrivial T>
    std::vector<T> ReadArray(size_t count) {
        std::vector<T> values(count);
        std::memcpy(values.data(), pos_, sizeof(T) * count);
        pos_ += sizeof(T) * count;
        return values;
    }

    const uint8_t* Take(size_t n) {
        const uint8_t* p = pos_;
        pos_ += n;
        return p;
    }

    const uint8_t* Pos() const {
        return pos_;
    }

    size_t Remaining() const {
        return static_cast<size_t>(end_ - pos_);
    }

private:
    const uint8_t* pos_;
    const uint8_t* end_;
};
}  // namespace columnar::util

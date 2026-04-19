#pragma once

#include <vector>
#include <cstdint>

namespace columnar::util {
class BitVector {
public:
    BitVector() : size_(0) {
    }

    BitVector(std::vector<uint64_t>&& data, size_t size)
        : size_(size), bits_(std::move(data)) {
    }

    BitVector(size_t size) {
        size_ = size;
        bits_.resize((size + 63) / 64, 0);
    }

    ~BitVector() = default;

    void Set(size_t i) {
        bits_[i / 64] |= static_cast<uint64_t>(1) << (i % 64);
    }

    void PushBack(bool value) {
        if (size_ % 64 == 0) {
            bits_.push_back(0);
        }
        if (value) {
            Set(size_);
        }
        ++size_;
    }

    void Clear() {
        bits_.clear();
        size_ = 0;
    }

    void Reserve(size_t n) {
        bits_.reserve((n + 63) / 64);
    }

    bool Get(size_t i) const {
        return (bits_[i / 64] >> (i % 64)) & 1;
    }

    size_t Size() const {
        return size_;
    }

    const std::vector<uint64_t>& GetData() const {
        return bits_;
    }

private:
    size_t size_;
    std::vector<uint64_t> bits_;
};
}  // namespace columnar::util

#pragma once

#include <vector>
#include <cstdint>

namespace columnar::util {
class BitVector {
public:
    BitVector() : size_(0) {
    }

    BitVector(std::vector<uint64_t>&& data, size_t size) : size_(size), bits_(std::move(data)) {
    }

    BitVector(size_t size) {
        size_ = size;
        bits_.resize((size + 63) / 64, 0);
    }

    ~BitVector() = default;

    void Set(size_t i) {
        bits_[i / 64] |= static_cast<uint64_t>(1) << (i % 64);
    }

    void SetRange(size_t pos, size_t len) {
        if (len == 0) {
            return;
        }
        size_t head_word = pos / 64;
        size_t tail_word = (pos + len) / 64;
        uint64_t ones = -1;
        if (head_word == tail_word) {
            uint64_t mask = (ones << (pos % 64)) & (ones >> (64 - (pos + len) % 64));
            bits_[head_word] |= mask;
            return;
        }
        bits_[head_word] |= ones << (pos % 64);
        for (size_t w = head_word + 1; w < tail_word; ++w) {
            bits_[w] = ones;
        }
        if ((pos + len) % 64 > 0) {
            bits_[tail_word] |= ones >> (64 - (pos + len) % 64);
        }
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

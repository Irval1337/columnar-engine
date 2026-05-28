#pragma once

#include <cstddef>
#include <cstring>
#include <memory>
#include <string_view>
#include <vector>

namespace columnar::util {
class StringArena {
public:
    StringArena() = default;

    StringArena(const StringArena&) = delete;
    StringArena& operator=(const StringArena&) = delete;
    StringArena(StringArena&& other) noexcept;
    StringArena& operator=(StringArena&& other) noexcept;

    std::string_view Intern(std::string_view s) {
        if (s.empty()) {
            return {};
        }
        if (s.size() > remaining_) {
            AllocateChunk(s.size());
        }
        char* dest = next_;
        std::memcpy(dest, s.data(), s.size());
        next_ += s.size();
        remaining_ -= s.size();
        bytes_used_ += s.size();
        return {dest, s.size()};
    }
    void Reset() noexcept;

    size_t BytesUsed() const noexcept;
    size_t BytesReserved() const noexcept;

private:
    void AllocateChunk(size_t min_size);
    void MoveFrom(StringArena&& other) noexcept;

    static constexpr size_t kMinChunkBytes = 64 * 1024;

    struct Chunk {
        std::unique_ptr<char[]> data;
        size_t capacity;
    };

    std::vector<Chunk> chunks_;
    char* next_ = nullptr;
    size_t remaining_ = 0;
    size_t bytes_used_ = 0;
};
}  // namespace columnar::util

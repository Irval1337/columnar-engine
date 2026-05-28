#include <util/string_arena.h>

#include <algorithm>
#include <utility>

namespace columnar::util {
StringArena::StringArena(StringArena&& other) noexcept {
    MoveFrom(std::move(other));
}

StringArena& StringArena::operator=(StringArena&& other) noexcept {
    if (this != &other) {
        MoveFrom(std::move(other));
    }
    return *this;
}

void StringArena::Reset() noexcept {
    chunks_.clear();
    next_ = nullptr;
    remaining_ = 0;
    bytes_used_ = 0;
}

size_t StringArena::BytesUsed() const noexcept {
    return bytes_used_;
}

size_t StringArena::BytesReserved() const noexcept {
    size_t total = 0;
    for (const auto& chunk : chunks_) {
        total += chunk.capacity;
    }
    return total;
}

void StringArena::AllocateChunk(size_t min_size) {
    size_t capacity = std::max(kMinChunkBytes, min_size);
    if (!chunks_.empty()) {
        capacity = std::max(capacity, chunks_.back().capacity * 2);
    }
    std::unique_ptr<char[]> data(new char[capacity]);
    char* next = data.get();
    chunks_.push_back(Chunk{std::move(data), capacity});
    next_ = next;
    remaining_ = capacity;
}

void StringArena::MoveFrom(StringArena&& other) noexcept {
    chunks_ = std::move(other.chunks_);
    next_ = std::exchange(other.next_, nullptr);
    remaining_ = std::exchange(other.remaining_, 0);
    bytes_used_ = std::exchange(other.bytes_used_, 0);
    other.chunks_.clear();
}
}  // namespace columnar::util

#pragma once

#include <util/byte_view.h>

#include <cstddef>
#include <cstdint>
#include <string>

namespace columnar::util {
class MemoryMappedInputFile {
public:
    explicit MemoryMappedInputFile(const std::string& path);

    ~MemoryMappedInputFile();

    MemoryMappedInputFile(const MemoryMappedInputFile&) = delete;
    MemoryMappedInputFile& operator=(const MemoryMappedInputFile&) = delete;

    MemoryMappedInputFile(MemoryMappedInputFile&& other) noexcept;
    MemoryMappedInputFile& operator=(MemoryMappedInputFile&& other) noexcept;

    ByteView View() const {
        return ByteView{data_, size_};
    }

private:
    void Close() noexcept;

    const uint8_t* data_ = nullptr;
    size_t size_ = 0;
    int fd_ = -1;
};
}  // namespace columnar::util

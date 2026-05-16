#include <util/memory_mapped_file.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <utility>

namespace columnar::util {
MemoryMappedInputFile::MemoryMappedInputFile(const std::string& path) {
    fd_ = open(path.c_str(), O_RDONLY);
    if (fd_ < 0) {
        throw std::runtime_error("Cannot open file " + path + ": " + std::strerror(errno));
    }

    struct stat st {};
    if (fstat(fd_, &st) != 0) {
        int err = errno;
        Close();
        throw std::runtime_error("Cannot stat file " + path + ": " + std::strerror(err));
    }
    if (st.st_size < 0) {
        Close();
        throw std::runtime_error("Invalid file size for " + path);
    }

    size_ = static_cast<size_t>(st.st_size);
    if (size_ == 0) {
        return;
    }

    void* mapped = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
    if (mapped == MAP_FAILED) {
        int err = errno;
        size_ = 0;
        Close();
        throw std::runtime_error("Cannot mmap file " + path + ": " + std::strerror(err));
    }
    data_ = static_cast<const uint8_t*>(mapped);
}

MemoryMappedInputFile::~MemoryMappedInputFile() {
    Close();
}

MemoryMappedInputFile::MemoryMappedInputFile(MemoryMappedInputFile&& other) noexcept {
    *this = std::move(other);
}

MemoryMappedInputFile& MemoryMappedInputFile::operator=(MemoryMappedInputFile&& other) noexcept {
    if (this != &other) {
        Close();
        data_ = other.data_;
        size_ = other.size_;
        fd_ = other.fd_;
        other.data_ = nullptr;
        other.size_ = 0;
        other.fd_ = -1;
    }
    return *this;
}

void MemoryMappedInputFile::Close() noexcept {
    if (data_ != nullptr) {
        munmap(const_cast<uint8_t*>(data_), size_);
    }
    if (fd_ >= 0) {
        close(fd_);
    }
    data_ = nullptr;
    size_ = 0;
    fd_ = -1;
}
}  // namespace columnar::util

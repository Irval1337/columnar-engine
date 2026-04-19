#include <util/buffered_file.h>

#include <stdexcept>

namespace columnar::util {
BufferedInputFile::BufferedInputFile(const std::string& path, size_t buf_size)
    : std::istream(&filebuf_), buf_(buf_size) {
    filebuf_.pubsetbuf(buf_.data(), buf_.size());
    if (!filebuf_.open(path, std::ios::in | std::ios::binary)) {
        throw std::runtime_error("Cannot open file " + path);
    }
}

BufferedOutputFile::BufferedOutputFile(const std::string& path, size_t buf_size)
    : std::ostream(&filebuf_), buf_(buf_size) {
    filebuf_.pubsetbuf(buf_.data(), buf_.size());
    if (!filebuf_.open(path, std::ios::out | std::ios::binary | std::ios::trunc)) {
        throw std::runtime_error("Cannot open file " + path);
    }
}
}  // namespace columnar::util

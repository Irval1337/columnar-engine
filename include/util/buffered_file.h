#pragma once

#include <fstream>
#include <istream>
#include <ostream>
#include <string>
#include <vector>

namespace columnar::util {
inline constexpr size_t kDefaultFileBufSize = 32 * 1024 * 1024;
class BufferedInputFile : public std::istream {
public:
    explicit BufferedInputFile(const std::string& path, size_t buf_size = kDefaultFileBufSize);

private:
    std::vector<char> buf_;
    std::filebuf filebuf_;
};

class BufferedOutputFile : public std::ostream {
public:
    explicit BufferedOutputFile(const std::string& path,
                                size_t buf_size = kDefaultFileBufSize);

private:
    std::vector<char> buf_;
    std::filebuf filebuf_;
};
}  // namespace columnar::util

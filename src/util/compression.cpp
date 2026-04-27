#include <util/compression.h>
#include <util/macro.h>

#include <lz4.h>
#include <zstd.h>

#include <cstring>
#include <limits>

namespace columnar::util {
namespace {
constexpr int kZstdLevel = 3;
}

void Compress(Compression codec, const uint8_t* src, size_t n, std::vector<uint8_t>& out) {
    switch (codec) {
        case Compression::None:
            out.assign(src, src + n);
            return;
        case Compression::Lz4: {
            if (n > static_cast<size_t>(LZ4_MAX_INPUT_SIZE)) {
                THROW_RUNTIME_ERROR("LZ4 input too large");
            }
            int bound = LZ4_compressBound(static_cast<int>(n));
            out.resize(static_cast<size_t>(bound));
            int written = LZ4_compress_default(reinterpret_cast<const char*>(src),
                                               reinterpret_cast<char*>(out.data()),
                                               static_cast<int>(n), bound);
            if (written <= 0) {
                THROW_RUNTIME_ERROR("LZ4 compress failed");
            }
            out.resize(static_cast<size_t>(written));
            return;
        }
        case Compression::Zstd: {
            size_t bound = ZSTD_compressBound(n);
            out.resize(bound);
            size_t written = ZSTD_compress(out.data(), bound, src, n, kZstdLevel);
            if (ZSTD_isError(written)) {
                THROW_RUNTIME_ERROR("ZSTD compress failed");
            }
            out.resize(written);
            return;
        }
    }
    THROW_RUNTIME_ERROR("Unknown compression codec");
}

void Decompress(Compression codec, const uint8_t* src, size_t n, uint8_t* dst,
                size_t uncompressed_size) {
    switch (codec) {
        case Compression::None:
            if (n != uncompressed_size) {
                THROW_RUNTIME_ERROR("None decompress size mismatch");
            }
            std::memcpy(dst, src, n);
            return;
        case Compression::Lz4: {
            if (n > static_cast<size_t>(std::numeric_limits<int>::max()) ||
                uncompressed_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
                THROW_RUNTIME_ERROR("LZ4 input too large");
            }
            int written = LZ4_decompress_safe(reinterpret_cast<const char*>(src),
                                              reinterpret_cast<char*>(dst), static_cast<int>(n),
                                              static_cast<int>(uncompressed_size));
            if (written < 0 || static_cast<size_t>(written) != uncompressed_size) {
                THROW_RUNTIME_ERROR("LZ4 decompress failed");
            }
            return;
        }
        case Compression::Zstd: {
            size_t written = ZSTD_decompress(dst, uncompressed_size, src, n);
            if (ZSTD_isError(written) || written != uncompressed_size) {
                THROW_RUNTIME_ERROR("ZSTD decompress failed");
            }
            return;
        }
    }
    THROW_RUNTIME_ERROR("Unknown compression codec");
}
}  // namespace columnar::util

#pragma once

#include <core/datatype.h>
#include <core/schema.h>

#include <cstdint>
#include <vector>

// File format was inspired by https://parquet.apache.org/docs/file-format/
// Binary representation looks like this:
// Magic BRUHDB bytes (8 bytes)
// Row groups with data (variable count and size)
// Footer (contains FileMetaData)
// Footer size (4 bytes)
// Magic BRUHDB bytes (8 bytes)
// My amazing DB uses ONLY little-endian bytes ordering
namespace columnar::bruh {
constexpr uint8_t kMagicBytes[8] = {'B', 'R', 'U', 'H', 'D', 'B', 0x67, 0x67};
constexpr int kCurrentVersion = 1;

struct ColumnChunkMetaData {
    uint64_t offset;
    uint64_t byte_size;  // (@Irval1337) TODO: split this into compressed and decompressed sizes
    uint64_t values_count;
};

struct RowGroupMetaData {
    uint64_t byte_size;
    uint64_t rows_count;
    std::vector<ColumnChunkMetaData> columns;
};

struct FileMetaData {
    int version;
    core::Schema schema;
    uint64_t rows_count;
    std::vector<RowGroupMetaData> row_groups;
};
}  // namespace columnar::bruh

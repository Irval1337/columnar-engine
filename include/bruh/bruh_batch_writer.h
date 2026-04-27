#pragma once

#include <bruh/format.h>
#include <core/batch_writer.h>
#include <core/encoding.h>
#include <util/compression.h>
#include <util/macro.h>

#include <cstdint>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <vector>

namespace columnar::bruh {
struct BruhWriterOptions {
    util::Compression compression = util::Compression::Lz4;
    core::Encoding encoding = core::Encoding::Auto;
    std::unordered_map<size_t, core::Encoding> column_encoding;
    std::unordered_map<size_t, util::Compression> column_compression;
};

class BruhBatchWriter final : public core::BatchWriter {
public:
    BruhBatchWriter(std::ostream& os, const core::Schema& schema, BruhWriterOptions options = {})
        : os_(os), schema_(schema), options_(std::move(options)) {
        metadata_.version = kCurrentVersion;
        metadata_.schema = schema;
        WriteMagic();
    }

    void Write(const core::Batch& batch) override;

    void Flush() override;

private:
    void WriteMagic() {
        os_.write(reinterpret_cast<const char*>(kMagicBytes), sizeof(kMagicBytes));
    }

    void WriteColumn(ColumnChunkMetaData& chunk, const core::Column& col, const core::Field& field,
                     size_t col_index);

    void WriteFooter();
    void WriteFields();
    void WriteRowGroups();

    std::ostream& os_;
    core::Schema schema_;
    BruhWriterOptions options_;
    FileMetaData metadata_;
    std::vector<uint8_t> packed_buf_;
    std::vector<uint8_t> encode_buf_;
    std::vector<uint8_t> compress_buf_;
};
}  // namespace columnar::bruh

#pragma once

#include <bruh/format.h>
#include <core/batch_writer.h>
#include <core/encoding.h>
#include <util/macro.h>

#include <optional>
#include <ostream>
#include <unordered_map>
#include <utility>

namespace columnar::bruh {
struct BruhWriterOptions {
    std::optional<core::Encoding> force_encoding;
    std::unordered_map<size_t, core::Encoding> per_column;
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
};
}  // namespace columnar::bruh

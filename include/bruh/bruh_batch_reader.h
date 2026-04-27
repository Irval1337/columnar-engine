#pragma once

#include <core/batch_reader.h>
#include <bruh/format.h>
#include <util/byte_buffer.h>
#include <util/macro.h>

#include <cstdint>
#include <memory>
#include <vector>

// (@Irval1337) TODO: Maybe implement file io using mmap?
namespace columnar::bruh {
class BruhBatchReader final : public core::BatchReader {
public:
    BruhBatchReader(std::istream& is) : is_(is), curr_row_group_(0) {
        ReadMetaData();
    }

    std::optional<core::Batch> ReadNext() override {
        if (curr_row_group_ >= metadata_.row_groups.size()) {
            return std::nullopt;
        }
        return ReadRowGroup(curr_row_group_++);
    }

    core::Batch ReadRowGroup(size_t i);

    size_t NumRowGroups() const {
        return metadata_.row_groups.size();
    }

    const FileMetaData& GetMetaData() const {
        return metadata_;
    }

    const core::Schema& GetSchema() const override {
        return metadata_.schema;
    }

private:
    void ReadMetaData();

    void EnsureBruhFormat();

    void ReadSchema(uint32_t cols_count);

    void ReadRowGroupsMetadata(uint32_t cols_count);

    void ReadColumn(util::BufReader& r, std::unique_ptr<core::Column>& col,
                    const core::Field& field, const ColumnChunkMetaData& chunk);

    std::istream& is_;
    FileMetaData metadata_;
    size_t curr_row_group_;
    std::vector<uint8_t> packed_buf_;
    std::vector<uint8_t> compress_buf_;
    std::vector<uint8_t> encode_buf_;
};
}  // namespace columnar::bruh

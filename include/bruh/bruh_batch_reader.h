#pragma once

#include <core/batch_reader.h>
#include <bruh/format.h>
#include <util/byte_view.h>
#include <util/byte_buffer.h>
#include <util/macro.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace columnar::bruh {
class BruhBatchReader final : public core::BatchReader {
public:
    explicit BruhBatchReader(util::ByteView data) : data_(data), curr_row_group_(0) {
        ReadMetaData();
    }

    std::optional<core::Batch> ReadNext() override {
        if (curr_row_group_ >= metadata_.row_groups.size()) {
            return std::nullopt;
        }
        return ReadRowGroup(curr_row_group_++);
    }

    std::optional<core::Batch> ReadNext(const std::vector<size_t>& column_indexes) {
        if (curr_row_group_ >= metadata_.row_groups.size()) {
            return std::nullopt;
        }
        return ReadRowGroup(curr_row_group_++, column_indexes);
    }

    core::Batch ReadRowGroup(size_t i);

    core::Batch ReadRowGroup(size_t i, const std::vector<size_t>& column_indexes);

    core::Batch ReadRowGroup(size_t i, const std::vector<std::string>& column_names) {
        return ReadRowGroup(i, ResolveColumnNames(column_names));
    }

    std::vector<size_t> ResolveColumnNames(const std::vector<std::string>& column_names) const;

    core::Schema ProjectSchema(const std::vector<size_t>& column_indexes) const;

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

    void ReadSchema(util::BufReader& r, uint32_t cols_count);

    void ReadRowGroupsMetadata(util::BufReader& r, uint32_t cols_count);

    void ReadColumn(util::BufReader& r, std::unique_ptr<core::Column>& col,
                    const core::Field& field, const ColumnChunkMetaData& chunk);

    util::ByteView data_;
    FileMetaData metadata_;
    size_t curr_row_group_;
    std::vector<uint8_t> encode_buf_;
};
}  // namespace columnar::bruh

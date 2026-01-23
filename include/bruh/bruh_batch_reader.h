#pragma once

#include <core/batch_reader.h>
#include <bruh/format.h>
#include <util/macro.h>

#include <fstream>
#include <memory>
#include <string>
#include <cstring>

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

    core::Batch ReadRowGroup(std::size_t i);

    std::size_t NumRowGroups() const {
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

    // Warning: You must call these methods in the same order as below
    void EnsureBruhFormat();

    void ReadSchema(uint32_t cols_count);

    void ReadRowGroupsMetadata(uint32_t cols_count);

    void ReadColumn(std::unique_ptr<core::Column>& col, const core::Field& field, std::size_t n);

    std::istream& is_;
    FileMetaData metadata_;
    std::size_t curr_row_group_;
};
}  // namespace columnar::bruh

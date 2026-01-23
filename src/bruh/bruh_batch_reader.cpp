#include <bruh/bruh_batch_reader.h>
#include <bruh/format.h>
#include <util/stream_helper.h>
#include <cstdint>
#include <core/column.h>
#include <util/bit_vector.h>

namespace columnar::bruh {
core::Batch BruhBatchReader::ReadRowGroup(std::size_t i) {
    if (i >= metadata_.row_groups.size()) {
        THROW_RUNTIME_ERROR("Row group index out of range");
    }

    auto& group = metadata_.row_groups[i];
    auto& schema = metadata_.schema;
    core::Batch batch(metadata_.schema, group.rows_count);
    auto& columns = batch.GetColumns();
    for (std::size_t col = 0; col < schema.FieldsCount(); ++col) {
        is_.seekg(group.columns[col].offset);
        ReadColumn(columns[col], schema.GetFields()[col], group.columns[col].values_count);
    }
    return batch;
}

void BruhBatchReader::ReadMetaData() {
    EnsureBruhFormat();
    uint32_t cols_count = util::Read<uint32_t>(is_);
    ReadSchema(cols_count);
    ReadRowGroupsMetadata(cols_count);
}

void BruhBatchReader::EnsureBruhFormat() {
    is_.seekg(-(sizeof(kMagicBytes) + 4), std::ios::end);
    auto meta_size = util::Read<uint32_t>(is_);
    auto magic_bruh = util::ReadArray<uint8_t>(is_, sizeof(kMagicBytes));
    if (std::memcmp(magic_bruh.data(), kMagicBytes, sizeof(kMagicBytes)) != 0) {
        THROW_RUNTIME_ERROR("Bad magic bytes");
    }
    is_.seekg(-(sizeof(kMagicBytes) + meta_size + 4), std::ios::end);

    metadata_.version = util::Read<int>(is_);
    if (metadata_.version != kCurrentVersion) {
        THROW_RUNTIME_ERROR("File version mismatch");
    }
}

void BruhBatchReader::ReadSchema(uint32_t cols_count) {
    std::vector<core::Field> fields;
    fields.reserve(cols_count);
    for (uint32_t i = 0; i < cols_count; ++i) {
        // (@Irval1337) TODO: maybe store field names concisely?
        auto name_len = util::Read<uint32_t>(is_);
        auto name = util::ReadString(is_, name_len);

        auto type = util::Read<core::DataType>(is_);
        auto nullable = util::Read<uint8_t>(is_) != 0;
        fields.emplace_back(name, type, nullable);
    }
    metadata_.schema = core::Schema(std::move(fields));
}

void BruhBatchReader::ReadRowGroupsMetadata(uint32_t cols_count) {
    metadata_.rows_count = util::Read<uint64_t>(is_);
    uint32_t groups_count = util::Read<uint32_t>(is_);
    metadata_.row_groups.reserve(groups_count);
    for (uint32_t i = 0; i < groups_count; ++i) {
        RowGroupMetaData group;
        group.rows_count = util::Read<uint64_t>(is_);
        group.byte_size = util::Read<uint64_t>(is_);
        group.columns.reserve(cols_count);
        for (uint32_t c = 0; c < cols_count; ++c) {
            ColumnChunkMetaData chunk;
            chunk.offset = util::Read<uint64_t>(is_);
            chunk.byte_size = util::Read<uint64_t>(is_);
            chunk.values_count = util::Read<uint64_t>(is_);
            group.columns.emplace_back(std::move(chunk));
        }
        metadata_.row_groups.emplace_back(std::move(group));
    }
}

void BruhBatchReader::ReadColumn(std::unique_ptr<core::Column>& col, const core::Field& field, std::size_t n) {
    util::BitVector is_null;
    if (field.nullable) {
        auto nulls_data = util::ReadBoolArray(is_, n);
        is_null = util::BitVector(std::move(nulls_data), n);
    }

    switch (field.type) {
        case core::DataType::Int64: {
            auto column_data = util::ReadArray<int64_t>(is_, n);
            col = std::make_unique<core::Int64Column>(std::move(column_data), std::move(is_null), field.nullable);
            break;
        }
        case core::DataType::Double: {
            auto column_data = util::ReadArray<double>(is_, n);
            col = std::make_unique<core::DoubleColumn>(std::move(column_data), std::move(is_null), field.nullable);
            break;
        }
        case core::DataType::Bool: {
            auto column_data_bits = util::ReadBoolArray(is_, n);
            util::BitVector column_data(std::move(column_data_bits), n);
            col = std::make_unique<core::BoolColumn>(std::move(column_data), std::move(is_null), field.nullable, n);
            break;
        }
        case core::DataType::String: {
            auto offsets = util::ReadArray<std::size_t>(is_, n);
            auto lengths = util::ReadArray<std::size_t>(is_, n);
            std::size_t data_length = offsets.empty() ? 0 : (offsets.back() + lengths.back());
            auto column_data = util::ReadArray<char>(is_, data_length);
            col = std::make_unique<core::StringColumn>(std::move(column_data), std::move(offsets), std::move(lengths),
                                     std::move(is_null), field.nullable);
            break;
        }
        default:
            THROW_RUNTIME_ERROR("Unsupported type");
    }
}
}  // namespace columnar::bruh

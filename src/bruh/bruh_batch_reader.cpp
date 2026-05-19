#include <bruh/bruh_batch_reader.h>
#include <bruh/format.h>
#include <core/columns/dictionary_string_column.h>
#include <core/columns/date_column.h>
#include <core/columns/timestamp_column.h>
#include <core/encoding/bit_packing.h>
#include <core/encoding/delta.h>
#include <core/encoding/dictionary.h>
#include <core/encoding/frame_of_reference.h>
#include <core/encoding/rle.h>
#include <util/bit_vector.h>
#include <util/byte_buffer.h>
#include <util/compression.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>
#include <vector>

namespace columnar::bruh {
namespace {
util::BitVector ReadBitVector(util::BufReader& r, size_t n) {
    size_t packed_size = core::encoding::BitPackedSize(n, 1);
    return core::encoding::UnpackBitVector(r.Take(packed_size), packed_size, n);
}

template <typename T>
std::vector<size_t> ReadOffsets(util::BufReader& r, size_t n) {
    auto raw = r.ReadArray<T>(n + 1);
    return std::vector<size_t>(raw.begin(), raw.end());
}

std::vector<size_t> DecodeOffsetsPlain(util::BufReader& r, size_t n) {
    auto width = r.Read<uint8_t>();
    if (width == 4) {
        return ReadOffsets<uint32_t>(r, n);
    }
    if (width == 8) {
        return ReadOffsets<uint64_t>(r, n);
    }
    THROW_RUNTIME_ERROR("Unsupported string offset width");
}

template <typename Column, typename T>
std::unique_ptr<core::Column> DecodeNumeric(util::BufReader& r, bool nullable,
                                            core::Encoding encoding, size_t n) {
    util::BitVector is_null;
    if (nullable) {
        is_null = ReadBitVector(r, n);
    }
    switch (encoding) {
        case core::Encoding::Plain: {
            auto data = r.ReadArray<T>(n);
            return std::make_unique<Column>(std::move(data), std::move(is_null), nullable);
        }
        case core::Encoding::RLE: {
            auto data = core::encoding::DecodeRLE<T>(r, n);
            return std::make_unique<Column>(std::move(data), std::move(is_null), nullable);
        }
        case core::Encoding::FrameOfReference: {
            if constexpr (std::is_integral_v<T>) {
                auto data = core::encoding::DecodeFOR<T>(r, n);
                return std::make_unique<Column>(std::move(data), std::move(is_null), nullable);
            } else {
                THROW_RUNTIME_ERROR("FrameOfReference needs an integer column");
            }
        }
        case core::Encoding::Delta: {
            if constexpr (std::is_integral_v<T>) {
                auto data = core::encoding::DecodeDelta<T>(r, n);
                return std::make_unique<Column>(std::move(data), std::move(is_null), nullable);
            } else {
                THROW_RUNTIME_ERROR("Delta needs an integer column");
            }
        }
        case core::Encoding::BitPacking: {
            if constexpr (std::is_integral_v<T>) {
                uint8_t bit_width = r.Read<uint8_t>();
                if (bit_width > core::encoding::kBitPackingMaxWidth) {
                    THROW_RUNTIME_ERROR("bit_width is too large");
                }
                std::vector<T> data(n);
                if (bit_width == 0 || n == 0) {
                    return std::make_unique<Column>(std::move(data), std::move(is_null), nullable);
                }
                size_t packed_size = core::encoding::BitPackedSize(n, bit_width);
                core::encoding::BitUnpackWithOffset(r.Take(packed_size), packed_size, n, bit_width,
                                                    T(0), data.data());
                return std::make_unique<Column>(std::move(data), std::move(is_null), nullable);
            } else {
                THROW_RUNTIME_ERROR("BitPacking needs an integer column");
            }
        }
        default:
            THROW_RUNTIME_ERROR("Encoding does not support numeric column");
    }
}

std::unique_ptr<core::Column> DecodeBool(util::BufReader& r, bool nullable, core::Encoding encoding,
                                         size_t n) {
    util::BitVector is_null;
    if (nullable) {
        is_null = ReadBitVector(r, n);
    }
    switch (encoding) {
        case core::Encoding::Plain:
        case core::Encoding::BitPacking: {
            auto data = ReadBitVector(r, n);
            return std::make_unique<core::BoolColumn>(std::move(data), std::move(is_null), nullable,
                                                      n);
        }
        case core::Encoding::RLE: {
            auto data = core::encoding::DecodeBoolRLE(r, n);
            return std::make_unique<core::BoolColumn>(std::move(data), std::move(is_null), nullable,
                                                      n);
        }
        default:
            THROW_RUNTIME_ERROR("Encoding does not support bool column");
    }
}

std::unique_ptr<core::Column> DecodeString(util::BufReader& r, bool nullable,
                                           core::Encoding encoding, size_t n) {
    util::BitVector is_null;
    if (nullable) {
        is_null = ReadBitVector(r, n);
    }
    switch (encoding) {
        case core::Encoding::Plain: {
            auto offsets = DecodeOffsetsPlain(r, n);
            auto data = r.ReadArray<char>(offsets.back());
            return std::make_unique<core::StringColumn>(std::move(data), std::move(offsets),
                                                        std::move(is_null), nullable);
        }
        case core::Encoding::Dictionary: {
            auto decoded = core::encoding::DecodeStringDictionary(r, n);
            return std::make_unique<core::DictionaryStringColumn>(
                std::move(decoded.dict_data), std::move(decoded.dict_offsets),
                std::move(decoded.ids), std::move(is_null), nullable);
        }
        default:
            THROW_RUNTIME_ERROR("Encoding does not support string column");
    }
}

std::unique_ptr<core::Column> DecodeChar(util::BufReader& r, bool nullable, core::Encoding encoding,
                                         size_t n) {
    util::BitVector is_null;
    if (nullable) {
        is_null = ReadBitVector(r, n);
    }
    switch (encoding) {
        case core::Encoding::Plain: {
            auto data = r.ReadArray<char>(n);
            return std::make_unique<core::CharColumn>(std::move(data), std::move(is_null),
                                                      nullable);
        }
        case core::Encoding::RLE: {
            auto data = core::encoding::DecodeRLE<char>(r, n);
            return std::make_unique<core::CharColumn>(std::move(data), std::move(is_null),
                                                      nullable);
        }
        default:
            THROW_RUNTIME_ERROR("Encoding does not support char column");
    }
}

void CheckMappedRange(uint64_t offset, uint64_t size, size_t mapped_size) {
    if (offset > std::numeric_limits<size_t>::max() || size > std::numeric_limits<size_t>::max()) {
        THROW_RUNTIME_ERROR("Mapped chunk range is too large");
    }
    auto begin = static_cast<size_t>(offset);
    auto len = static_cast<size_t>(size);
    if (begin > mapped_size || len > mapped_size - begin) {
        THROW_RUNTIME_ERROR("Mapped chunk range is out of file bounds");
    }
}

}  // namespace

core::Batch BruhBatchReader::ReadRowGroup(size_t i) {
    std::vector<size_t> column_indexes(metadata_.schema.FieldsCount());
    for (size_t col = 0; col < column_indexes.size(); ++col) {
        column_indexes[col] = col;
    }
    return ReadRowGroup(i, column_indexes);
}

core::Batch BruhBatchReader::ReadRowGroup(size_t i, const std::vector<size_t>& column_indexes) {
    if (i >= metadata_.row_groups.size()) {
        THROW_RUNTIME_ERROR("Row group index out of range");
    }

    auto& group = metadata_.row_groups[i];
    auto& schema = metadata_.schema;
    core::Batch batch(ProjectSchema(column_indexes), group.rows_count);
    auto& columns = batch.GetColumns();

    for (size_t out_col = 0; out_col < column_indexes.size(); ++out_col) {
        size_t col = column_indexes[out_col];
        if (col >= schema.FieldsCount()) {
            THROW_RUNTIME_ERROR("Column index out of range");
        }
        auto& chunk = group.columns[col];
        uint64_t mapped_size = chunk.compression == util::Compression::None
                                   ? chunk.uncompressed_size
                                   : chunk.compressed_size;
        CheckMappedRange(chunk.offset, mapped_size, data_.size());
        const uint8_t* chunk_data = data_.data() + static_cast<size_t>(chunk.offset);
        if (chunk.compression == util::Compression::None) {
            util::BufReader r(chunk_data, static_cast<size_t>(chunk.uncompressed_size));
            ReadColumn(r, columns[out_col], schema.GetFields()[col], chunk);
        } else {
            encode_buf_.resize(chunk.uncompressed_size);
            util::Decompress(chunk.compression, chunk_data, chunk.compressed_size,
                             encode_buf_.data(), chunk.uncompressed_size);
            util::BufReader r(encode_buf_.data(), encode_buf_.size());
            ReadColumn(r, columns[out_col], schema.GetFields()[col], chunk);
        }
    }
    return batch;
}

std::vector<size_t> BruhBatchReader::ResolveColumnNames(
    const std::vector<std::string>& column_names) const {
    std::vector<size_t> column_indexes;
    column_indexes.reserve(column_names.size());
    for (auto& column : column_names) {
        column_indexes.push_back(metadata_.schema.GetIndex(column));
    }
    return column_indexes;
}

core::Schema BruhBatchReader::ProjectSchema(const std::vector<size_t>& column_indexes) const {
    std::vector<core::Field> fields;
    fields.reserve(column_indexes.size());
    auto& schema_fields = metadata_.schema.GetFields();
    for (size_t col : column_indexes) {
        if (col >= schema_fields.size()) {
            THROW_RUNTIME_ERROR("Column index out of range");
        }
        fields.push_back(schema_fields[col]);
    }
    return core::Schema(std::move(fields));
}

void BruhBatchReader::ReadMetaData() {
    EnsureBruhFormat();
    size_t footer_size = sizeof(kMagicBytes) + 4;
    CheckMappedRange(data_.size() - footer_size, footer_size, data_.size());
    util::BufReader tail(data_.data() + data_.size() - footer_size, footer_size);
    uint32_t meta_size = tail.Read<uint32_t>();
    if (meta_size > data_.size() - footer_size) {
        THROW_RUNTIME_ERROR("Footer metadata is out of file bounds");
    }
    size_t meta_offset = data_.size() - footer_size - meta_size;
    CheckMappedRange(meta_offset, meta_size, data_.size());
    util::BufReader r(data_.data() + meta_offset, meta_size);
    metadata_.version = r.Read<int>();
    if (metadata_.version != kCurrentVersion) {
        THROW_RUNTIME_ERROR("File version mismatch");
    }
    uint32_t cols_count = r.Read<uint32_t>();
    ReadSchema(r, cols_count);
    ReadRowGroupsMetadata(r, cols_count);
}

void BruhBatchReader::EnsureBruhFormat() {
    size_t footer_size = sizeof(kMagicBytes) + 4;
    if (data_.size() < footer_size) {
        THROW_RUNTIME_ERROR("File is too small");
    }
    if (std::memcmp(data_.data() + data_.size() - sizeof(kMagicBytes), kMagicBytes,
                    sizeof(kMagicBytes)) != 0) {
        THROW_RUNTIME_ERROR("Bad magic bytes");
    }
}

void BruhBatchReader::ReadSchema(util::BufReader& r, uint32_t cols_count) {
    std::vector<core::Field> fields;
    fields.reserve(cols_count);
    for (uint32_t i = 0; i < cols_count; ++i) {
        auto name_len = r.Read<uint32_t>();
        auto name = r.ReadString(name_len);
        auto type = static_cast<core::DataType>(r.Read<uint8_t>());
        auto nullable = r.Read<uint8_t>() != 0;
        fields.emplace_back(name, type, nullable);
    }
    metadata_.schema = core::Schema(std::move(fields));
}

void BruhBatchReader::ReadRowGroupsMetadata(util::BufReader& r, uint32_t cols_count) {
    metadata_.rows_count = r.Read<uint64_t>();
    auto groups_count = r.Read<uint32_t>();
    metadata_.row_groups.reserve(groups_count);
    for (uint32_t i = 0; i < groups_count; ++i) {
        RowGroupMetaData group;
        group.rows_count = r.Read<uint64_t>();
        group.byte_size = r.Read<uint64_t>();
        group.columns.reserve(cols_count);
        for (uint32_t c = 0; c < cols_count; ++c) {
            ColumnChunkMetaData chunk;
            chunk.offset = r.Read<uint64_t>();
            chunk.compressed_size = r.Read<uint64_t>();
            chunk.uncompressed_size = r.Read<uint64_t>();
            chunk.values_count = r.Read<uint64_t>();
            chunk.encoding = static_cast<core::Encoding>(r.Read<uint8_t>());
            chunk.compression = static_cast<util::Compression>(r.Read<uint8_t>());
            group.columns.emplace_back(std::move(chunk));
        }
        metadata_.row_groups.emplace_back(std::move(group));
    }
}

void BruhBatchReader::ReadColumn(util::BufReader& r, std::unique_ptr<core::Column>& col,
                                 const core::Field& field, const ColumnChunkMetaData& chunk) {
    switch (field.type) {
        case core::DataType::Int16:
            col = DecodeNumeric<core::Int16Column, int16_t>(r, field.nullable, chunk.encoding,
                                                            chunk.values_count);
            return;
        case core::DataType::Int32:
            col = DecodeNumeric<core::Int32Column, int32_t>(r, field.nullable, chunk.encoding,
                                                            chunk.values_count);
            return;
        case core::DataType::Int64:
            col = DecodeNumeric<core::Int64Column, int64_t>(r, field.nullable, chunk.encoding,
                                                            chunk.values_count);
            return;
        case core::DataType::Double:
            col = DecodeNumeric<core::DoubleColumn, double>(r, field.nullable, chunk.encoding,
                                                            chunk.values_count);
            return;
        case core::DataType::Date:
            col = DecodeNumeric<core::DateColumn, int32_t>(r, field.nullable, chunk.encoding,
                                                           chunk.values_count);
            return;
        case core::DataType::Timestamp:
            col = DecodeNumeric<core::TimestampColumn, int64_t>(r, field.nullable, chunk.encoding,
                                                                chunk.values_count);
            return;
        case core::DataType::Bool:
            col = DecodeBool(r, field.nullable, chunk.encoding, chunk.values_count);
            return;
        case core::DataType::String:
            col = DecodeString(r, field.nullable, chunk.encoding, chunk.values_count);
            return;
        case core::DataType::Char:
            col = DecodeChar(r, field.nullable, chunk.encoding, chunk.values_count);
            return;
    }
    THROW_RUNTIME_ERROR("Unsupported type");
}
}  // namespace columnar::bruh

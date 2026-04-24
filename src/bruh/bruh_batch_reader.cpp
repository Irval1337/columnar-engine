#include <bruh/bruh_batch_reader.h>
#include <bruh/format.h>
#include <core/columns/date_column.h>
#include <core/columns/timestamp_column.h>
#include <core/encoding/bit_packing.h>
#include <core/encoding/delta.h>
#include <core/encoding/dictionary.h>
#include <core/encoding/frame_of_reference.h>
#include <core/encoding/rle.h>
#include <util/bit_vector.h>
#include <util/stream_helper.h>

#include <cstdint>
#include <cstring>
#include <type_traits>

namespace columnar::bruh {
namespace {
util::BitVector ReadBitVector(std::istream& is, size_t n) {
    size_t packed_size = core::encoding::BitPackedSize(n, 1);
    std::vector<uint8_t> packed(packed_size);
    if (packed_size > 0) {
        util::ReadRaw(is, packed.data(), packed_size);
    }
    return core::encoding::UnpackBitVector(packed.data(), packed_size, n);
}

template <typename T>
std::vector<size_t> ReadOffsets(std::istream& is, size_t n) {
    auto raw = util::ReadArray<T>(is, n + 1);
    return std::vector<size_t>(raw.begin(), raw.end());
}

std::vector<size_t> DecodeOffsetsPlain(std::istream& is, size_t n) {
    auto width = util::Read<uint8_t>(is);
    if (width == 4) {
        return ReadOffsets<uint32_t>(is, n);
    }
    if (width == 8) {
        return ReadOffsets<uint64_t>(is, n);
    }
    THROW_RUNTIME_ERROR("Unsupported string offset width");
}

template <typename Column, typename T>
std::unique_ptr<core::Column> DecodeNumeric(std::istream& is, bool nullable,
                                            core::Encoding encoding, size_t n) {
    util::BitVector is_null;
    if (nullable) {
        is_null = ReadBitVector(is, n);
    }
    switch (encoding) {
        case core::Encoding::Plain: {
            auto data = util::ReadArray<T>(is, n);
            return std::make_unique<Column>(std::move(data), std::move(is_null), nullable);
        }
        case core::Encoding::RLE: {
            auto data = core::encoding::DecodeRLE<T>(is, n);
            return std::make_unique<Column>(std::move(data), std::move(is_null), nullable);
        }
        case core::Encoding::FrameOfReference: {
            if constexpr (std::is_integral_v<T>) {
                auto data = core::encoding::DecodeFOR<T>(is, n);
                return std::make_unique<Column>(std::move(data), std::move(is_null), nullable);
            } else {
                THROW_RUNTIME_ERROR("FrameOfReference needs an integer column");
            }
        }
        case core::Encoding::Delta: {
            if constexpr (std::is_integral_v<T>) {
                auto data = core::encoding::DecodeDelta<T>(is, n);
                return std::make_unique<Column>(std::move(data), std::move(is_null), nullable);
            } else {
                THROW_RUNTIME_ERROR("Delta needs an integer column");
            }
        }
        case core::Encoding::BitPacking: {
            if constexpr (std::is_integral_v<T>) {
                uint8_t bit_width = util::Read<uint8_t>(is);
                if (bit_width > core::encoding::kBitPackingMaxWidth) {
                    THROW_RUNTIME_ERROR("bit_width is too large");
                }
                std::vector<T> data(n);
                if (bit_width == 0 || n == 0) {
                    return std::make_unique<Column>(std::move(data), std::move(is_null), nullable);
                }
                size_t packed_size = core::encoding::BitPackedSize(n, bit_width);
                std::vector<uint8_t> packed(packed_size);
                util::ReadRaw(is, packed.data(), packed_size);
                std::vector<uint64_t> extended(n);
                core::encoding::BitUnpack(packed.data(), packed_size, n, bit_width,
                                          extended.data());
                for (size_t i = 0; i < n; ++i) {
                    data[i] = static_cast<T>(extended[i]);
                }
                return std::make_unique<Column>(std::move(data), std::move(is_null), nullable);
            } else {
                THROW_RUNTIME_ERROR("BitPacking needs an integer column");
            }
        }
        default:
            THROW_RUNTIME_ERROR("Encoding does not support numeric column");
    }
}

std::unique_ptr<core::Column> DecodeBool(std::istream& is, bool nullable, core::Encoding encoding,
                                         size_t n) {
    util::BitVector is_null;
    if (nullable) {
        is_null = ReadBitVector(is, n);
    }
    switch (encoding) {
        case core::Encoding::Plain:
        case core::Encoding::BitPacking: {
            auto data = ReadBitVector(is, n);
            return std::make_unique<core::BoolColumn>(std::move(data), std::move(is_null), nullable,
                                                      n);
        }
        case core::Encoding::RLE: {
            auto data = core::encoding::DecodeBoolRLE(is, n);
            return std::make_unique<core::BoolColumn>(std::move(data), std::move(is_null), nullable,
                                                      n);
        }
        default:
            THROW_RUNTIME_ERROR("Encoding does not support bool column");
    }
}

std::unique_ptr<core::Column> DecodeString(std::istream& is, bool nullable, core::Encoding encoding,
                                           size_t n) {
    util::BitVector is_null;
    if (nullable) {
        is_null = ReadBitVector(is, n);
    }
    switch (encoding) {
        case core::Encoding::Plain: {
            auto offsets = DecodeOffsetsPlain(is, n);
            auto data = util::ReadArray<char>(is, offsets.back());
            return std::make_unique<core::StringColumn>(std::move(data), std::move(offsets),
                                                        std::move(is_null), nullable);
        }
        case core::Encoding::Dictionary: {
            auto decoded = core::encoding::DecodeStringDictionary(is, n);
            return std::make_unique<core::StringColumn>(
                std::move(decoded.data), std::move(decoded.offsets), std::move(is_null), nullable);
        }
        default:
            THROW_RUNTIME_ERROR("Encoding does not support string column");
    }
}

std::unique_ptr<core::Column> DecodeChar(std::istream& is, bool nullable, core::Encoding encoding,
                                         size_t n) {
    util::BitVector is_null;
    if (nullable) {
        is_null = ReadBitVector(is, n);
    }
    switch (encoding) {
        case core::Encoding::Plain: {
            auto data = util::ReadArray<char>(is, n);
            return std::make_unique<core::CharColumn>(std::move(data), std::move(is_null),
                                                      nullable);
        }
        case core::Encoding::RLE: {
            auto data = core::encoding::DecodeRLE<char>(is, n);
            return std::make_unique<core::CharColumn>(std::move(data), std::move(is_null),
                                                      nullable);
        }
        default:
            THROW_RUNTIME_ERROR("Encoding does not support char column");
    }
}

}  // namespace

core::Batch BruhBatchReader::ReadRowGroup(size_t i) {
    if (i >= metadata_.row_groups.size()) {
        THROW_RUNTIME_ERROR("Row group index out of range");
    }

    auto& group = metadata_.row_groups[i];
    auto& schema = metadata_.schema;
    core::Batch batch(metadata_.schema, group.rows_count);
    auto& columns = batch.GetColumns();
    for (size_t col = 0; col < schema.FieldsCount(); ++col) {
        auto& chunk = group.columns[col];
        if (is_.tellg() != static_cast<std::streamoff>(chunk.offset)) {
            is_.seekg(chunk.offset);
        }
        ReadColumn(columns[col], schema.GetFields()[col], chunk);
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
        auto name_len = util::Read<uint32_t>(is_);
        auto name = util::ReadString(is_, name_len);
        auto type = static_cast<core::DataType>(util::Read<uint8_t>(is_));
        auto nullable = util::Read<uint8_t>(is_) != 0;
        fields.emplace_back(name, type, nullable);
    }
    metadata_.schema = core::Schema(std::move(fields));
}

void BruhBatchReader::ReadRowGroupsMetadata(uint32_t cols_count) {
    metadata_.rows_count = util::Read<uint64_t>(is_);
    auto groups_count = util::Read<uint32_t>(is_);
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
            chunk.encoding = static_cast<core::Encoding>(util::Read<uint8_t>(is_));
            group.columns.emplace_back(std::move(chunk));
        }
        metadata_.row_groups.emplace_back(std::move(group));
    }
}

void BruhBatchReader::ReadColumn(std::unique_ptr<core::Column>& col, const core::Field& field,
                                 const ColumnChunkMetaData& chunk) {
    switch (field.type) {
        case core::DataType::Int16:
            col = DecodeNumeric<core::Int16Column, int16_t>(is_, field.nullable, chunk.encoding,
                                                            chunk.values_count);
            return;
        case core::DataType::Int32:
            col = DecodeNumeric<core::Int32Column, int32_t>(is_, field.nullable, chunk.encoding,
                                                            chunk.values_count);
            return;
        case core::DataType::Int64:
            col = DecodeNumeric<core::Int64Column, int64_t>(is_, field.nullable, chunk.encoding,
                                                            chunk.values_count);
            return;
        case core::DataType::Double:
            col = DecodeNumeric<core::DoubleColumn, double>(is_, field.nullable, chunk.encoding,
                                                            chunk.values_count);
            return;
        case core::DataType::Date:
            col = DecodeNumeric<core::DateColumn, int32_t>(is_, field.nullable, chunk.encoding,
                                                           chunk.values_count);
            return;
        case core::DataType::Timestamp:
            col = DecodeNumeric<core::TimestampColumn, int64_t>(is_, field.nullable, chunk.encoding,
                                                                chunk.values_count);
            return;
        case core::DataType::Bool:
            col = DecodeBool(is_, field.nullable, chunk.encoding, chunk.values_count);
            return;
        case core::DataType::String:
            col = DecodeString(is_, field.nullable, chunk.encoding, chunk.values_count);
            return;
        case core::DataType::Char:
            col = DecodeChar(is_, field.nullable, chunk.encoding, chunk.values_count);
            return;
    }
    THROW_RUNTIME_ERROR("Unsupported type");
}
}  // namespace columnar::bruh

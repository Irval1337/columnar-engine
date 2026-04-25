#include <bruh/bruh_batch_writer.h>
#include <core/encoding/auto_select.h>
#include <core/encoding/bit_packing.h>
#include <core/encoding/delta.h>
#include <core/encoding/dictionary.h>
#include <core/encoding/frame_of_reference.h>
#include <core/encoding/rle.h>
#include <util/macro.h>
#include <util/stream_helper.h>

#include <cstdint>
#include <limits>
#include <type_traits>

namespace columnar::bruh {
namespace {
void WriteBitVector(std::ostream& os, const util::BitVector& bits) {
    size_t packed = core::encoding::BitPackedSize(bits.Size(), 1);
    if (packed > 0) {
        util::WriteRaw(os, bits.GetData().data(), packed);
    }
}

void ValidateSchema(const core::Schema& writer_schema, const core::Schema& batch_schema) {
    if (writer_schema.FieldsCount() != batch_schema.FieldsCount()) {
        THROW_RUNTIME_ERROR("Batch schema does not match writer schema");
    }
    auto& expected = writer_schema.GetFields();
    auto& curr = batch_schema.GetFields();
    for (size_t i = 0; i < expected.size(); ++i) {
        if (expected[i].name != curr[i].name || expected[i].type != curr[i].type ||
            expected[i].nullable != curr[i].nullable) {
            THROW_RUNTIME_ERROR("Batch schema does not match writer schema");
        }
    }
}

template <typename T>
void WriteOffsets(std::ostream& os, const std::vector<size_t>& offsets) {
    std::vector<T> out(offsets.begin(), offsets.end());
    util::WriteArray(os, out);
}

void EncodeOffsetsPlain(std::ostream& os, const std::vector<size_t>& offsets, size_t data_size) {
    if (data_size <= std::numeric_limits<uint32_t>::max()) {
        util::Write<uint8_t>(os, 4);
        WriteOffsets<uint32_t>(os, offsets);
    } else {
        util::Write<uint8_t>(os, 8);
        WriteOffsets<uint64_t>(os, offsets);
    }
}

template <typename ColumnT>
void EncodeNumeric(std::ostream& os, const core::Column& col, bool nullable,
                   core::Encoding encoding, const core::encoding::AutoEncoding& auto_encoding) {
    auto& numeric = static_cast<const ColumnT&>(col);
    if (nullable) {
        WriteBitVector(os, numeric.GetNullMask());
    }
    using ValueT = std::remove_cvref_t<decltype(numeric.GetData())>::value_type;
    switch (encoding) {
        case core::Encoding::Plain:
            util::WriteArray(os, numeric.GetData());
            return;
        case core::Encoding::RLE:
            core::encoding::EncodeRLE<ValueT>(os, numeric.GetData().data(),
                                              numeric.GetData().size());
            return;
        case core::Encoding::FrameOfReference:
            if constexpr (std::is_integral_v<ValueT>) {
                auto& data = numeric.GetData();
                if (auto_encoding.has_int_stats) {
                    core::encoding::EncodeFOR<ValueT>(os, data.data(), data.size(),
                                                      static_cast<ValueT>(auto_encoding.mn),
                                                      static_cast<ValueT>(auto_encoding.mx));
                } else {
                    core::encoding::EncodeFOR<ValueT>(os, data.data(), data.size());
                }
                return;
            }
            THROW_RUNTIME_ERROR("FrameOfReference needs an integer column");
        case core::Encoding::Delta:
            if constexpr (std::is_integral_v<ValueT>) {
                auto& data = numeric.GetData();
                if (auto_encoding.has_int_stats) {
                    core::encoding::EncodeDelta<ValueT>(
                        os, data.data(), data.size(), static_cast<ValueT>(auto_encoding.min_delta),
                        static_cast<ValueT>(auto_encoding.max_delta));
                } else {
                    core::encoding::EncodeDelta<ValueT>(os, data.data(), data.size());
                }
                return;
            }
            THROW_RUNTIME_ERROR("Delta needs an integer column");
        case core::Encoding::BitPacking:
            if constexpr (std::is_integral_v<ValueT>) {
                auto& data = numeric.GetData();
                uint64_t mx = 0;
                for (auto v : data) {
                    if (v < 0) {
                        THROW_RUNTIME_ERROR("BitPacking needs only positive values");
                    }
                    uint64_t u = static_cast<uint64_t>(v);
                    if (u > mx) {
                        mx = u;
                    }
                }
                uint8_t bit_width = core::encoding::BitWidth(mx);
                if (bit_width > core::encoding::kBitPackingMaxWidth) {
                    THROW_RUNTIME_ERROR("bit_width is too large");
                }
                util::Write<uint8_t>(os, bit_width);
                if (bit_width == 0 || data.empty()) {
                    return;
                }
                std::vector<uint8_t> packed(core::encoding::BitPackedSize(data.size(), bit_width));
                core::encoding::BitPackWithOffset(data.data(), data.size(), ValueT(0), bit_width,
                                                  packed.data());
                util::WriteRaw(os, packed.data(), packed.size());
                return;
            }
            THROW_RUNTIME_ERROR("BitPacking needs an integer column");
        default:
            THROW_RUNTIME_ERROR("Encoding does not support numeric column");
    }
}

void EncodeBool(std::ostream& os, const core::Column& col, bool nullable, core::Encoding encoding) {
    auto& boolcol = static_cast<const core::BoolColumn&>(col);
    if (nullable) {
        WriteBitVector(os, boolcol.GetNullMask());
    }
    switch (encoding) {
        case core::Encoding::Plain:
        case core::Encoding::BitPacking:
            WriteBitVector(os, boolcol.GetData());
            return;
        case core::Encoding::RLE:
            core::encoding::EncodeBoolRLE(os, boolcol.GetData(), boolcol.Size());
            return;
        default:
            THROW_RUNTIME_ERROR("Encoding does not support bool column");
    }
}

void EncodeString(std::ostream& os, const core::Column& col, bool nullable, core::Encoding encoding,
                  const core::encoding::AutoEncoding& auto_encoding) {
    auto& stringcol = static_cast<const core::StringColumn&>(col);
    if (nullable) {
        WriteBitVector(os, stringcol.GetNullMask());
    }
    switch (encoding) {
        case core::Encoding::Plain: {
            auto& data = stringcol.GetData();
            EncodeOffsetsPlain(os, stringcol.GetOffsets(), data.size());
            util::WriteArray(os, data);
            return;
        }
        case core::Encoding::Dictionary:
            if (!auto_encoding.dict_values.empty()) {
                core::encoding::EncodeStringDictionary(os, auto_encoding.dict_values,
                                                       auto_encoding.dict_indexes);
            } else {
                core::encoding::EncodeStringDictionary(os, stringcol.GetData(),
                                                       stringcol.GetOffsets());
            }
            return;
        default:
            THROW_RUNTIME_ERROR("Encoding does not support string column");
    }
}

void EncodeChar(std::ostream& os, const core::Column& col, bool nullable, core::Encoding encoding) {
    auto& charcol = static_cast<const core::CharColumn&>(col);
    if (nullable) {
        WriteBitVector(os, charcol.GetNullMask());
    }
    switch (encoding) {
        case core::Encoding::Plain:
            util::WriteArray(os, charcol.GetData());
            return;
        case core::Encoding::RLE:
            core::encoding::EncodeRLE<char>(os, charcol.GetData().data(), charcol.GetData().size());
            return;
        default:
            THROW_RUNTIME_ERROR("Encoding does not support char column");
    }
}

void EncodeColumn(std::ostream& os, const core::Column& col, const core::Field& field,
                  core::Encoding encoding, const core::encoding::AutoEncoding& auto_encoding) {
    switch (core::DataTypeToPhysical(field.type)) {
        case core::PhysicalType::Int16:
            EncodeNumeric<core::Int16Column>(os, col, field.nullable, encoding, auto_encoding);
            return;
        case core::PhysicalType::Int32:
            EncodeNumeric<core::Int32Column>(os, col, field.nullable, encoding, auto_encoding);
            return;
        case core::PhysicalType::Int64:
            EncodeNumeric<core::Int64Column>(os, col, field.nullable, encoding, auto_encoding);
            return;
        case core::PhysicalType::Double:
            EncodeNumeric<core::DoubleColumn>(os, col, field.nullable, encoding, auto_encoding);
            return;
        case core::PhysicalType::Bool:
            EncodeBool(os, col, field.nullable, encoding);
            return;
        case core::PhysicalType::String:
            EncodeString(os, col, field.nullable, encoding, auto_encoding);
            return;
        case core::PhysicalType::Char:
            EncodeChar(os, col, field.nullable, encoding);
            return;
    }
    THROW_RUNTIME_ERROR("Unsupported type");
}
}  // namespace

void BruhBatchWriter::Write(const core::Batch& batch) {
    ValidateSchema(schema_, batch.GetSchema());

    RowGroupMetaData group;
    group.rows_count = batch.RowsCount();
    group.byte_size = 0;
    metadata_.rows_count += batch.RowsCount();
    for (size_t col = 0; col < batch.ColumnsCount(); ++col) {
        ColumnChunkMetaData chunk;
        chunk.values_count = batch.RowsCount();
        WriteColumn(chunk, batch.ColumnAt(col), schema_.GetFields()[col], col);
        group.byte_size += chunk.byte_size;
        group.columns.push_back(chunk);
    }
    metadata_.row_groups.push_back(std::move(group));
}

void BruhBatchWriter::WriteColumn(ColumnChunkMetaData& chunk, const core::Column& col,
                                  const core::Field& field, size_t col_index) {
    core::encoding::AutoEncoding auto_encoding;
    core::Encoding encoding;

    auto it = options_.per_column.find(col_index);
    if (it != options_.per_column.end()) {
        encoding = it->second;
    } else if (options_.force_encoding.has_value()) {
        encoding = *options_.force_encoding;
    } else {
        auto_encoding = core::encoding::SelectEncoding(col, field);
        encoding = auto_encoding.encoding;
    }

    chunk.encoding = encoding;
    chunk.offset = static_cast<uint64_t>(os_.tellp());
    EncodeColumn(os_, col, field, encoding, auto_encoding);
    chunk.byte_size = static_cast<uint64_t>(os_.tellp()) - chunk.offset;
}

void BruhBatchWriter::Flush() {
    auto footer_pos = os_.tellp();
    WriteFooter();
    util::Write<uint32_t>(os_, os_.tellp() - footer_pos);
    WriteMagic();
    os_.flush();
}

void BruhBatchWriter::WriteFooter() {
    util::Write<int>(os_, metadata_.version);
    WriteFields();
    util::Write<uint64_t>(os_, metadata_.rows_count);
    WriteRowGroups();
}

void BruhBatchWriter::WriteFields() {
    auto& fields = metadata_.schema.GetFields();
    util::Write<uint32_t>(os_, fields.size());
    for (auto& f : fields) {
        util::Write<uint32_t>(os_, f.name.size());
        util::WriteString(os_, f.name);
        util::Write<uint8_t>(os_, static_cast<uint8_t>(f.type));
        util::Write<uint8_t>(os_, f.nullable ? 1 : 0);
    }
}

void BruhBatchWriter::WriteRowGroups() {
    util::Write<uint32_t>(os_, metadata_.row_groups.size());
    for (auto& group : metadata_.row_groups) {
        util::Write<uint64_t>(os_, group.rows_count);
        util::Write<uint64_t>(os_, group.byte_size);
        for (auto& chunk : group.columns) {
            util::Write<uint64_t>(os_, chunk.offset);
            util::Write<uint64_t>(os_, chunk.byte_size);
            util::Write<uint64_t>(os_, chunk.values_count);
            util::Write<uint8_t>(os_, static_cast<uint8_t>(chunk.encoding));
        }
    }
}
}  // namespace columnar::bruh

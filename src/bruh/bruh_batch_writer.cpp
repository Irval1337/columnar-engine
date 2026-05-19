#include <bruh/bruh_batch_writer.h>
#include <core/columns/dictionary_string_column.h>
#include <core/encoding/auto_select.h>
#include <core/encoding/bit_packing.h>
#include <core/encoding/delta.h>
#include <core/encoding/dictionary.h>
#include <core/encoding/frame_of_reference.h>
#include <core/encoding/rle.h>
#include <util/byte_buffer.h>
#include <util/compression.h>
#include <util/macro.h>
#include <util/stream_helper.h>

#include <cstdint>
#include <limits>
#include <string_view>
#include <type_traits>
#include <vector>

namespace columnar::bruh {
namespace {
void WriteBitVector(util::BufWriter& w, const util::BitVector& bits) {
    size_t packed = core::encoding::BitPackedSize(bits.Size(), 1);
    if (packed > 0) {
        w.WriteRaw(bits.GetData().data(), packed);
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
void WriteOffsets(util::BufWriter& w, const std::vector<size_t>& offsets) {
    std::vector<T> out(offsets.begin(), offsets.end());
    w.WriteArray(out);
}

void EncodeOffsetsPlain(util::BufWriter& w, const std::vector<size_t>& offsets, size_t data_size) {
    if (data_size <= std::numeric_limits<uint32_t>::max()) {
        w.Write<uint8_t>(4);
        WriteOffsets<uint32_t>(w, offsets);
    } else {
        w.Write<uint8_t>(8);
        WriteOffsets<uint64_t>(w, offsets);
    }
}

template <typename ColumnT>
void EncodeNumeric(util::BufWriter& w, const core::Column& col, bool nullable,
                   core::Encoding encoding, const core::encoding::AutoEncoding& auto_encoding,
                   std::vector<uint8_t>& packed_buf) {
    auto& numeric = static_cast<const ColumnT&>(col);
    if (nullable) {
        WriteBitVector(w, numeric.GetNullMask());
    }
    using ValueT = std::remove_cvref_t<decltype(numeric.GetData())>::value_type;
    switch (encoding) {
        case core::Encoding::Plain:
            w.WriteArray(numeric.GetData());
            return;
        case core::Encoding::RLE:
            core::encoding::EncodeRLE<ValueT>(w, numeric.GetData().data(),
                                              numeric.GetData().size());
            return;
        case core::Encoding::FrameOfReference:
            if constexpr (std::is_integral_v<ValueT>) {
                auto& data = numeric.GetData();
                if (auto_encoding.has_int_stats) {
                    core::encoding::EncodeFOR<ValueT>(
                        w, data.data(), data.size(), static_cast<ValueT>(auto_encoding.mn),
                        static_cast<ValueT>(auto_encoding.mx), packed_buf);
                } else {
                    core::encoding::EncodeFOR<ValueT>(w, data.data(), data.size());
                }
                return;
            }
            THROW_RUNTIME_ERROR("FrameOfReference needs an integer column");
        case core::Encoding::Delta:
            if constexpr (std::is_integral_v<ValueT>) {
                auto& data = numeric.GetData();
                if (auto_encoding.has_int_stats) {
                    core::encoding::EncodeDelta<ValueT>(
                        w, data.data(), data.size(), static_cast<ValueT>(auto_encoding.min_delta),
                        static_cast<ValueT>(auto_encoding.max_delta), packed_buf);
                } else {
                    core::encoding::EncodeDelta<ValueT>(w, data.data(), data.size());
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
                w.Write<uint8_t>(bit_width);
                if (bit_width == 0 || data.empty()) {
                    return;
                }
                size_t packed_size = core::encoding::BitPackedSize(data.size(), bit_width);
                packed_buf.resize(packed_size);
                core::encoding::BitPackWithOffset(data.data(), data.size(), ValueT(0), bit_width,
                                                  packed_buf.data());
                w.WriteRaw(packed_buf.data(), packed_size);
                return;
            }
            THROW_RUNTIME_ERROR("BitPacking needs an integer column");
        default:
            THROW_RUNTIME_ERROR("Encoding does not support numeric column");
    }
}

void EncodeBool(util::BufWriter& w, const core::Column& col, bool nullable,
                core::Encoding encoding) {
    auto& boolcol = static_cast<const core::BoolColumn&>(col);
    if (nullable) {
        WriteBitVector(w, boolcol.GetNullMask());
    }
    switch (encoding) {
        case core::Encoding::Plain:
        case core::Encoding::BitPacking:
            WriteBitVector(w, boolcol.GetData());
            return;
        case core::Encoding::RLE:
            core::encoding::EncodeBoolRLE(w, boolcol.GetData(), boolcol.Size());
            return;
        default:
            THROW_RUNTIME_ERROR("Encoding does not support bool column");
    }
}

void EncodeString(util::BufWriter& w, const core::Column& col, bool nullable,
                  core::Encoding encoding, const core::encoding::AutoEncoding& auto_encoding) {
    auto* stringcol = dynamic_cast<const core::StringColumn*>(&col);
    auto* dictcol = dynamic_cast<const core::DictionaryStringColumn*>(&col);
    if (stringcol == nullptr && dictcol == nullptr) {
        THROW_RUNTIME_ERROR("Encoding string column expected a string implementation");
    }
    if (nullable) {
        WriteBitVector(w, stringcol != nullptr ? stringcol->GetNullMask() : dictcol->GetNullMask());
    }
    switch (encoding) {
        case core::Encoding::Plain: {
            if (stringcol != nullptr) {
                auto& data = stringcol->GetData();
                EncodeOffsetsPlain(w, stringcol->GetOffsets(), data.size());
                w.WriteArray(data);
            } else {
                std::vector<char> data;
                std::vector<size_t> offsets;
                offsets.reserve(dictcol->Size() + 1);
                offsets.push_back(0);
                for (size_t i = 0; i < dictcol->Size(); ++i) {
                    if (!dictcol->IsNull(i)) {
                        auto value = dictcol->Get(i);
                        data.insert(data.end(), value.begin(), value.end());
                    }
                    offsets.push_back(data.size());
                }
                EncodeOffsetsPlain(w, offsets, data.size());
                w.WriteArray(data);
            }
            return;
        }
        case core::Encoding::Dictionary:
            if (!auto_encoding.dict_values.empty()) {
                core::encoding::EncodeStringDictionary(w, auto_encoding.dict_values,
                                                       auto_encoding.dict_indexes);
            } else if (dictcol != nullptr) {
                std::vector<std::string_view> dict_values;
                dict_values.reserve(dictcol->DictSize());
                for (uint32_t id = 0; id < dictcol->DictSize(); ++id) {
                    dict_values.push_back(dictcol->DictValue(id));
                }
                core::encoding::EncodeStringDictionary(w, dict_values, dictcol->GetIds());
            } else {
                core::encoding::EncodeStringDictionary(w, stringcol->GetData(),
                                                       stringcol->GetOffsets());
            }
            return;
        default:
            THROW_RUNTIME_ERROR("Encoding does not support string column");
    }
}

void EncodeChar(util::BufWriter& w, const core::Column& col, bool nullable,
                core::Encoding encoding) {
    auto& charcol = static_cast<const core::CharColumn&>(col);
    if (nullable) {
        WriteBitVector(w, charcol.GetNullMask());
    }
    switch (encoding) {
        case core::Encoding::Plain:
            w.WriteArray(charcol.GetData());
            return;
        case core::Encoding::RLE:
            core::encoding::EncodeRLE<char>(w, charcol.GetData().data(), charcol.GetData().size());
            return;
        default:
            THROW_RUNTIME_ERROR("Encoding does not support char column");
    }
}

void EncodeColumn(util::BufWriter& w, const core::Column& col, const core::Field& field,
                  core::Encoding encoding, const core::encoding::AutoEncoding& auto_encoding,
                  std::vector<uint8_t>& packed_buf) {
    switch (core::DataTypeToPhysical(field.type)) {
        case core::PhysicalType::Int16:
            EncodeNumeric<core::Int16Column>(w, col, field.nullable, encoding, auto_encoding,
                                             packed_buf);
            return;
        case core::PhysicalType::Int32:
            EncodeNumeric<core::Int32Column>(w, col, field.nullable, encoding, auto_encoding,
                                             packed_buf);
            return;
        case core::PhysicalType::Int64:
            EncodeNumeric<core::Int64Column>(w, col, field.nullable, encoding, auto_encoding,
                                             packed_buf);
            return;
        case core::PhysicalType::Double:
            EncodeNumeric<core::DoubleColumn>(w, col, field.nullable, encoding, auto_encoding,
                                              packed_buf);
            return;
        case core::PhysicalType::Bool:
            EncodeBool(w, col, field.nullable, encoding);
            return;
        case core::PhysicalType::String:
            EncodeString(w, col, field.nullable, encoding, auto_encoding);
            return;
        case core::PhysicalType::Char:
            EncodeChar(w, col, field.nullable, encoding);
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
        group.byte_size += chunk.compressed_size;
        group.columns.push_back(chunk);
    }
    metadata_.row_groups.push_back(std::move(group));
}

void BruhBatchWriter::WriteColumn(ColumnChunkMetaData& chunk, const core::Column& col,
                                  const core::Field& field, size_t col_index) {
    core::Encoding encoding = options_.encoding;
    if (auto it = options_.column_encoding.find(col_index); it != options_.column_encoding.end()) {
        encoding = it->second;
    }
    core::encoding::AutoEncoding auto_encoding;
    if (encoding == core::Encoding::Auto) {
        auto_encoding = core::encoding::SelectEncoding(col, field);
        encoding = auto_encoding.encoding;
    }

    util::Compression compression = options_.compression;
    if (auto it = options_.column_compression.find(col_index);
        it != options_.column_compression.end()) {
        compression = it->second;
    }

    encode_buf_.clear();
    util::BufWriter encode_writer(encode_buf_);
    EncodeColumn(encode_writer, col, field, encoding, auto_encoding, packed_buf_);

    const uint8_t* out = encode_buf_.data();
    size_t out_size = encode_buf_.size();
    chunk.compression = util::Compression::None;
    if (compression != util::Compression::None) {
        compress_buf_.clear();
        util::Compress(compression, encode_buf_.data(), encode_buf_.size(), compress_buf_);
        if (compress_buf_.size() < encode_buf_.size()) {
            out = compress_buf_.data();
            out_size = compress_buf_.size();
            chunk.compression = compression;
        }
    }

    chunk.encoding = encoding;
    chunk.uncompressed_size = encode_buf_.size();
    chunk.offset = static_cast<uint64_t>(os_.tellp());
    chunk.compressed_size = out_size;
    util::WriteRaw(os_, out, out_size);
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
            util::Write<uint64_t>(os_, chunk.compressed_size);
            util::Write<uint64_t>(os_, chunk.uncompressed_size);
            util::Write<uint64_t>(os_, chunk.values_count);
            util::Write<uint8_t>(os_, static_cast<uint8_t>(chunk.encoding));
            util::Write<uint8_t>(os_, static_cast<uint8_t>(chunk.compression));
        }
    }
}
}  // namespace columnar::bruh

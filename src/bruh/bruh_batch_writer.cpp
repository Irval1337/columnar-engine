#include <bruh/bruh_batch_writer.h>
#include <core/columns/bool_column.h>
#include <core/columns/char_column.h>
#include <core/columns/dictionary_string_column.h>
#include <core/columns/string_column.h>
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
#include <cmath>
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
void UpdateMinMax(T value, T& mn, T& mx, bool& first) {
    if (first) {
        mn = value;
        mx = value;
        first = false;
        return;
    }
    if (value < mn) {
        mn = value;
    }
    if (value > mx) {
        mx = value;
    }
}

template <typename ColumnT>
ColumnChunkStatistics BuildIntStatistics(const core::Column& col,
                                         const core::encoding::AutoEncoding& auto_encoding) {
    auto& numeric = static_cast<const ColumnT&>(col);
    ColumnChunkStatistics statistics;
    statistics.present = true;
    if (numeric.IsNullable()) {
        statistics.nulls_count = numeric.GetNullMask().PopCount();
    }

    if (statistics.nulls_count == 0 && auto_encoding.has_int_stats && numeric.Size() > 0) {
        statistics.has_min_max = true;
        statistics.min_int = auto_encoding.mn;
        statistics.max_int = auto_encoding.mx;
        return statistics;
    }

    int64_t mn = 0;
    int64_t mx = 0;
    bool first = true;
    for (size_t i = 0; i < numeric.Size(); ++i) {
        if (numeric.IsNull(i)) {
            continue;
        }
        UpdateMinMax(static_cast<int64_t>(numeric.Get(i)), mn, mx, first);
    }
    statistics.has_min_max = !first;
    statistics.min_int = mn;
    statistics.max_int = mx;
    return statistics;
}

ColumnChunkStatistics BuildDoubleStatistics(const core::Column& col) {
    auto& numeric = static_cast<const core::DoubleColumn&>(col);
    ColumnChunkStatistics statistics;
    statistics.present = true;
    if (numeric.IsNullable()) {
        statistics.nulls_count = numeric.GetNullMask().PopCount();
    }

    double mn = 0;
    double mx = 0;
    bool first = true;
    for (size_t i = 0; i < numeric.Size(); ++i) {
        if (numeric.IsNull(i)) {
            continue;
        }
        double value = numeric.Get(i);
        if (std::isnan(value)) {
            continue;
        }
        UpdateMinMax(value, mn, mx, first);
    }
    statistics.has_min_max = !first;
    statistics.min_double = mn;
    statistics.max_double = mx;
    return statistics;
}

ColumnChunkStatistics BuildBoolStatistics(const core::Column& col) {
    auto& bool_col = static_cast<const core::BoolColumn&>(col);
    ColumnChunkStatistics statistics;
    statistics.present = true;
    if (bool_col.IsNullable()) {
        statistics.nulls_count = bool_col.GetNullMask().PopCount();
    }

    int64_t mn = 0;
    int64_t mx = 0;
    bool first = true;
    for (size_t i = 0; i < bool_col.Size(); ++i) {
        if (bool_col.IsNull(i)) {
            continue;
        }
        UpdateMinMax(static_cast<int64_t>(bool_col.Get(i) ? 1 : 0), mn, mx, first);
    }
    statistics.has_min_max = !first;
    statistics.min_int = mn;
    statistics.max_int = mx;
    return statistics;
}

ColumnChunkStatistics BuildCharStatistics(const core::Column& col) {
    auto& char_col = static_cast<const core::CharColumn&>(col);
    ColumnChunkStatistics statistics;
    statistics.present = true;
    if (char_col.IsNullable()) {
        statistics.nulls_count = char_col.GetNullMask().PopCount();
    }

    int64_t mn = 0;
    int64_t mx = 0;
    bool first = true;
    for (size_t i = 0; i < char_col.Size(); ++i) {
        if (char_col.IsNull(i)) {
            continue;
        }
        UpdateMinMax(static_cast<int64_t>(char_col.Get(i)), mn, mx, first);
    }
    statistics.has_min_max = !first;
    statistics.min_int = mn;
    statistics.max_int = mx;
    return statistics;
}

std::string_view ReadString(const core::Column& col, size_t row) {
    if (auto* string_col = core::AsString(col)) {
        return string_col->Get(row);
    }
    if (auto* dict_col = core::AsDictionaryString(col)) {
        return dict_col->Get(row);
    }
    THROW_RUNTIME_ERROR("Expected string column");
}

ColumnChunkStatistics BuildStringStatistics(const core::Column& col) {
    ColumnChunkStatistics statistics;
    statistics.present = true;
    if (col.IsNullable()) {
        if (auto* string_col = core::AsString(col)) {
            statistics.nulls_count = string_col->GetNullMask().PopCount();
        } else {
            auto& dict_col = static_cast<const core::DictionaryStringColumn&>(col);
            statistics.nulls_count = dict_col.GetNullMask().PopCount();
        }
    }

    std::string_view mn;
    std::string_view mx;
    bool first = true;
    for (size_t i = 0; i < col.Size(); ++i) {
        if (col.IsNull(i)) {
            continue;
        }
        auto value = ReadString(col, i);
        UpdateMinMax(value, mn, mx, first);
    }
    statistics.has_min_max = !first;
    if (statistics.has_min_max) {
        statistics.min_string.assign(mn.data(), mn.size());
        statistics.max_string.assign(mx.data(), mx.size());
    }
    return statistics;
}

ColumnChunkStatistics BuildStatistics(const core::Column& col, const core::Field& field,
                                      const core::encoding::AutoEncoding& auto_encoding) {
    switch (core::DataTypeToPhysical(field.type)) {
        case core::PhysicalType::Int16:
            return BuildIntStatistics<core::Int16Column>(col, auto_encoding);
        case core::PhysicalType::Int32:
            return BuildIntStatistics<core::Int32Column>(col, auto_encoding);
        case core::PhysicalType::Int64:
            return BuildIntStatistics<core::Int64Column>(col, auto_encoding);
        case core::PhysicalType::Double:
            return BuildDoubleStatistics(col);
        case core::PhysicalType::Bool:
            return BuildBoolStatistics(col);
        case core::PhysicalType::String:
            return BuildStringStatistics(col);
        case core::PhysicalType::Char:
            return BuildCharStatistics(col);
    }
    THROW_RUNTIME_ERROR("Unsupported type");
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
    auto* stringcol = core::AsString(col);
    auto* dictcol = core::AsDictionaryString(col);
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

void WriteColumnStatistics(util::BufWriter& w, const ColumnChunkStatistics& statistics,
                           core::PhysicalType physical_type) {
    w.Write<uint8_t>(statistics.present ? 1 : 0);
    if (!statistics.present) {
        return;
    }
    w.Write<uint8_t>(statistics.has_min_max ? 1 : 0);
    w.Write<uint64_t>(statistics.nulls_count);
    if (!statistics.has_min_max) {
        return;
    }
    switch (physical_type) {
        case core::PhysicalType::Double:
            w.Write<double>(statistics.min_double);
            w.Write<double>(statistics.max_double);
            break;
        case core::PhysicalType::String:
            w.Write<uint32_t>(static_cast<uint32_t>(statistics.min_string.size()));
            w.WriteString(statistics.min_string);
            w.Write<uint32_t>(static_cast<uint32_t>(statistics.max_string.size()));
            w.WriteString(statistics.max_string);
            break;
        default:
            w.Write<int64_t>(statistics.min_int);
            w.Write<int64_t>(statistics.max_int);
            break;
    }
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

    chunk.statistics = BuildStatistics(col, field, auto_encoding);

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
    std::vector<uint8_t> statistics_payload;
    for (auto& group : metadata_.row_groups) {
        util::Write<uint64_t>(os_, group.rows_count);
        util::Write<uint64_t>(os_, group.byte_size);
        for (size_t col = 0; col < group.columns.size(); ++col) {
            auto& chunk = group.columns[col];
            util::Write<uint64_t>(os_, chunk.offset);
            util::Write<uint64_t>(os_, chunk.compressed_size);
            util::Write<uint64_t>(os_, chunk.uncompressed_size);
            util::Write<uint64_t>(os_, chunk.values_count);
            util::Write<uint8_t>(os_, static_cast<uint8_t>(chunk.encoding));
            util::Write<uint8_t>(os_, static_cast<uint8_t>(chunk.compression));
            if (!chunk.statistics.has_value()) {
                THROW_RUNTIME_ERROR("Column statistics is missing");
            }
            statistics_payload.clear();
            util::BufWriter statistics_writer(statistics_payload);
            WriteColumnStatistics(statistics_writer, *chunk.statistics,
                                  core::DataTypeToPhysical(metadata_.schema.GetFields()[col].type));
            if (statistics_payload.size() > std::numeric_limits<uint32_t>::max()) {
                THROW_RUNTIME_ERROR("Column statistics payload is too large");
            }
            util::Write<uint32_t>(os_, static_cast<uint32_t>(statistics_payload.size()));
            util::WriteRaw(os_, statistics_payload.data(), statistics_payload.size());
        }
    }
}
}  // namespace columnar::bruh

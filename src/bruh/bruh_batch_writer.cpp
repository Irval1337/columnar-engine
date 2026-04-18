#include <bruh/bruh_batch_writer.h>
#include <core/column_factory.h>
#include <util/macro.h>
#include <util/stream_helper.h>

#include <cstdint>
#include <limits>

namespace columnar::bruh {
namespace {
void WriteOffsets(std::ostream& os, const std::vector<std::size_t>& offsets) {
    std::vector<uint32_t> offsets32;
    offsets32.reserve(offsets.size());
    for (auto value : offsets) {
        offsets32.push_back(static_cast<uint32_t>(value));
    }
    util::WriteArray(os, offsets32);
}

void ValidateSchema(const core::Schema& writer_schema, const core::Schema& batch_schema) {
    if (writer_schema.FieldsCount() != batch_schema.FieldsCount()) {
        THROW_RUNTIME_ERROR("Batch schema does not match writer schema");
    }

    auto& expected = writer_schema.GetFields();
    auto& curr = batch_schema.GetFields();
    for (std::size_t i = 0; i < expected.size(); ++i) {
        if (expected[i].name != curr[i].name || expected[i].type != curr[i].type ||
            expected[i].nullable != curr[i].nullable) {
            THROW_RUNTIME_ERROR("Batch schema does not match writer schema");
        }
    }
}
}  // namespace

void BruhBatchWriter::Write(const core::Batch& batch) {
    ValidateSchema(schema_, batch.GetSchema());

    RowGroupMetaData group;
    group.rows_count = batch.RowsCount();
    group.byte_size = 0;
    metadata_.rows_count += batch.RowsCount();
    for (std::size_t col = 0; col < batch.ColumnsCount(); ++col) {
        ColumnChunkMetaData chunk;
        chunk.offset = os_.tellp();
        chunk.values_count = batch.RowsCount();
        WriteColumn(batch.ColumnAt(col), schema_.GetFields()[col]);
        chunk.byte_size = static_cast<uint64_t>(os_.tellp()) - chunk.offset;
        group.byte_size += chunk.byte_size;
        group.columns.push_back(chunk);
    }
    metadata_.row_groups.push_back(std::move(group));
}

void BruhBatchWriter::Flush() {
    auto footer_pos = os_.tellp();
    WriteFooter();
    util::Write<uint32_t>(os_, os_.tellp() - footer_pos);
    WriteMagic();
    os_.flush();
}

void BruhBatchWriter::WriteColumn(const core::Column& col, const core::Field& field) {
    switch (field.type) {
        case core::DataType::Int64: {
            const auto& int64col = static_cast<const core::Int64Column&>(col);
            if (field.nullable) {
                util::WriteBoolArray(os_, int64col.GetNullMask().GetData());
            }
            util::WriteArray(os_, int64col.GetData());
            break;
        }
        case core::DataType::Double: {
            const auto& doublecol = static_cast<const core::DoubleColumn&>(col);
            if (field.nullable) {
                util::WriteBoolArray(os_, doublecol.GetNullMask().GetData());
            }
            util::WriteArray(os_, doublecol.GetData());
            break;
        }
        case core::DataType::Bool: {
            const auto& boolcol = static_cast<const core::BoolColumn&>(col);
            if (field.nullable) {
                util::WriteBoolArray(os_, boolcol.GetNullMask().GetData());
            }
            util::WriteBoolArray(os_, boolcol.GetData().GetData());
            break;
        }
        case core::DataType::String: {
            const auto& stringcol = static_cast<const core::StringColumn&>(col);
            if (field.nullable) {
                util::WriteBoolArray(os_, stringcol.GetNullMask().GetData());
            }
            auto& data = stringcol.GetData();
            if (data.size() <= std::numeric_limits<uint32_t>::max()) {
                util::Write<uint8_t>(os_, 4);
                WriteOffsets(os_, stringcol.GetOffsets());
            } else {
                util::Write<uint8_t>(os_, 8);
                util::WriteArray(os_, stringcol.GetOffsets());
            }
            util::WriteArray(os_, data);
            break;
        }
        default:
            THROW_RUNTIME_ERROR("Unsupported type");
    }
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
        }
    }
}
}  // namespace columnar::bruh

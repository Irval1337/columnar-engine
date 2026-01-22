#include <bruh/bruh_batch_writer.h>
#include "core/column.h"
#include "core/columns/bool_column.h"
#include "util/stream_helper.h"

namespace columnar::bruh {
void BruhBatchWriter::Write(const core::Batch& batch) {
    RowGroupMetaData group;
    group.rows_count = batch.RowsCount();
    metadata_.rows_count += batch.RowsCount();
    for (std::size_t col = 0; col < batch.ColumnsCount(); ++col) {
        ColumnChunkMetaData chunk;
        chunk.offset = os_.tellp();
        chunk.values_count = batch.RowsCount();
        WriteColumn(batch.ColumnAt(col), schema_.GetFields()[col]);
        chunk.byte_size = static_cast<uint64_t>(os_.tellp()) - chunk.offset;
        group.columns.emplace_back(std::move(chunk));
    }
    metadata_.row_groups.emplace_back(std::move(group));
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
            auto int64col = dynamic_cast<const core::Int64Column*>(&col);
            if (field.nullable) {
                util::WriteArray(os_, int64col->GetNullMask().GetData());
            }
            util::WriteArray(os_, int64col->GetData());
            break;
        }
        case core::DataType::Double: {
            auto doublecol = dynamic_cast<const core::DoubleColumn*>(&col);
            if (field.nullable) {
                util::WriteArray(os_, doublecol->GetNullMask().GetData());
            }
            util::WriteArray(os_, doublecol->GetData());
            break;
        }
        case core::DataType::Bool: {
            auto boolcol = dynamic_cast<const core::BoolColumn*>(&col);
            if (field.nullable) {
                util::WriteArray(os_, boolcol->GetNullMask().GetData());
            }
            util::WriteArray(os_, boolcol->GetData().GetData());
            break;
        }
        case core::DataType::String: {
            auto stringcol = dynamic_cast<const core::StringColumn*>(&col);
            if (field.nullable) {
                util::WriteArray(os_, stringcol->GetNullMask().GetData());
            }
            util::WriteArray(os_, stringcol->GetOffsets());
            util::WriteArray(os_, stringcol->GetLengths());
            util::WriteArray(os_, stringcol->GetData());
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
        util::Write<uint8_t>(os_, static_cast<uint8_t>(f.nullable ? 1 : 0));
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

#include <exec/late_materialize_operator.h>

#include <core/columns/numeric_column.h>
#include <core/field.h>
#include <core/schema.h>
#include <exec/column_row_access.h>
#include <exec/expression/types.h>
#include <exec/selection.h>
#include <util/macro.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace columnar::exec {
namespace {
uint32_t RowGroupOf(int64_t id) {
    return static_cast<uint32_t>(static_cast<uint64_t>(id) >> 32);
}

uint32_t RowOf(int64_t id) {
    return static_cast<uint32_t>(static_cast<uint64_t>(id) & 0xFFFFFFFFU);
}

const core::Int64Column& RowIdColumn(const core::Batch& batch, size_t index) {
    auto& column = batch.ColumnAt(index);
    if (column.GetDataType() != core::DataType::Int64) {
        THROW_RUNTIME_ERROR("Row id column must be Int64");
    }
    return static_cast<const core::Int64Column&>(column);
}
}  // namespace

LateMaterializeSink::LateMaterializeSink(IOperator& downstream, bruh::BruhBatchReader& reader,
                                         std::vector<ProjectionUnit> projections)
    : downstream_(downstream), reader_(reader), projections_(std::move(projections)) {
}

void LateMaterializeSink::Consume(core::Batch batch) {
    if (batch.RowsCount() == 0) {
        return;
    }
    auto& row_ids = RowIdColumn(batch, batch.GetSchema().GetIndex(kRowIdColumn));
    const std::vector<uint32_t>* selection = batch.HasSelection() ? &batch.Selection() : nullptr;
    ForSelectedRows(selection, batch.RowsCount(),
                    [&](size_t row) { row_ids_.push_back(row_ids.Get(row)); });
}

void LateMaterializeSink::Finalize() {
    if (row_ids_.empty()) {
        downstream_.Finalize();
        return;
    }

    auto& table_schema = reader_.GetSchema();
    std::vector<std::string> source_names;
    std::vector<core::Field> out_fields;
    source_names.reserve(projections_.size());
    out_fields.reserve(projections_.size());
    for (auto& projection : projections_) {
        if (projection.expression->type != ExpressionType::Column) {
            THROW_RUNTIME_ERROR("LateMaterialize: projection must be a column");
        }
        auto& column = static_cast<const ColumnExpr&>(*projection.expression);
        auto* field = table_schema.FindField(column.name);
        if (field == nullptr) {
            THROW_RUNTIME_ERROR("LateMaterialize: unknown column " + column.name);
        }
        source_names.push_back(column.name);
        out_fields.emplace_back(projection.name, field->type, field->nullable);
    }
    auto source_indexes = reader_.ResolveColumnNames(source_names);
    core::Schema out_schema(std::move(out_fields));

    std::vector<size_t> order(row_ids_.size());
    for (size_t i = 0; i < order.size(); ++i) {
        order[i] = i;
    }
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        return static_cast<uint64_t>(row_ids_[a]) < static_cast<uint64_t>(row_ids_[b]);
    });

    core::Batch grouped(out_schema, row_ids_.size());
    std::vector<size_t> grouped_position(row_ids_.size());
    core::Batch source;
    size_t loaded_group = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < order.size(); ++i) {
        int64_t id = row_ids_[order[i]];
        size_t group = RowGroupOf(id);
        if (loaded_group != group) {
            source = reader_.ReadRowGroup(group, source_indexes);
            loaded_group = group;
        }
        size_t row = RowOf(id);
        if (row >= source.RowsCount()) {
            THROW_RUNTIME_ERROR("LateMaterialize: row id is out of range");
        }
        for (size_t col = 0; col < grouped.ColumnsCount(); ++col) {
            AppendRow(grouped.ColumnAt(col), source.ColumnAt(col), row);
        }
        grouped_position[order[i]] = i;
    }

    core::Batch out(out_schema, row_ids_.size());
    for (size_t i = 0; i < row_ids_.size(); ++i) {
        size_t source_row = grouped_position[i];
        for (size_t col = 0; col < out.ColumnsCount(); ++col) {
            AppendRow(out.ColumnAt(col), grouped.ColumnAt(col), source_row);
        }
    }
    downstream_.Consume(std::move(out));
    downstream_.Finalize();
}

TopNRowIdSink::TopNRowIdSink(IOperator& downstream, std::vector<SortUnit> sort_units,
                             std::optional<size_t> limit, std::optional<size_t> offset)
    : downstream_(downstream), sort_units_(std::move(sort_units)), limit_(limit), offset_(offset) {
    if (limit_) {
        prefix_ = offset_.value_or(0) + *limit_;
        refs_.reserve(prefix_);
    }
}

void TopNRowIdSink::Consume(core::Batch batch) {
    if (batch.SelectedRowsCount() == 0 || (limit_ && prefix_ == 0)) {
        return;
    }
    Init(batch);
    auto& row_ids = RowIdColumn(batch, row_id_index_);
    std::vector<const core::Column*> sort_cols;
    sort_cols.reserve(sort_indexes_.size());
    for (size_t index : sort_indexes_) {
        sort_cols.push_back(&batch.ColumnAt(index));
    }

    RowIdRef candidate;
    candidate.sort_values.reserve(sort_units_.size());
    auto less = [&](const RowIdRef& lhs, const RowIdRef& rhs) { return Less(lhs, rhs); };
    const std::vector<uint32_t>* selection = batch.HasSelection() ? &batch.Selection() : nullptr;
    ForSelectedRows(selection, batch.RowsCount(), [&](size_t row) {
        candidate.row_id = row_ids.Get(row);
        candidate.sort_values.clear();
        for (const core::Column* col : sort_cols) {
            candidate.sort_values.push_back(ReadSortValue(*col, row));
        }
        if (limit_) {
            OfferToTopN(refs_, prefix_, candidate, less);
        } else {
            refs_.push_back(candidate);
        }
    });
}

void TopNRowIdSink::Finalize() {
    if (refs_.empty()) {
        downstream_.Finalize();
        return;
    }

    auto less = [&](const RowIdRef& lhs, const RowIdRef& rhs) { return Less(lhs, rhs); };
    if (limit_) {
        FinishTopN(refs_, offset_.value_or(0), less);
    } else {
        std::sort(refs_.begin(), refs_.end(), less);
        size_t offset = offset_.value_or(0);
        if (offset >= refs_.size()) {
            refs_.clear();
        } else if (offset > 0) {
            refs_.erase(refs_.begin(), refs_.begin() + offset);
        }
    }

    core::Batch out(core::Schema({core::Field(kRowIdColumn, core::DataType::Int64)}), refs_.size());
    for (auto& ref : refs_) {
        AppendInteger(out.ColumnAt(0), ref.row_id);
    }
    downstream_.Consume(std::move(out));
    downstream_.Finalize();
}

void TopNRowIdSink::Init(const core::Batch& batch) {
    if (initialized_) {
        return;
    }
    row_id_index_ = batch.GetSchema().GetIndex(kRowIdColumn);
    sort_indexes_.reserve(sort_units_.size());
    for (auto& unit : sort_units_) {
        if (unit.expression->type != ExpressionType::Column) {
            THROW_RUNTIME_ERROR("TopNRowId: sort expression must be a column");
        }
        auto& column = static_cast<const ColumnExpr&>(*unit.expression);
        sort_indexes_.push_back(batch.GetSchema().GetIndex(column.name));
    }
    initialized_ = true;
}

bool TopNRowIdSink::Less(const RowIdRef& lhs, const RowIdRef& rhs) const {
    for (size_t i = 0; i < sort_units_.size(); ++i) {
        int cmp = CompareSortValues(lhs.sort_values[i], rhs.sort_values[i]);
        if (cmp == 0) {
            continue;
        }
        return sort_units_[i].ascending ? cmp < 0 : cmp > 0;
    }
    return static_cast<uint64_t>(lhs.row_id) < static_cast<uint64_t>(rhs.row_id);
}
}  // namespace columnar::exec

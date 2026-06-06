#include <exec/late_topn.h>

#include <exec/column_row_access.h>
#include <exec/expression/eval.h>
#include <exec/expression/types.h>
#include <exec/metadata_pruning.h>
#include <exec/topn_common.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace columnar::exec {
namespace {
struct SortValue {
    core::DataType type = core::DataType::Int64;
    bool is_null = false;
    int64_t int_value = 0;
    double double_value = 0.0;
    std::string string_value;
};

struct LateRowRef {
    uint32_t row_group = 0;
    uint32_t row = 0;
    std::vector<SortValue> sort_values;
};

bool IsColumnExpr(const Expression& expr) {
    return expr.type == ExpressionType::Column;
}

bool IsAllColumnsProjection(const ProjectOperator& project, const core::Schema& schema) {
    if (project.projections.size() != schema.FieldsCount()) {
        return false;
    }
    for (size_t i = 0; i < project.projections.size(); ++i) {
        auto& projection = project.projections[i];
        auto& field = schema.GetFields()[i];
        if (projection.name != field.name || !IsColumnExpr(*projection.expression)) {
            return false;
        }
        auto& column = static_cast<const ColumnExpr&>(*projection.expression);
        if (column.name != field.name || column.type != field.type) {
            return false;
        }
    }
    return true;
}

bool CanLateMaterializeTopN(const TopNOperator& topn) {
    if (!topn.limit || topn.sort_units.empty()) {
        return false;
    }
    for (auto& unit : topn.sort_units) {
        if (!IsColumnExpr(*unit.expression)) {
            return false;
        }
    }
    return true;
}

SortValue ReadSortValue(const core::Batch& batch, const ColumnExpr& column_expr, size_t row) {
    auto& col = batch.ColumnAt(batch.GetSchema().GetIndex(column_expr.name));
    SortValue value;
    value.type = col.GetDataType();
    value.is_null = col.IsNull(row);
    if (value.is_null) {
        return value;
    }
    switch (value.type) {
        case core::DataType::String:
            value.string_value = std::string(ReadStringRow(col, row));
            break;
        case core::DataType::Double:
            value.double_value = ReadDoubleRow(col, row);
            break;
        default:
            value.int_value = ReadIntegerRow(col, row);
            break;
    }
    return value;
}

int CompareSortValues(const SortValue& lhs, const SortValue& rhs) {
    if (lhs.is_null != rhs.is_null) {
        return lhs.is_null ? -1 : 1;
    }
    if (lhs.is_null) {
        return 0;
    }
    switch (lhs.type) {
        case core::DataType::String:
            return Compare3(lhs.string_value, rhs.string_value);
        case core::DataType::Double:
            return Compare3(lhs.double_value, rhs.double_value);
        default:
            return Compare3(lhs.int_value, rhs.int_value);
    }
}

bool LateRowLess(const LateRowRef& lhs, const LateRowRef& rhs,
                 const std::vector<SortUnit>& sort_units) {
    for (size_t i = 0; i < sort_units.size(); ++i) {
        int cmp = CompareSortValues(lhs.sort_values[i], rhs.sort_values[i]);
        if (cmp == 0) {
            continue;
        }
        return sort_units[i].ascending ? cmp < 0 : cmp > 0;
    }
    if (lhs.row_group != rhs.row_group) {
        return lhs.row_group < rhs.row_group;
    }
    return lhs.row < rhs.row;
}

core::Batch MaterializeLateRows(bruh::BruhBatchReader& reader,
                                const std::vector<LateRowRef>& refs) {
    auto& schema = reader.GetSchema();
    std::vector<size_t> all_indexes(schema.FieldsCount());
    for (size_t i = 0; i < all_indexes.size(); ++i) {
        all_indexes[i] = i;
    }

    std::vector<size_t> order(refs.size());
    for (size_t i = 0; i < order.size(); ++i) {
        order[i] = i;
    }
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        if (refs[a].row_group != refs[b].row_group) {
            return refs[a].row_group < refs[b].row_group;
        }
        return refs[a].row < refs[b].row;
    });

    core::Batch grouped(schema, refs.size());
    std::vector<size_t> grouped_position(refs.size());
    core::Batch payload;
    size_t loaded_group = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < order.size(); ++i) {
        auto& ref = refs[order[i]];
        if (loaded_group != ref.row_group) {
            payload = reader.ReadRowGroup(ref.row_group, all_indexes);
            loaded_group = ref.row_group;
        }
        for (size_t col = 0; col < grouped.ColumnsCount(); ++col) {
            AppendRow(grouped.ColumnAt(col), payload.ColumnAt(col), ref.row);
        }
        grouped_position[order[i]] = i;
    }

    core::Batch out(schema, refs.size());
    for (size_t i = 0; i < refs.size(); ++i) {
        size_t source = grouped_position[i];
        for (size_t col = 0; col < out.ColumnsCount(); ++col) {
            AppendRow(out.ColumnAt(col), grouped.ColumnAt(col), source);
        }
    }
    return out;
}
}  // namespace

bool TryExecuteLateTopN(bruh::BruhBatchReader& reader, const ProjectOperator& project,
                        IOperator& downstream) {
    if (!IsAllColumnsProjection(project, reader.GetSchema()) ||
        project.child->type != OperatorType::TopN) {
        return false;
    }
    auto& topn = static_cast<const TopNOperator&>(*project.child);
    if (!CanLateMaterializeTopN(topn) || topn.child->type != OperatorType::Filter) {
        return false;
    }
    auto& filter = static_cast<const FilterOperator&>(*topn.child);
    if (filter.child->type != OperatorType::Scan) {
        return false;
    }

    std::vector<std::string> initial_names;
    CollectColumns(*filter.condition, initial_names);
    for (auto& unit : topn.sort_units) {
        CollectColumns(*unit.expression, initial_names);
    }
    if (initial_names.empty()) {
        return false;
    }
    auto initial_indexes = reader.ResolveColumnNames(initial_names);

    size_t offset = topn.offset.value_or(0);
    size_t prefix = offset + *topn.limit;
    std::vector<LateRowRef> refs;
    refs.reserve(prefix);
    bool saw_match = false;
    auto less = [&](const LateRowRef& lhs, const LateRowRef& rhs) {
        return LateRowLess(lhs, rhs, topn.sort_units);
    };

    LateRowRef candidate;
    candidate.sort_values.reserve(topn.sort_units.size());
    for (size_t group = 0; group < reader.NumRowGroups(); ++group) {
        if (!PredicateMayMatch(reader, group, *filter.condition)) {
            continue;
        }
        auto batch = reader.ReadRowGroup(group, initial_indexes);
        auto selection = EvaluatePredicateSelection(batch, *filter.condition);
        if (!selection.empty()) {
            saw_match = true;
        }
        for (uint32_t row : selection) {
            candidate.row_group = static_cast<uint32_t>(group);
            candidate.row = row;
            candidate.sort_values.clear();
            for (auto& unit : topn.sort_units) {
                candidate.sort_values.push_back(
                    ReadSortValue(batch, static_cast<const ColumnExpr&>(*unit.expression), row));
            }
            OfferToTopN(refs, prefix, candidate, less);
        }
    }

    FinishTopN(refs, offset, less);

    if (saw_match) {
        downstream.Consume(MaterializeLateRows(reader, refs));
    }
    downstream.Finalize();
    return true;
}
}  // namespace columnar::exec

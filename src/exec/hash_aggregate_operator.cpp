#include <exec/hash_aggregate_operator.h>

#include <core/column_factory.h>
#include <core/datatype.h>
#include <core/field.h>
#include <exec/column_dispatch.h>
#include <exec/expression/types.h>
#include <exec/expression/eval.h>
#include <exec/expression/utils.h>
#include <exec/kernel.h>
#include <exec/topn_common.h>
#include <util/macro.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace columnar::exec {
namespace {
core::DataType KeyOutputType(const Expression& expr) {
    auto type = GetExpressionType(expr);
    if (type == core::DataType::String || type == core::DataType::Timestamp ||
        type == core::DataType::Date) {
        return type;
    }
    if (HasIntegerValue(type)) {
        return core::DataType::Int64;
    }
    THROW_RUNTIME_ERROR("GROUP BY key must be integer, string, timestamp, or date");
}

core::Schema MakeHashAggregateSchema(const std::vector<ProjectionUnit>& keys,
                                     const std::vector<AggregationUnit>& aggregations) {
    std::vector<core::Field> fields;
    fields.reserve(keys.size() + aggregations.size());
    for (auto& key : keys) {
        fields.emplace_back(key.name, KeyOutputType(*key.expression), false);
    }
    auto aggregation_schema = MakeAggregationSchema(aggregations);
    for (auto& field : aggregation_schema.GetFields()) {
        fields.push_back(field);
    }
    return core::Schema(std::move(fields));
}
}  // namespace

HashAggregationSink::HashAggregationSink(IOperator& downstream, std::vector<ProjectionUnit> keys,
                                         std::vector<AggregationUnit> aggregations,
                                         std::optional<HashAggregateTopN> top_n)
    : downstream_(downstream),
      keys_(std::move(keys)),
      aggregations_(std::move(aggregations)),
      output_schema_(MakeHashAggregateSchema(keys_, aggregations_)),
      needs_dense_(RequiresDenseBatch(keys_) || RequiresDenseBatch(aggregations_)),
      state_(aggregations_, string_arena_),
      key_table_(GroupKeyTable::Make(keys_, string_arena_)),
      top_n_(std::move(top_n)) {
}

void HashAggregationSink::ReserveForBatch(size_t selected_rows, size_t max_new_groups) {
    if (selected_rows == 0 || max_new_groups == 0) {
        return;
    }

    size_t expected_new_groups = max_new_groups;
    if (input_rows_seen_ != 0) {
        long double groups_per_row = static_cast<long double>(state_.GroupsCount()) /
                                     static_cast<long double>(input_rows_seen_);
        expected_new_groups =
            static_cast<size_t>(static_cast<long double>(selected_rows) * groups_per_row * 1.25L) +
            1;
        expected_new_groups = std::min(expected_new_groups, max_new_groups);
    }

    size_t target_groups = static_cast<size_t>(state_.GroupsCount()) + expected_new_groups;
    if (target_groups <= reserved_groups_) {
        return;
    }

    size_t reserve_groups = target_groups;
    if (reserved_groups_ != 0) {
        reserve_groups = reserved_groups_;
        while (reserve_groups < target_groups) {
            reserve_groups += std::max<size_t>(reserve_groups / 2, 1024);
        }
    }

    reserved_groups_ = reserve_groups;
    state_.Reserve(reserve_groups);
    key_table_->ReserveBuckets(reserve_groups);
}

void HashAggregationSink::Consume(core::Batch batch) {
    size_t rows = batch.RowsCount();
    if (rows == 0) {
        return;
    }
    if (batch.HasSelection() && needs_dense_) {
        batch = kernel::Materialize(batch);
        rows = batch.RowsCount();
    }
    size_t selected_rows = batch.SelectedRowsCount();

    std::vector<EvalResult> key_evals;
    key_evals.reserve(keys_.size());
    std::vector<const core::Column*> key_cols;
    key_cols.reserve(keys_.size());
    for (auto& key : keys_) {
        key_evals.emplace_back(Evaluate(batch, *key.expression));
        key_cols.push_back(&key_evals.back().Get());
    }

    std::vector<EvalResult> agg_evals;
    agg_evals.reserve(aggregations_.size());
    std::vector<const core::Column*> agg_cols(aggregations_.size(), nullptr);
    for (size_t i = 0; i < aggregations_.size(); ++i) {
        if (aggregations_[i].type == AggregationType::Count) {
            continue;
        }
        agg_evals.emplace_back(Evaluate(batch, *aggregations_[i].expression));
        agg_cols[i] = &agg_evals.back().Get();
    }

    if (auto bound = key_table_->MaxNewGroupsForBatch(key_cols, selected_rows)) {
        ReserveForBatch(selected_rows, *bound);
    }

    const std::vector<uint32_t>* selection = batch.HasSelection() ? &batch.Selection() : nullptr;
    key_table_->Consume(key_cols, agg_cols, selection, rows, state_);
    input_rows_seen_ += selected_rows;
}

void HashAggregationSink::Finalize() {
    if (top_n_) {
        FinalizeTopN(*top_n_);
        return;
    }
    core::Batch out(output_schema_, state_.GroupsCount());
    for (uint32_t group_id = 0; group_id < state_.GroupsCount(); ++group_id) {
        key_table_->AppendKeys(group_id, out);
        for (size_t i = 0; i < aggregations_.size(); ++i) {
            state_.AppendResult(i, group_id, out.ColumnAt(keys_.size() + i));
        }
    }
    downstream_.Consume(std::move(out));
    downstream_.Finalize();
}

void HashAggregationSink::FinalizeTopN(const HashAggregateTopN& spec) {
    uint32_t groups = state_.GroupsCount();
    size_t keys_count = keys_.size();
    size_t offset = spec.offset.value_or(0);
    size_t prefix = offset + spec.limit;
    auto& fields = output_schema_.GetFields();

    std::vector<size_t> output_index;
    output_index.reserve(spec.sort_units.size());
    bool need_keys = false;
    for (auto& unit : spec.sort_units) {
        size_t index =
            output_schema_.GetIndex(static_cast<const ColumnExpr&>(*unit.expression).name);
        output_index.push_back(index);
        need_keys = need_keys || index < keys_count;
    }

    core::Batch keys_batch;
    if (need_keys) {
        std::vector<core::Field> key_fields(fields.begin(), fields.begin() + keys_count);
        keys_batch = core::Batch(core::Schema(std::move(key_fields)), groups);
        for (uint32_t group_id = 0; group_id < groups; ++group_id) {
            key_table_->AppendKeys(group_id, keys_batch);
        }
    }

    std::vector<std::unique_ptr<core::Column>> agg_columns(spec.sort_units.size());
    std::vector<const core::Column*> sort_columns(spec.sort_units.size());
    for (size_t s = 0; s < spec.sort_units.size(); ++s) {
        size_t index = output_index[s];
        if (index < keys_count) {
            sort_columns[s] = &keys_batch.ColumnAt(index);
            continue;
        }
        auto& field = fields[index];
        auto column = core::MakeColumn(field.type, field.nullable);
        column->Reserve(groups);
        for (uint32_t group_id = 0; group_id < groups; ++group_id) {
            state_.AppendResult(index - keys_count, group_id, *column);
        }
        sort_columns[s] = column.get();
        agg_columns[s] = std::move(column);
    }

    auto less = [&](uint32_t a, uint32_t b) {
        for (size_t s = 0; s < sort_columns.size(); ++s) {
            int cmp = CompareRowRefs(*sort_columns[s], a, *sort_columns[s], b);
            if (cmp != 0) {
                return spec.sort_units[s].ascending ? cmp < 0 : cmp > 0;
            }
        }
        return a < b;
    };

    std::vector<uint32_t> refs;
    refs.reserve(prefix);
    for (uint32_t group_id = 0; group_id < groups; ++group_id) {
        OfferToTopN(refs, prefix, group_id, less);
    }
    FinishTopN(refs, offset, less);

    core::Batch out(output_schema_, refs.size());
    for (uint32_t group_id : refs) {
        key_table_->AppendKeys(group_id, out);
        for (size_t i = 0; i < aggregations_.size(); ++i) {
            state_.AppendResult(i, group_id, out.ColumnAt(keys_count + i));
        }
    }
    downstream_.Consume(std::move(out));
    downstream_.Finalize();
}
}  // namespace columnar::exec

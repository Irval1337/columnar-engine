#include <exec/hash_aggregate_operator.h>

#include <core/datatype.h>
#include <core/field.h>
#include <exec/column_dispatch.h>
#include <exec/expression/types.h>
#include <exec/expression/eval.h>
#include <exec/expression/utils.h>
#include <exec/kernel.h>
#include <util/macro.h>

#include <algorithm>
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
                                         std::vector<AggregationUnit> aggregations)
    : downstream_(downstream),
      keys_(std::move(keys)),
      aggregations_(std::move(aggregations)),
      output_schema_(MakeHashAggregateSchema(keys_, aggregations_)),
      needs_dense_(RequiresDenseBatch(keys_) || RequiresDenseBatch(aggregations_)),
      state_(aggregations_, string_arena_),
      key_table_(GroupKeyTable::Make(keys_, string_arena_)) {
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
}  // namespace columnar::exec

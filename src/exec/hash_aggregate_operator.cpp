#include <exec/hash_aggregate_operator.h>

#include <core/columns/string_column.h>
#include <core/datatype.h>
#include <core/field.h>
#include <exec/column_helpers.h>
#include <exec/expression.h>
#include <util/macro.h>

#include <utility>

namespace columnar::exec {
namespace {
core::DataType KeyOutputType(const Expression& expr) {
    auto type = GetExpressionType(expr);
    if (type == core::DataType::String) {
        return core::DataType::String;
    }
    if (HasIntegerValue(type)) {
        return core::DataType::Int64;
    }
    THROW_RUNTIME_ERROR("GROUP BY key must be integer or string");
}

core::Schema MakeHashAggregateSchema(const Expression& key, const std::string& key_name,
                                     const std::vector<AggregationUnit>& aggregations) {
    std::vector<core::Field> fields;
    fields.reserve(aggregations.size() + 1);
    fields.emplace_back(key_name, KeyOutputType(key), false);
    auto aggregation_schema = MakeAggregationSchema(aggregations);
    for (auto& field : aggregation_schema.GetFields()) {
        fields.push_back(field);
    }
    return core::Schema(std::move(fields));
}
}  // namespace

HashAggregationSink::KeyMode HashAggregationSink::SelectKeyMode(const Expression& key) {
    auto type = GetExpressionType(key);
    if (type == core::DataType::String) {
        return KeyMode::String;
    }
    return KeyMode::Int64;
}

HashAggregationSink::HashAggregationSink(IOperator& downstream, std::shared_ptr<Expression> key,
                                         std::string key_name,
                                         std::vector<AggregationUnit> aggregations)
    : downstream_(downstream),
      key_(std::move(key)),
      key_name_(std::move(key_name)),
      aggregations_(std::move(aggregations)),
      output_schema_(MakeHashAggregateSchema(*key_, key_name_, aggregations_)),
      mode_(SelectKeyMode(*key_)) {
}

void HashAggregationSink::UpdateAggsForRow(States& states,
                                           const std::vector<const core::Column*>& agg_cols,
                                           size_t row) {
    for (size_t i = 0; i < aggregations_.size(); ++i) {
        auto& unit = aggregations_[i];
        if (unit.type == AggregationType::Count) {
            ++std::get<CountState>(states[i]).value;
            continue;
        }
        UpdateAggregationStateRow(states[i], unit, *agg_cols[i], row);
    }
}

void HashAggregationSink::ConsumeInt64(const core::Column& key_col,
                                       const std::vector<const core::Column*>& agg_cols,
                                       size_t rows) {
    for (size_t row = 0; row < rows; ++row) {
        if (key_col.IsNull(row)) {
            continue;
        }
        int64_t key = ReadIntegerRow(key_col, row);
        auto it = int64_groups_.find(key);
        if (it == int64_groups_.end()) {
            it = int64_groups_.emplace(key, MakeAggregationStates(aggregations_)).first;
        }
        UpdateAggsForRow(it->second, agg_cols, row);
    }
}

void HashAggregationSink::ConsumeString(const core::Column& key_col,
                                        const std::vector<const core::Column*>& agg_cols,
                                        size_t rows) {
    auto& s = static_cast<const core::StringColumn&>(key_col);
    for (size_t row = 0; row < rows; ++row) {
        if (s.IsNull(row)) {
            continue;
        }
        auto key = s.Get(row);
        auto it = string_groups_.find(key);
        if (it == string_groups_.end()) {
            it = string_groups_.emplace(std::string(key), MakeAggregationStates(aggregations_))
                     .first;
        }
        UpdateAggsForRow(it->second, agg_cols, row);
    }
}

void HashAggregationSink::Consume(core::Batch batch) {
    size_t rows = batch.RowsCount();
    if (rows == 0) {
        return;
    }

    auto key_eval = Evaluate(batch, *key_);
    auto& key_col = key_eval.Get();

    std::vector<EvalResult> evals;
    evals.reserve(aggregations_.size());
    std::vector<const core::Column*> agg_cols(aggregations_.size(), nullptr);
    for (size_t i = 0; i < aggregations_.size(); ++i) {
        if (aggregations_[i].type == AggregationType::Count) {
            continue;
        }
        evals.emplace_back(Evaluate(batch, *aggregations_[i].expression));
        agg_cols[i] = &evals.back().Get();
    }

    if (mode_ == KeyMode::Int64) {
        ConsumeInt64(key_col, agg_cols, rows);
    } else {
        ConsumeString(key_col, agg_cols, rows);
    }
}

void HashAggregationSink::Finalize() {
    size_t groups_count = mode_ == KeyMode::Int64 ? int64_groups_.size() : string_groups_.size();
    core::Batch out(output_schema_, groups_count);
    auto& key_out = out.ColumnAt(0);

    auto append_aggs = [&](const States& states) {
        for (size_t i = 0; i < aggregations_.size(); ++i) {
            AppendAggregationResult(states[i], aggregations_[i], out.ColumnAt(i + 1));
        }
    };

    if (mode_ == KeyMode::Int64) {
        for (auto& [key, states] : int64_groups_) {
            AppendInteger(key_out, key);
            append_aggs(states);
        }
    } else {
        for (auto& [key, states] : string_groups_) {
            static_cast<core::StringColumn&>(key_out).Append(key);
            append_aggs(states);
        }
    }

    downstream_.Consume(std::move(out));
    downstream_.Finalize();
}
}  // namespace columnar::exec

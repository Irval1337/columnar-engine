#include <exec/hash_aggregate_operator.h>

#include <core/columns/string_column.h>
#include <core/datatype.h>
#include <core/field.h>
#include <exec/column_helpers.h>
#include <exec/expression.h>
#include <util/macro.h>

#include <functional>
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

size_t HashAggregationSink::GroupKeyHash::operator()(const GroupKey& k) const noexcept {
    return std::visit(
        [](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, int64_t>) {
                return std::hash<int64_t>{}(value);
            } else {
                return std::hash<std::string>{}(value);
            }
        },
        k);
}

HashAggregationSink::HashAggregationSink(IOperator& downstream, std::shared_ptr<Expression> key,
                                         std::string key_name,
                                         std::vector<AggregationUnit> aggregations)
    : downstream_(downstream),
      key_(std::move(key)),
      key_name_(std::move(key_name)),
      aggregations_(std::move(aggregations)),
      output_schema_(MakeHashAggregateSchema(*key_, key_name_, aggregations_)) {
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

    bool string_key = key_col.GetDataType() == core::DataType::String;

    for (size_t row = 0; row < rows; ++row) {
        if (key_col.IsNull(row)) {
            continue;
        }

        GroupKey key = string_key ? GroupKey{std::string(ReadStringRow(key_col, row))}
                                  : GroupKey{ReadIntegerRow(key_col, row)};
        auto& states = groups_[std::move(key)];
        if (states.empty()) {
            states = MakeAggregationStates(aggregations_);
        }
        for (size_t i = 0; i < aggregations_.size(); ++i) {
            auto& unit = aggregations_[i];
            if (unit.type == AggregationType::Count) {
                ++std::get<CountState>(states[i]).value;
                continue;
            }
            UpdateAggregationStateRow(states[i], unit, *agg_cols[i], row);
        }
    }
}

void HashAggregationSink::Finalize() {
    core::Batch out(output_schema_, groups_.size());
    auto& key_out = out.ColumnAt(0);

    for (auto& [key, states] : groups_) {
        std::visit(
            [&](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, int64_t>) {
                    AppendInteger(key_out, value);
                } else {
                    static_cast<core::StringColumn&>(key_out).Append(value);
                }
            },
            key);
        for (size_t i = 0; i < aggregations_.size(); ++i) {
            AppendAggregationResult(states[i], aggregations_[i], out.ColumnAt(i + 1));
        }
    }

    downstream_.Consume(std::move(out));
    downstream_.Finalize();
}
}  // namespace columnar::exec

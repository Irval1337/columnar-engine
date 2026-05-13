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

void HashCombine(size_t& seed, size_t value) noexcept {
    seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
}
}  // namespace

size_t HashAggregationSink::GroupKeyHash::operator()(const GroupKey& key) const noexcept {
    size_t seed = key.size();
    for (auto& component : key) {
        std::visit(
            [&](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, int64_t>) {
                    HashCombine(seed, std::hash<int64_t>{}(value));
                } else {
                    HashCombine(seed, std::hash<std::string>{}(value));
                }
            },
            component);
    }
    return seed;
}

HashAggregationSink::HashAggregationSink(IOperator& downstream, std::vector<ProjectionUnit> keys,
                                         std::vector<AggregationUnit> aggregations)
    : downstream_(downstream),
      keys_(std::move(keys)),
      aggregations_(std::move(aggregations)),
      output_schema_(MakeHashAggregateSchema(keys_, aggregations_)) {
}

void HashAggregationSink::Consume(core::Batch batch) {
    size_t rows = batch.RowsCount();
    if (rows == 0) {
        return;
    }

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

    GroupKey key;
    key.reserve(key_cols.size());
    for (size_t row = 0; row < rows; ++row) {
        key.clear();
        bool has_null_key = false;
        for (auto* col : key_cols) {
            if (col->IsNull(row)) {
                has_null_key = true;
                break;
            }
            if (col->GetDataType() == core::DataType::String) {
                key.emplace_back(std::string(ReadStringRow(*col, row)));
            } else {
                key.emplace_back(ReadIntegerRow(*col, row));
            }
        }
        if (has_null_key) {
            continue;
        }

        auto it = groups_.find(key);
        if (it == groups_.end()) {
            it = groups_.emplace(key, MakeAggregationStates(aggregations_)).first;
        }
        auto& states = it->second;
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

    for (auto& [key, states] : groups_) {
        for (size_t i = 0; i < keys_.size(); ++i) {
            auto& key_out = out.ColumnAt(i);
            std::visit(
                [&](const auto& value) {
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<T, int64_t>) {
                        AppendInteger(key_out, value);
                    } else {
                        static_cast<core::StringColumn&>(key_out).Append(value);
                    }
                },
                key[i]);
        }
        for (size_t i = 0; i < aggregations_.size(); ++i) {
            AppendAggregationResult(states[i], aggregations_[i], out.ColumnAt(keys_.size() + i));
        }
    }

    downstream_.Consume(std::move(out));
    downstream_.Finalize();
}
}  // namespace columnar::exec

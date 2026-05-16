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

HashAggregationSink::KeyMode HashAggregationSink::SelectKeyMode(
    const std::vector<ProjectionUnit>& keys) {
    if (keys.size() != 1) {
        return KeyMode::Generic;
    }

    auto type = GetExpressionType(*keys.front().expression);
    if (type == core::DataType::String) {
        return KeyMode::SingleString;
    }
    return KeyMode::SingleInt64;
}

HashAggregationSink::HashAggregationSink(IOperator& downstream, std::vector<ProjectionUnit> keys,
                                         std::vector<AggregationUnit> aggregations)
    : downstream_(downstream),
      keys_(std::move(keys)),
      aggregations_(std::move(aggregations)),
      output_schema_(MakeHashAggregateSchema(keys_, aggregations_)),
      mode_(SelectKeyMode(keys_)) {
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

void HashAggregationSink::ConsumeSingleInt64(const core::Column& key_col,
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

void HashAggregationSink::ConsumeSingleString(const core::Column& key_col,
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

void HashAggregationSink::ConsumeGeneric(const std::vector<const core::Column*>& key_cols,
                                         const std::vector<const core::Column*>& agg_cols,
                                         size_t rows) {
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
            it = groups_.emplace(std::move(key), MakeAggregationStates(aggregations_)).first;
        }
        UpdateAggsForRow(it->second, agg_cols, row);
    }
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

    if (mode_ == KeyMode::SingleInt64) {
        ConsumeSingleInt64(*key_cols.front(), agg_cols, rows);
    } else if (mode_ == KeyMode::SingleString) {
        ConsumeSingleString(*key_cols.front(), agg_cols, rows);
    } else {
        ConsumeGeneric(key_cols, agg_cols, rows);
    }
}

void HashAggregationSink::Finalize() {
    size_t groups_count = groups_.size();
    if (mode_ == KeyMode::SingleInt64) {
        groups_count = int64_groups_.size();
    } else if (mode_ == KeyMode::SingleString) {
        groups_count = string_groups_.size();
    }

    core::Batch out(output_schema_, groups_count);

    auto append_aggs = [&](const States& states) {
        for (size_t i = 0; i < aggregations_.size(); ++i) {
            AppendAggregationResult(states[i], aggregations_[i], out.ColumnAt(keys_.size() + i));
        }
    };

    if (mode_ == KeyMode::SingleInt64) {
        auto& key_out = out.ColumnAt(0);
        for (auto& [key, states] : int64_groups_) {
            AppendInteger(key_out, key);
            append_aggs(states);
        }
    } else if (mode_ == KeyMode::SingleString) {
        auto& key_out = out.ColumnAt(0);
        for (auto& [key, states] : string_groups_) {
            static_cast<core::StringColumn&>(key_out).Append(key);
            append_aggs(states);
        }
    } else {
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
            append_aggs(states);
        }
    }

    downstream_.Consume(std::move(out));
    downstream_.Finalize();
}
}  // namespace columnar::exec

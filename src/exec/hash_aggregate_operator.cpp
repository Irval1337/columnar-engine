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

size_t HashAggregationSink::CompositeKeyHash::operator()(const CompositeKey& key) const noexcept {
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

size_t HashAggregationSink::Int64PairHash::operator()(const Int64Pair& key) const noexcept {
    size_t seed = 2;
    HashCombine(seed, std::hash<int64_t>{}(key.first));
    HashCombine(seed, std::hash<int64_t>{}(key.second));
    return seed;
}

HashAggregationSink::KeyMode HashAggregationSink::SelectKeyMode(
    const std::vector<ProjectionUnit>& keys) {
    if (keys.size() == 2) {
        auto first_type = GetExpressionType(*keys[0].expression);
        auto second_type = GetExpressionType(*keys[1].expression);
        if ((HasIntegerValue(first_type) || first_type == core::DataType::Timestamp ||
             first_type == core::DataType::Date) &&
            (HasIntegerValue(second_type) || second_type == core::DataType::Timestamp ||
             second_type == core::DataType::Date)) {
            return KeyMode::Int64Pair;
        }
    }
    if (keys.size() != 1) {
        return KeyMode::Composite;
    }
    auto type = GetExpressionType(*keys[0].expression);
    if (type == core::DataType::String) {
        return KeyMode::String;
    }
    if (HasIntegerValue(type) || type == core::DataType::Timestamp ||
        type == core::DataType::Date) {
        return KeyMode::Int64;
    }
    return KeyMode::Composite;
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

void HashAggregationSink::ConsumeInt64(const core::Column& key_col,
                                       const std::vector<const core::Column*>& agg_cols,
                                       size_t rows) {
    VisitIntegerCol(key_col, [&](const auto& typed) {
        const util::BitVector* mask = typed.IsNullable() ? &typed.GetNullMask() : nullptr;
        for (size_t row = 0; row < rows; ++row) {
            if (mask != nullptr && mask->Get(row)) {
                continue;
            }
            int64_t key = static_cast<int64_t>(ReadTypedValue(typed, row));
            auto it = int64_groups_.find(key);
            if (it == int64_groups_.end()) {
                it = int64_groups_.emplace(key, MakeAggregationStates(aggregations_)).first;
            }
            UpdateAggsForRow(it->second, agg_cols, row);
        }
    });
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

void HashAggregationSink::ConsumeInt64Pair(const core::Column& first_key_col,
                                           const core::Column& second_key_col,
                                           const std::vector<const core::Column*>& agg_cols,
                                           size_t rows) {
    VisitIntegerCol(first_key_col, [&](const auto& first_typed) {
        VisitIntegerCol(second_key_col, [&](const auto& second_typed) {
            const util::BitVector* first_mask =
                first_typed.IsNullable() ? &first_typed.GetNullMask() : nullptr;
            const util::BitVector* second_mask =
                second_typed.IsNullable() ? &second_typed.GetNullMask() : nullptr;
            for (size_t row = 0; row < rows; ++row) {
                if ((first_mask != nullptr && first_mask->Get(row)) ||
                    (second_mask != nullptr && second_mask->Get(row))) {
                    continue;
                }
                Int64Pair key{static_cast<int64_t>(ReadTypedValue(first_typed, row)),
                              static_cast<int64_t>(ReadTypedValue(second_typed, row))};
                auto it = int64_pair_groups_.find(key);
                if (it == int64_pair_groups_.end()) {
                    it =
                        int64_pair_groups_.emplace(key, MakeAggregationStates(aggregations_)).first;
                }
                UpdateAggsForRow(it->second, agg_cols, row);
            }
        });
    });
}

void HashAggregationSink::ConsumeComposite(const std::vector<const core::Column*>& key_cols,
                                           const std::vector<const core::Column*>& agg_cols,
                                           size_t rows) {
    CompositeKey key;
    key.reserve(key_cols.size());
    for (size_t row = 0; row < rows; ++row) {
        key.clear();
        key.reserve(key_cols.size());
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

        auto it = composite_groups_.find(key);
        if (it == composite_groups_.end()) {
            it = composite_groups_.emplace(std::move(key), MakeAggregationStates(aggregations_))
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

    switch (mode_) {
        case KeyMode::Int64:
            ConsumeInt64(*key_cols[0], agg_cols, rows);
            return;
        case KeyMode::String:
            ConsumeString(*key_cols[0], agg_cols, rows);
            return;
        case KeyMode::Int64Pair:
            ConsumeInt64Pair(*key_cols[0], *key_cols[1], agg_cols, rows);
            return;
        case KeyMode::Composite:
            ConsumeComposite(key_cols, agg_cols, rows);
            return;
    }
}

void HashAggregationSink::Finalize() {
    size_t groups_count = mode_ == KeyMode::Int64       ? int64_groups_.size()
                          : mode_ == KeyMode::String    ? string_groups_.size()
                          : mode_ == KeyMode::Int64Pair ? int64_pair_groups_.size()
                                                        : composite_groups_.size();
    core::Batch out(output_schema_, groups_count);

    auto append_aggs = [&](const States& states) {
        for (size_t i = 0; i < aggregations_.size(); ++i) {
            AppendAggregationResult(states[i], aggregations_[i], out.ColumnAt(keys_.size() + i));
        }
    };

    if (mode_ == KeyMode::Int64) {
        for (auto& [key, states] : int64_groups_) {
            AppendInteger(out.ColumnAt(0), key);
            append_aggs(states);
        }
    } else if (mode_ == KeyMode::String) {
        for (auto& [key, states] : string_groups_) {
            static_cast<core::StringColumn&>(out.ColumnAt(0)).Append(key);
            append_aggs(states);
        }
    } else if (mode_ == KeyMode::Int64Pair) {
        for (auto& [key, states] : int64_pair_groups_) {
            AppendInteger(out.ColumnAt(0), key.first);
            AppendInteger(out.ColumnAt(1), key.second);
            append_aggs(states);
        }
    } else {
        for (auto& [key, states] : composite_groups_) {
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

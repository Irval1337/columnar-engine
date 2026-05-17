#include <exec/hash_aggregate_operator.h>

#include <core/columns/string_column.h>
#include <core/datatype.h>
#include <core/field.h>
#include <exec/column_helpers.h>
#include <exec/expression.h>
#include <exec/kernel.h>
#include <util/macro.h>

#include <limits>
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

void HashCombine(size_t& seed, size_t value) noexcept {
    seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
}

template <typename F>
void ForSelectedRows(const std::vector<uint32_t>* selection, size_t rows, F&& f) {
    if (selection != nullptr) {
        for (uint32_t row : *selection) {
            f(static_cast<size_t>(row));
        }
    } else {
        for (size_t row = 0; row < rows; ++row) {
            f(row);
        }
    }
}

bool RequiresDenseBatch(const std::vector<ProjectionUnit>& keys,
                        const std::vector<AggregationUnit>& aggregations) {
    for (auto& key : keys) {
        if (!IsTrivialExpression(*key.expression)) {
            return true;
        }
    }
    for (auto& unit : aggregations) {
        if (unit.expression != nullptr && !IsTrivialExpression(*unit.expression)) {
            return true;
        }
    }
    return false;
}
}  // namespace

size_t HashAggregationSink::HashCompositeKey(const CompositeKey& key) noexcept {
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

size_t HashAggregationSink::CompositeGroupHash::operator()(uint32_t group_id) const noexcept {
    return HashCompositeKey((*keys)[group_id]);
}

size_t HashAggregationSink::CompositeGroupHash::operator()(const CompositeKey& key) const noexcept {
    return HashCompositeKey(key);
}

bool HashAggregationSink::CompositeGroupEq::operator()(uint32_t lhs, uint32_t rhs) const noexcept {
    return (*keys)[lhs] == (*keys)[rhs];
}

bool HashAggregationSink::CompositeGroupEq::operator()(uint32_t lhs,
                                                       const CompositeKey& rhs) const noexcept {
    return (*keys)[lhs] == rhs;
}

bool HashAggregationSink::CompositeGroupEq::operator()(const CompositeKey& lhs,
                                                       uint32_t rhs) const noexcept {
    return lhs == (*keys)[rhs];
}

bool HashAggregationSink::CompositeGroupEq::operator()(const CompositeKey& lhs,
                                                       const CompositeKey& rhs) const noexcept {
    return lhs == rhs;
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
      mode_(SelectKeyMode(keys_)),
      needs_dense_(RequiresDenseBatch(keys_, aggregations_)),
      composite_groups_(0, CompositeGroupHash{&composite_group_keys_},
                        CompositeGroupEq{&composite_group_keys_}) {
    agg_arrays_.reserve(aggregations_.size());
    for (auto& unit : aggregations_) {
        switch (unit.type) {
            case AggregationType::Count:
                agg_arrays_.emplace_back(CountArray{});
                break;
            case AggregationType::Sum: {
                SumArray array;
                array.is_double = GetExpressionType(*unit.expression) == core::DataType::Double;
                agg_arrays_.emplace_back(std::move(array));
                break;
            }
            case AggregationType::Avg:
                agg_arrays_.emplace_back(AvgArray{});
                break;
            case AggregationType::Distinct: {
                DistinctArray array;
                array.is_string = GetExpressionType(*unit.expression) == core::DataType::String;
                agg_arrays_.emplace_back(std::move(array));
                break;
            }
            case AggregationType::Min:
            case AggregationType::Max: {
                MinMaxArray array;
                array.value_type = GetExpressionType(*unit.expression);
                agg_arrays_.emplace_back(std::move(array));
                break;
            }
            default:
                THROW_RUNTIME_ERROR("Unsupported aggregate type");
        }
    }
}

uint32_t HashAggregationSink::EmplaceGroup() {
    if (groups_count_ == std::numeric_limits<uint32_t>::max()) {
        THROW_RUNTIME_ERROR("Too many hash aggregation groups");
    }
    uint32_t group_id = groups_count_++;
    for (auto& array : agg_arrays_) {
        std::visit([](auto& typed) { typed.PushDefault(); }, array);
    }
    return group_id;
}

void HashAggregationSink::UpdateAggsForRow(uint32_t group_id,
                                           const std::vector<const core::Column*>& agg_cols,
                                           size_t row) {
    for (size_t i = 0; i < aggregations_.size(); ++i) {
        switch (aggregations_[i].type) {
            case AggregationType::Count: {
                ++std::get<CountArray>(agg_arrays_[i]).values[group_id];
                break;
            }
            case AggregationType::Sum: {
                const core::Column& col = *agg_cols[i];
                if (col.IsNull(row)) {
                    break;
                }
                auto& array = std::get<SumArray>(agg_arrays_[i]);
                array.has_value[group_id] = 1;
                if (array.is_double) {
                    array.double_values[group_id] +=
                        static_cast<long double>(ReadDoubleRow(col, row));
                } else {
                    array.int_values[group_id] += ReadIntegerRow(col, row);
                }
                break;
            }
            case AggregationType::Avg: {
                const core::Column& col = *agg_cols[i];
                if (col.IsNull(row)) {
                    break;
                }
                auto& array = std::get<AvgArray>(agg_arrays_[i]);
                array.int_sums[group_id] += static_cast<__int128>(ReadIntegerRow(col, row));
                ++array.counts[group_id];
                break;
            }
            case AggregationType::Distinct: {
                const core::Column& col = *agg_cols[i];
                if (col.IsNull(row)) {
                    break;
                }
                auto& array = std::get<DistinctArray>(agg_arrays_[i]);
                if (array.is_string) {
                    array.strings[group_id].emplace(ReadStringRow(col, row));
                } else {
                    array.ints[group_id].insert(ReadIntegerRow(col, row));
                }
                break;
            }
            case AggregationType::Min:
            case AggregationType::Max: {
                const core::Column& col = *agg_cols[i];
                if (col.IsNull(row)) {
                    break;
                }
                auto& array = std::get<MinMaxArray>(agg_arrays_[i]);
                bool is_min = aggregations_[i].type == AggregationType::Min;
                uint8_t& has_value = array.has_value[group_id];
                if (array.value_type == core::DataType::String) {
                    auto value = ReadStringRow(col, row);
                    auto& slot = array.string_values[group_id];
                    if (has_value == 0 || (is_min ? value < slot : value > slot)) {
                        slot.assign(value.data(), value.size());
                        has_value = 1;
                    }
                } else if (array.value_type == core::DataType::Double) {
                    double value = ReadDoubleRow(col, row);
                    auto& slot = array.double_values[group_id];
                    if (has_value == 0 || (is_min ? value < slot : value > slot)) {
                        slot = value;
                        has_value = 1;
                    }
                } else {
                    int64_t value = ReadIntegerRow(col, row);
                    auto& slot = array.int_values[group_id];
                    if (has_value == 0 || (is_min ? value < slot : value > slot)) {
                        slot = value;
                        has_value = 1;
                    }
                }
                break;
            }
        }
    }
}

void HashAggregationSink::AppendAggResult(size_t agg_index, uint32_t group_id,
                                          core::Column& out) const {
    const AggArray& array = agg_arrays_[agg_index];
    switch (aggregations_[agg_index].type) {
        case AggregationType::Count:
            AppendInteger(out, std::get<CountArray>(array).values[group_id]);
            return;
        case AggregationType::Sum: {
            auto& sum = std::get<SumArray>(array);
            if (sum.has_value[group_id] == 0) {
                out.AppendNull();
            } else if (sum.is_double) {
                AppendDouble(out, static_cast<double>(sum.double_values[group_id]));
            } else {
                AppendInteger(out, sum.int_values[group_id]);
            }
            return;
        }
        case AggregationType::Avg: {
            auto& avg = std::get<AvgArray>(array);
            if (avg.counts[group_id] == 0) {
                out.AppendNull();
            } else {
                AppendInteger(out,
                              static_cast<int64_t>(avg.int_sums[group_id] /
                                                   static_cast<__int128>(avg.counts[group_id])));
            }
            return;
        }
        case AggregationType::Distinct: {
            auto& distinct = std::get<DistinctArray>(array);
            size_t count = distinct.is_string ? distinct.strings[group_id].size()
                                              : distinct.ints[group_id].size();
            AppendInteger(out, static_cast<int64_t>(count));
            return;
        }
        case AggregationType::Min:
        case AggregationType::Max: {
            auto& min_max = std::get<MinMaxArray>(array);
            if (min_max.has_value[group_id] == 0) {
                out.AppendNull();
            } else if (min_max.value_type == core::DataType::String) {
                out.AppendFromString(min_max.string_values[group_id]);
            } else if (min_max.value_type == core::DataType::Double) {
                AppendDouble(out, min_max.double_values[group_id]);
            } else {
                AppendInteger(out, min_max.int_values[group_id]);
            }
            return;
        }
    }
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}

void HashAggregationSink::ConsumeInt64(const core::Column& key_col,
                                       const std::vector<const core::Column*>& agg_cols,
                                       const std::vector<uint32_t>* selection, size_t rows) {
    VisitIntegerCol(key_col, [&](const auto& typed) {
        const util::BitVector* mask = typed.IsNullable() ? &typed.GetNullMask() : nullptr;
        ForSelectedRows(selection, rows, [&](size_t row) {
            if (mask != nullptr && mask->Get(row)) {
                return;
            }
            int64_t key = static_cast<int64_t>(ReadTypedValue(typed, row));
            auto it = int64_groups_.find(key);
            uint32_t group_id;
            if (it == int64_groups_.end()) {
                group_id = EmplaceGroup();
                int64_groups_.emplace(key, group_id);
                int64_group_keys_.push_back(key);
            } else {
                group_id = it->second;
            }
            UpdateAggsForRow(group_id, agg_cols, row);
        });
    });
}

void HashAggregationSink::ConsumeString(const core::Column& key_col,
                                        const std::vector<const core::Column*>& agg_cols,
                                        const std::vector<uint32_t>* selection, size_t rows) {
    auto& s = static_cast<const core::StringColumn&>(key_col);
    ForSelectedRows(selection, rows, [&](size_t row) {
        if (s.IsNull(row)) {
            return;
        }
        auto key = s.Get(row);
        auto it = string_groups_.find(key);
        uint32_t group_id;
        if (it == string_groups_.end()) {
            group_id = EmplaceGroup();
            string_groups_.emplace(std::string(key), group_id);
            string_group_keys_.emplace_back(key);
        } else {
            group_id = it->second;
        }
        UpdateAggsForRow(group_id, agg_cols, row);
    });
}

void HashAggregationSink::ConsumeInt64Pair(const core::Column& first_key_col,
                                           const core::Column& second_key_col,
                                           const std::vector<const core::Column*>& agg_cols,
                                           const std::vector<uint32_t>* selection, size_t rows) {
    VisitIntegerCol(first_key_col, [&](const auto& first_typed) {
        VisitIntegerCol(second_key_col, [&](const auto& second_typed) {
            const util::BitVector* first_mask =
                first_typed.IsNullable() ? &first_typed.GetNullMask() : nullptr;
            const util::BitVector* second_mask =
                second_typed.IsNullable() ? &second_typed.GetNullMask() : nullptr;
            ForSelectedRows(selection, rows, [&](size_t row) {
                if ((first_mask != nullptr && first_mask->Get(row)) ||
                    (second_mask != nullptr && second_mask->Get(row))) {
                    return;
                }
                Int64Pair key{static_cast<int64_t>(ReadTypedValue(first_typed, row)),
                              static_cast<int64_t>(ReadTypedValue(second_typed, row))};
                auto it = int64_pair_groups_.find(key);
                uint32_t group_id;
                if (it == int64_pair_groups_.end()) {
                    group_id = EmplaceGroup();
                    int64_pair_groups_.emplace(key, group_id);
                    int64_pair_group_keys_.push_back(key);
                } else {
                    group_id = it->second;
                }
                UpdateAggsForRow(group_id, agg_cols, row);
            });
        });
    });
}

void HashAggregationSink::ConsumeComposite(const std::vector<const core::Column*>& key_cols,
                                           const std::vector<const core::Column*>& agg_cols,
                                           const std::vector<uint32_t>* selection, size_t rows) {
    CompositeKey key;
    key.reserve(key_cols.size());
    ForSelectedRows(selection, rows, [&](size_t row) {
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
            return;
        }

        auto it = composite_groups_.find(key);
        uint32_t group_id;
        if (it == composite_groups_.end()) {
            group_id = EmplaceGroup();
            composite_group_keys_.push_back(std::move(key));
            composite_groups_.insert(group_id);
        } else {
            group_id = *it;
        }
        UpdateAggsForRow(group_id, agg_cols, row);
    });
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

    const std::vector<uint32_t>* selection = batch.HasSelection() ? &batch.Selection() : nullptr;

    switch (mode_) {
        case KeyMode::Int64:
            ConsumeInt64(*key_cols[0], agg_cols, selection, rows);
            return;
        case KeyMode::String:
            ConsumeString(*key_cols[0], agg_cols, selection, rows);
            return;
        case KeyMode::Int64Pair:
            ConsumeInt64Pair(*key_cols[0], *key_cols[1], agg_cols, selection, rows);
            return;
        case KeyMode::Composite:
            ConsumeComposite(key_cols, agg_cols, selection, rows);
            return;
    }
}

void HashAggregationSink::Finalize() {
    core::Batch out(output_schema_, groups_count_);

    auto append_aggs = [&](uint32_t group_id) {
        for (size_t i = 0; i < aggregations_.size(); ++i) {
            AppendAggResult(i, group_id, out.ColumnAt(keys_.size() + i));
        }
    };

    if (mode_ == KeyMode::Int64) {
        for (uint32_t group_id = 0; group_id < groups_count_; ++group_id) {
            AppendInteger(out.ColumnAt(0), int64_group_keys_[group_id]);
            append_aggs(group_id);
        }
    } else if (mode_ == KeyMode::String) {
        for (uint32_t group_id = 0; group_id < groups_count_; ++group_id) {
            static_cast<core::StringColumn&>(out.ColumnAt(0)).Append(string_group_keys_[group_id]);
            append_aggs(group_id);
        }
    } else if (mode_ == KeyMode::Int64Pair) {
        for (uint32_t group_id = 0; group_id < groups_count_; ++group_id) {
            const auto& key = int64_pair_group_keys_[group_id];
            AppendInteger(out.ColumnAt(0), key.first);
            AppendInteger(out.ColumnAt(1), key.second);
            append_aggs(group_id);
        }
    } else {
        for (uint32_t group_id = 0; group_id < groups_count_; ++group_id) {
            const auto& key = composite_group_keys_[group_id];
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
            append_aggs(group_id);
        }
    }

    downstream_.Consume(std::move(out));
    downstream_.Finalize();
}
}  // namespace columnar::exec

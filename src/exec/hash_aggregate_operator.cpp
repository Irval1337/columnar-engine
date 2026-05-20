#include <exec/hash_aggregate_operator.h>

#include <core/columns/dictionary_string_column.h>
#include <core/columns/string_column.h>
#include <core/datatype.h>
#include <core/field.h>
#include <exec/column_helpers.h>
#include <exec/expression.h>
#include <exec/kernel.h>
#include <util/macro.h>

#include <cassert>
#include <functional>
#include <limits>
#include <string_view>
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

HashAggregationSink::CompositeKeyStorage::CompositeKeyStorage(
    const std::vector<ProjectionUnit>& keys)
    : groups_(0, GroupHash{this}, GroupEq{this}) {
    part_kinds_.reserve(keys.size());
    int_group_keys_.resize(keys.size());
    string_group_keys_.resize(keys.size());
    for (auto& key : keys) {
        auto type = GetExpressionType(*key.expression);
        part_kinds_.push_back(type == core::DataType::String ? PartKind::String : PartKind::Int64);
    }
}

HashAggregationSink::CompositeKeyStorage::ProbeKey
HashAggregationSink::CompositeKeyStorage::MakeProbe(const Batch& batch, size_t row) const noexcept {
    return ProbeKey{&batch, row, batch.HashRow(row)};
}

bool HashAggregationSink::CompositeKeyStorage::LookupGroup(const ProbeKey& key,
                                                           uint32_t& group_id) const {
    auto it = groups_.find(key);
    if (it == groups_.end()) {
        return false;
    }
    group_id = *it;
    return true;
}

void HashAggregationSink::CompositeKeyStorage::InsertGroup(uint32_t group_id, const ProbeKey& key) {
    assert(group_id == group_hashes_.size());
    group_hashes_.push_back(key.hash);
    for (size_t i = 0; i < part_kinds_.size(); ++i) {
        if (part_kinds_[i] == PartKind::String) {
            auto value = key.batch->ReadString(i, key.row);
            string_group_keys_[i].emplace_back(value);
        } else {
            int_group_keys_[i].push_back(key.batch->ReadInt(i, key.row));
        }
    }
    groups_.insert(group_id);
}

void HashAggregationSink::CompositeKeyStorage::AppendKey(uint32_t group_id, size_t key_index,
                                                         core::Column& out) const {
    if (part_kinds_[key_index] == PartKind::String) {
        out.AppendFromString(string_group_keys_[key_index][group_id]);
    } else {
        AppendInteger(out, int_group_keys_[key_index][group_id]);
    }
}

size_t HashAggregationSink::CompositeKeyStorage::HashGroup(uint32_t group_id) const noexcept {
    return group_hashes_[group_id];
}

bool HashAggregationSink::CompositeKeyStorage::GroupsEqual(uint32_t lhs,
                                                           uint32_t rhs) const noexcept {
    if (group_hashes_[lhs] != group_hashes_[rhs]) {
        return false;
    }
    for (size_t i = 0; i < part_kinds_.size(); ++i) {
        if (part_kinds_[i] == PartKind::String) {
            if (string_group_keys_[i][lhs] != string_group_keys_[i][rhs]) {
                return false;
            }
        } else if (int_group_keys_[i][lhs] != int_group_keys_[i][rhs]) {
            return false;
        }
    }
    return true;
}

bool HashAggregationSink::CompositeKeyStorage::GroupEqualsProbe(
    uint32_t group_id, const ProbeKey& key) const noexcept {
    if (group_hashes_[group_id] != key.hash) {
        return false;
    }
    for (size_t i = 0; i < part_kinds_.size(); ++i) {
        if (part_kinds_[i] == PartKind::String) {
            auto stored_value = std::string_view(string_group_keys_[i][group_id]);
            if (stored_value != key.batch->ReadString(i, key.row)) {
                return false;
            }
        } else if (int_group_keys_[i][group_id] != key.batch->ReadInt(i, key.row)) {
            return false;
        }
    }
    return true;
}

bool HashAggregationSink::CompositeKeyStorage::ProbesEqual(const ProbeKey& lhs,
                                                           const ProbeKey& rhs) const noexcept {
    if (lhs.hash != rhs.hash) {
        return false;
    }
    for (size_t i = 0; i < part_kinds_.size(); ++i) {
        if (part_kinds_[i] == PartKind::String) {
            if (lhs.batch->ReadString(i, lhs.row) != rhs.batch->ReadString(i, rhs.row)) {
                return false;
            }
        } else if (lhs.batch->ReadInt(i, lhs.row) != rhs.batch->ReadInt(i, rhs.row)) {
            return false;
        }
    }
    return true;
}

size_t HashAggregationSink::CompositeKeyStorage::GroupHash::operator()(
    uint32_t group_id) const noexcept {
    return storage->HashGroup(group_id);
}

size_t HashAggregationSink::CompositeKeyStorage::GroupHash::operator()(
    const ProbeKey& key) const noexcept {
    return key.hash;
}

bool HashAggregationSink::CompositeKeyStorage::GroupEq::operator()(uint32_t lhs,
                                                                   uint32_t rhs) const noexcept {
    return storage->GroupsEqual(lhs, rhs);
}

bool HashAggregationSink::CompositeKeyStorage::GroupEq::operator()(
    uint32_t lhs, const ProbeKey& rhs) const noexcept {
    return storage->GroupEqualsProbe(lhs, rhs);
}

bool HashAggregationSink::CompositeKeyStorage::GroupEq::operator()(const ProbeKey& lhs,
                                                                   uint32_t rhs) const noexcept {
    return storage->GroupEqualsProbe(rhs, lhs);
}

bool HashAggregationSink::CompositeKeyStorage::GroupEq::operator()(
    const ProbeKey& lhs, const ProbeKey& rhs) const noexcept {
    return storage->ProbesEqual(lhs, rhs);
}

HashAggregationSink::CompositeKeyStorage::Batch::Batch(
    const CompositeKeyStorage& storage, const std::vector<const core::Column*>& columns)
    : storage_(storage),
      columns_(columns),
      dictionary_columns_(columns.size(), nullptr),
      dictionary_hashes_(columns.size()) {
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (storage_.part_kinds_[i] != PartKind::String) {
            continue;
        }
        dictionary_columns_[i] = dynamic_cast<const core::DictionaryStringColumn*>(columns_[i]);
        if (dictionary_columns_[i] == nullptr) {
            continue;
        }
        auto& hashes = dictionary_hashes_[i];
        hashes.resize(dictionary_columns_[i]->DictSize());
        for (uint32_t id = 0; id < hashes.size(); ++id) {
            hashes[id] = std::hash<std::string_view>{}(dictionary_columns_[i]->DictValue(id));
        }
    }
}

bool HashAggregationSink::CompositeKeyStorage::Batch::HasNull(size_t row) const {
    for (auto* col : columns_) {
        if (col->IsNull(row)) {
            return true;
        }
    }
    return false;
}

size_t HashAggregationSink::CompositeKeyStorage::Batch::HashRow(size_t row) const noexcept {
    size_t seed = storage_.part_kinds_.size();
    for (size_t i = 0; i < storage_.part_kinds_.size(); ++i) {
        if (storage_.part_kinds_[i] == PartKind::String) {
            size_t part_hash;
            if (dictionary_columns_[i] != nullptr) {
                part_hash = dictionary_hashes_[i][dictionary_columns_[i]->GetId(row)];
            } else {
                part_hash = std::hash<std::string_view>{}(ReadString(i, row));
            }
            HashCombine(seed, part_hash);
        } else {
            HashCombine(seed, std::hash<int64_t>{}(ReadInt(i, row)));
        }
    }
    return seed;
}

int64_t HashAggregationSink::CompositeKeyStorage::Batch::ReadInt(size_t key_index,
                                                                 size_t row) const {
    return ReadIntegerRow(*columns_[key_index], row);
}

std::string_view HashAggregationSink::CompositeKeyStorage::Batch::ReadString(size_t key_index,
                                                                             size_t row) const {
    if (dictionary_columns_[key_index] != nullptr) {
        return dictionary_columns_[key_index]->Get(row);
    }
    return static_cast<const core::StringColumn&>(*columns_[key_index]).Get(row);
}

size_t HashAggregationSink::Int64PairHash::operator()(const Int64Pair& key) const noexcept {
    size_t seed = 2;
    HashCombine(seed, std::hash<int64_t>{}(key.first));
    HashCombine(seed, std::hash<int64_t>{}(key.second));
    return seed;
}

HashAggregationSink::KeyMode HashAggregationSink::SelectKeyMode(
    const std::vector<ProjectionUnit>& keys) {
    if (keys.size() == 2 && HasIntegerValue(GetExpressionType(*keys[0].expression)) &&
        HasIntegerValue(GetExpressionType(*keys[1].expression))) {
        return KeyMode::Int64Pair;
    }
    if (keys.size() != 1) {
        return KeyMode::Composite;
    }
    auto type = GetExpressionType(*keys[0].expression);
    if (type == core::DataType::String) {
        return KeyMode::String;
    }
    if (HasIntegerValue(type)) {
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
      composite_keys_(keys_) {
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
        auto type = aggregations_[i].type;
        if (type == AggregationType::Count) {
            ++std::get<CountArray>(agg_arrays_[i]).values[group_id];
            continue;
        }
        const core::Column& col = *agg_cols[i];
        if (col.IsNull(row)) {
            continue;
        }
        switch (type) {
            case AggregationType::Sum: {
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
                auto& array = std::get<AvgArray>(agg_arrays_[i]);
                array.int_sums[group_id] += static_cast<__int128>(ReadIntegerRow(col, row));
                ++array.counts[group_id];
                break;
            }
            case AggregationType::Distinct: {
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
                auto& array = std::get<MinMaxArray>(agg_arrays_[i]);
                bool is_min = type == AggregationType::Min;
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
            default:
                THROW_RUNTIME_ERROR("Unsupported aggregate type");
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
    if (auto* dict = dynamic_cast<const core::DictionaryStringColumn*>(&key_col)) {
        ConsumeDictionaryString(*dict, agg_cols, selection, rows);
        return;
    }
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

void HashAggregationSink::ConsumeDictionaryString(const core::DictionaryStringColumn& key_col,
                                                  const std::vector<const core::Column*>& agg_cols,
                                                  const std::vector<uint32_t>* selection,
                                                  size_t rows) {
    constexpr uint32_t kUnknownGroup = std::numeric_limits<uint32_t>::max();
    std::vector<uint32_t> id_to_group(key_col.DictSize(), kUnknownGroup);

    auto resolve_group = [&](uint32_t local_id) {
        uint32_t group_id = id_to_group[local_id];
        if (group_id != kUnknownGroup) {
            return group_id;
        }
        auto key = key_col.DictValue(local_id);
        auto it = string_groups_.find(key);
        if (it == string_groups_.end()) {
            group_id = EmplaceGroup();
            string_groups_.emplace(std::string(key), group_id);
            string_group_keys_.emplace_back(key);
        } else {
            group_id = it->second;
        }
        id_to_group[local_id] = group_id;
        return group_id;
    };

    ForSelectedRows(selection, rows, [&](size_t row) {
        if (key_col.IsNull(row)) {
            return;
        }
        UpdateAggsForRow(resolve_group(key_col.GetId(row)), agg_cols, row);
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
    CompositeKeyStorage::Batch composite_batch(composite_keys_, key_cols);
    ForSelectedRows(selection, rows, [&](size_t row) {
        if (composite_batch.HasNull(row)) {
            return;
        }

        auto key = composite_keys_.MakeProbe(composite_batch, row);
        uint32_t group_id;
        if (!composite_keys_.LookupGroup(key, group_id)) {
            group_id = EmplaceGroup();
            composite_keys_.InsertGroup(group_id, key);
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
            for (size_t i = 0; i < keys_.size(); ++i) {
                composite_keys_.AppendKey(group_id, i, out.ColumnAt(i));
            }
            append_aggs(group_id);
        }
    }

    downstream_.Consume(std::move(out));
    downstream_.Finalize();
}
}  // namespace columnar::exec

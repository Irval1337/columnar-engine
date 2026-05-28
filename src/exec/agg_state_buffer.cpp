#include <exec/agg_state_buffer.h>

#include <core/columns/string_column.h>
#include <exec/column_row_access.h>
#include <exec/expression/eval.h>
#include <util/macro.h>

#include <limits>

namespace columnar::exec {
void agg_array::Count::Reserve(size_t n) {
    values.reserve(n);
}

void agg_array::Count::PushDefault() {
    values.push_back(0);
}

void agg_array::Sum::Reserve(size_t n) {
    has_value.Reserve(n);
    if (is_double) {
        double_values.reserve(n);
    } else {
        int_values.reserve(n);
    }
}

void agg_array::Sum::PushDefault() {
    has_value.PushBack(false);
    if (is_double) {
        double_values.push_back(0.0L);
    } else {
        int_values.push_back(0);
    }
}

void agg_array::Avg::Reserve(size_t n) {
    int_sums.reserve(n);
    counts.reserve(n);
}

void agg_array::Avg::PushDefault() {
    int_sums.push_back(0);
    counts.push_back(0);
}

void agg_array::MinMax::Reserve(size_t n) {
    has_value.Reserve(n);
    if (value_type == core::DataType::String) {
        string_values.reserve(n);
    } else if (value_type == core::DataType::Double) {
        double_values.reserve(n);
    } else {
        int_values.reserve(n);
    }
}

void agg_array::MinMax::PushDefault() {
    has_value.PushBack(false);
    if (value_type == core::DataType::String) {
        string_values.emplace_back();
    } else if (value_type == core::DataType::Double) {
        double_values.push_back(0.0);
    } else {
        int_values.push_back(0);
    }
}

void agg_array::Distinct::Reserve(size_t n) {
    if (is_string) {
        strings.reserve(n);
    } else {
        ints.reserve(n);
    }
}

void agg_array::Distinct::PushDefault() {
    if (is_string) {
        strings.emplace_back();
    } else {
        ints.emplace_back();
    }
}

agg_array::Any AggStateBuffer::MakeArray(const AggregationUnit& unit) {
    switch (unit.type) {
        case AggregationType::Count:
            return agg_array::Count{};
        case AggregationType::Sum: {
            agg_array::Sum array;
            array.is_double = GetExpressionType(*unit.expression) == core::DataType::Double;
            return array;
        }
        case AggregationType::Avg:
            return agg_array::Avg{};
        case AggregationType::Distinct: {
            agg_array::Distinct array;
            array.is_string = GetExpressionType(*unit.expression) == core::DataType::String;
            return array;
        }
        case AggregationType::Min:
        case AggregationType::Max: {
            agg_array::MinMax array;
            array.value_type = GetExpressionType(*unit.expression);
            return array;
        }
    }
    THROW_RUNTIME_ERROR("AggStateBuffer: unsupported aggregate type " +
                        std::to_string(static_cast<int>(unit.type)));
}

AggStateBuffer::AggStateBuffer(const std::vector<AggregationUnit>& aggregations,
                               util::StringArena& arena)
    : aggregations_(aggregations), arena_(arena) {
    arrays_.reserve(aggregations_.size());
    for (auto& unit : aggregations_) {
        arrays_.push_back(MakeArray(unit));
    }
    if (aggregations_.size() == 1 && aggregations_[0].type == AggregationType::Count) {
        single_count_ = &std::get<agg_array::Count>(arrays_[0]);
    }
}

uint32_t AggStateBuffer::EmplaceGroup() {
    if (groups_count_ == std::numeric_limits<uint32_t>::max()) {
        THROW_RUNTIME_ERROR("Too many hash aggregation groups");
    }
    uint32_t group_id = groups_count_++;
    if (single_count_ != nullptr) {
        single_count_->PushDefault();
        return group_id;
    }
    for (auto& array : arrays_) {
        std::visit([](auto& typed) { typed.PushDefault(); }, array);
    }
    return group_id;
}

void AggStateBuffer::Reserve(size_t n) {
    if (single_count_ != nullptr) {
        single_count_->Reserve(n);
        return;
    }
    for (auto& array : arrays_) {
        std::visit([&](auto& typed) { typed.Reserve(n); }, array);
    }
}

void AggStateBuffer::UpdateRow(uint32_t group_id, const std::vector<const core::Column*>& agg_cols,
                               size_t row) {
    for (size_t i = 0; i < aggregations_.size(); ++i) {
        auto type = aggregations_[i].type;
        if (type == AggregationType::Count) {
            ++std::get<agg_array::Count>(arrays_[i]).values[group_id];
            continue;
        }
        const core::Column& col = *agg_cols[i];
        if (col.IsNull(row)) {
            continue;
        }
        switch (type) {
            case AggregationType::Sum: {
                auto& array = std::get<agg_array::Sum>(arrays_[i]);
                array.has_value.Set(group_id);
                if (array.is_double) {
                    array.double_values[group_id] +=
                        static_cast<long double>(ReadDoubleRow(col, row));
                } else {
                    array.int_values[group_id] += ReadIntegerRow(col, row);
                }
                break;
            }
            case AggregationType::Avg: {
                auto& array = std::get<agg_array::Avg>(arrays_[i]);
                array.int_sums[group_id] += static_cast<__int128>(ReadIntegerRow(col, row));
                ++array.counts[group_id];
                break;
            }
            case AggregationType::Distinct: {
                auto& array = std::get<agg_array::Distinct>(arrays_[i]);
                if (array.is_string) {
                    auto value = ReadStringRow(col, row);
                    auto& set = array.strings[group_id];
                    if (!set.contains(value)) {
                        set.insert(arena_.Intern(value));
                    }
                } else {
                    array.ints[group_id].insert(ReadIntegerRow(col, row));
                }
                break;
            }
            case AggregationType::Min:
            case AggregationType::Max: {
                auto& array = std::get<agg_array::MinMax>(arrays_[i]);
                bool is_min = type == AggregationType::Min;
                bool has = array.has_value.Get(group_id);
                if (array.value_type == core::DataType::String) {
                    auto value = ReadStringRow(col, row);
                    auto& slot = array.string_values[group_id];
                    if (!has || (is_min ? value < slot : value > slot)) {
                        slot.assign(value.data(), value.size());
                        array.has_value.Set(group_id);
                    }
                } else if (array.value_type == core::DataType::Double) {
                    double value = ReadDoubleRow(col, row);
                    auto& slot = array.double_values[group_id];
                    if (!has || (is_min ? value < slot : value > slot)) {
                        slot = value;
                        array.has_value.Set(group_id);
                    }
                } else {
                    int64_t value = ReadIntegerRow(col, row);
                    auto& slot = array.int_values[group_id];
                    if (!has || (is_min ? value < slot : value > slot)) {
                        slot = value;
                        array.has_value.Set(group_id);
                    }
                }
                break;
            }
            default:
                THROW_RUNTIME_ERROR("AggStateBuffer: unsupported aggregate type " +
                                    std::to_string(static_cast<int>(type)));
        }
    }
}

void AggStateBuffer::AppendResult(size_t agg_index, uint32_t group_id, core::Column& out) const {
    const agg_array::Any& array = arrays_[agg_index];
    switch (aggregations_[agg_index].type) {
        case AggregationType::Count:
            AppendInteger(out, std::get<agg_array::Count>(array).values[group_id]);
            return;
        case AggregationType::Sum: {
            auto& sum = std::get<agg_array::Sum>(array);
            if (!sum.has_value.Get(group_id)) {
                out.AppendNull();
            } else if (sum.is_double) {
                AppendDouble(out, static_cast<double>(sum.double_values[group_id]));
            } else {
                AppendInteger(out, sum.int_values[group_id]);
            }
            return;
        }
        case AggregationType::Avg: {
            auto& avg = std::get<agg_array::Avg>(array);
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
            auto& distinct = std::get<agg_array::Distinct>(array);
            size_t count = distinct.is_string ? distinct.strings[group_id].size()
                                              : distinct.ints[group_id].size();
            AppendInteger(out, static_cast<int64_t>(count));
            return;
        }
        case AggregationType::Min:
        case AggregationType::Max: {
            auto& min_max = std::get<agg_array::MinMax>(array);
            if (!min_max.has_value.Get(group_id)) {
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
    THROW_RUNTIME_ERROR("AggStateBuffer: unsupported aggregate type " +
                        std::to_string(static_cast<int>(aggregations_[agg_index].type)));
}
}  // namespace columnar::exec

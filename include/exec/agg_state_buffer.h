#pragma once

#include <core/column.h>
#include <core/datatype.h>
#include <exec/aggregation.h>
#include <util/bit_vector.h>
#include <util/string_arena.h>

#include <absl/container/flat_hash_set.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace columnar::exec {
namespace agg_array {
struct Count {
    std::vector<int64_t> values;

    void Reserve(size_t n);
    void PushDefault();
};

struct Sum {
    bool is_double = false;
    util::BitVector has_value;
    std::vector<int64_t> int_values;
    std::vector<long double> double_values;

    void Reserve(size_t n);
    void PushDefault();
};

struct Avg {
    std::vector<__int128> int_sums;
    std::vector<uint64_t> counts;

    void Reserve(size_t n);
    void PushDefault();
};

struct MinMax {
    core::DataType value_type = core::DataType::Int64;
    util::BitVector has_value;
    std::vector<int64_t> int_values;
    std::vector<double> double_values;
    std::vector<std::string> string_values;

    void Reserve(size_t n);
    void PushDefault();
};

struct Distinct {
    bool is_string = false;
    std::vector<absl::flat_hash_set<int64_t>> ints;
    std::vector<absl::flat_hash_set<std::string_view>> strings;

    void Reserve(size_t n);
    void PushDefault();
};

using Any = std::variant<Count, Sum, Avg, MinMax, Distinct>;
}  // namespace agg_array


class AggStateBuffer {
public:
    AggStateBuffer(const std::vector<AggregationUnit>& aggregations, util::StringArena& arena);

    AggStateBuffer(const AggStateBuffer&) = delete;
    AggStateBuffer& operator=(const AggStateBuffer&) = delete;
    AggStateBuffer(AggStateBuffer&&) = delete;
    AggStateBuffer& operator=(AggStateBuffer&&) = delete;

    uint32_t GroupsCount() const noexcept {
        return groups_count_;
    }

    uint32_t EmplaceGroup();

    void Reserve(size_t n);

    void OnRow(uint32_t group_id, const std::vector<const core::Column*>& agg_cols, size_t row) {
        if (single_count_ != nullptr) {
            ++single_count_->values[group_id];
            return;
        }
        UpdateRow(group_id, agg_cols, row);
    }

    void AppendResult(size_t agg_index, uint32_t group_id, core::Column& out) const;

private:
    static agg_array::Any MakeArray(const AggregationUnit& unit);

    void UpdateRow(uint32_t group_id, const std::vector<const core::Column*>& agg_cols, size_t row);

    const std::vector<AggregationUnit>& aggregations_;
    util::StringArena& arena_;
    std::vector<agg_array::Any> arrays_;
    agg_array::Count* single_count_ = nullptr;
    uint32_t groups_count_ = 0;
};
}  // namespace columnar::exec

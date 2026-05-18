#pragma once

#include <core/datatype.h>
#include <core/schema.h>
#include <exec/aggregation.h>
#include <exec/operator.h>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>
#include <vector>

namespace columnar::exec {
class HashAggregationSink final : public IOperator {
public:
    HashAggregationSink(IOperator& downstream, std::vector<ProjectionUnit> keys,
                        std::vector<AggregationUnit> aggregations);

    HashAggregationSink(const HashAggregationSink&) = delete;
    HashAggregationSink& operator=(const HashAggregationSink&) = delete;
    HashAggregationSink(HashAggregationSink&&) = delete;
    HashAggregationSink& operator=(HashAggregationSink&&) = delete;

    void Consume(core::Batch batch) override;

    void Finalize() override;

private:
    enum class KeyMode { Int64, String, Int64Pair, Composite };

    struct Int64Pair {
        int64_t first = 0;
        int64_t second = 0;

        bool operator==(const Int64Pair&) const = default;
    };

    using CompositeKey = std::vector<std::variant<int64_t, std::string>>;

    struct CompositeGroupHash {
        using is_transparent = void;  // NOLINT(readability-identifier-naming)
        const std::vector<CompositeKey>* keys = nullptr;
        size_t operator()(uint32_t group_id) const noexcept;
        size_t operator()(const CompositeKey& key) const noexcept;
    };

    struct CompositeGroupEq {
        using is_transparent = void;  // NOLINT(readability-identifier-naming)
        const std::vector<CompositeKey>* keys = nullptr;
        bool operator()(uint32_t lhs, uint32_t rhs) const noexcept;
        bool operator()(uint32_t lhs, const CompositeKey& rhs) const noexcept;
        bool operator()(const CompositeKey& lhs, uint32_t rhs) const noexcept;
        bool operator()(const CompositeKey& lhs, const CompositeKey& rhs) const noexcept;
    };

    struct Int64PairHash {
        size_t operator()(const Int64Pair& key) const noexcept;
    };

    struct StringHash {
        using is_transparent = void;  // NOLINT(readability-identifier-naming)
        size_t operator()(std::string_view s) const noexcept {
            return std::hash<std::string_view>{}(s);
        }
        size_t operator()(const std::string& s) const noexcept {
            return std::hash<std::string_view>{}(s);
        }
    };

    struct StringEq {
        using is_transparent = void;  // NOLINT(readability-identifier-naming)
        bool operator()(std::string_view a, std::string_view b) const noexcept {
            return a == b;
        }
        bool operator()(const std::string& a, std::string_view b) const noexcept {
            return a == b;
        }
        bool operator()(std::string_view a, const std::string& b) const noexcept {
            return a == b;
        }
        bool operator()(const std::string& a, const std::string& b) const noexcept {
            return a == b;
        }
    };

    struct CountArray {
        std::vector<int64_t> values;

        void PushDefault() {
            values.push_back(0);
        }
    };

    struct SumArray {
        bool is_double = false;
        std::vector<uint8_t> has_value;
        std::vector<int64_t> int_values;
        std::vector<long double> double_values;

        void PushDefault() {
            has_value.push_back(0);
            if (is_double) {
                double_values.push_back(0.0L);
            } else {
                int_values.push_back(0);
            }
        }
    };

    struct AvgArray {
        std::vector<__int128> int_sums;
        std::vector<uint64_t> counts;

        void PushDefault() {
            int_sums.push_back(0);
            counts.push_back(0);
        }
    };

    struct MinMaxArray {
        core::DataType value_type = core::DataType::Int64;
        std::vector<uint8_t> has_value;
        std::vector<int64_t> int_values;
        std::vector<double> double_values;
        std::vector<std::string> string_values;

        void PushDefault() {
            has_value.push_back(0);
            if (value_type == core::DataType::String) {
                string_values.emplace_back();
            } else if (value_type == core::DataType::Double) {
                double_values.push_back(0.0);
            } else {
                int_values.push_back(0);
            }
        }
    };

    struct DistinctArray {
        bool is_string = false;
        std::vector<std::unordered_set<int64_t>> ints;
        std::vector<std::unordered_set<std::string>> strings;

        void PushDefault() {
            if (is_string) {
                strings.emplace_back();
            } else {
                ints.emplace_back();
            }
        }
    };
    using AggArray = std::variant<CountArray, SumArray, AvgArray, MinMaxArray, DistinctArray>;

    using Int64Groups = absl::flat_hash_map<int64_t, uint32_t>;
    using StringGroups = absl::flat_hash_map<std::string, uint32_t, StringHash, StringEq>;
    using Int64PairGroups = absl::flat_hash_map<Int64Pair, uint32_t, Int64PairHash>;
    using CompositeGroups = absl::flat_hash_set<uint32_t, CompositeGroupHash, CompositeGroupEq>;

    static KeyMode SelectKeyMode(const std::vector<ProjectionUnit>& keys);

    static size_t HashCompositeKey(const CompositeKey& key) noexcept;

    uint32_t EmplaceGroup();

    void UpdateAggsForRow(uint32_t group_id, const std::vector<const core::Column*>& agg_cols,
                          size_t row);

    void AppendAggResult(size_t agg_index, uint32_t group_id, core::Column& out) const;

    void ConsumeInt64(const core::Column& key_col, const std::vector<const core::Column*>& agg_cols,
                      const std::vector<uint32_t>* selection, size_t rows);
    void ConsumeString(const core::Column& key_col,
                       const std::vector<const core::Column*>& agg_cols,
                       const std::vector<uint32_t>* selection, size_t rows);
    void ConsumeInt64Pair(const core::Column& first_key_col, const core::Column& second_key_col,
                          const std::vector<const core::Column*>& agg_cols,
                          const std::vector<uint32_t>* selection, size_t rows);
    void ConsumeComposite(const std::vector<const core::Column*>& key_cols,
                          const std::vector<const core::Column*>& agg_cols,
                          const std::vector<uint32_t>* selection, size_t rows);

    IOperator& downstream_;
    std::vector<ProjectionUnit> keys_;
    std::vector<AggregationUnit> aggregations_;
    core::Schema output_schema_;
    KeyMode mode_;
    bool needs_dense_;
    uint32_t groups_count_ = 0;
    std::vector<AggArray> agg_arrays_;
    std::vector<int64_t> int64_group_keys_;
    std::vector<std::string> string_group_keys_;
    std::vector<Int64Pair> int64_pair_group_keys_;
    std::vector<CompositeKey> composite_group_keys_;
    Int64Groups int64_groups_;
    StringGroups string_groups_;
    Int64PairGroups int64_pair_groups_;
    CompositeGroups composite_groups_;
};
}  // namespace columnar::exec

#pragma once

#include <core/datatype.h>
#include <core/columns/dictionary_string_column.h>
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

        void Reserve(size_t n) {
            values.reserve(n);
        }

        void PushDefault() {
            values.push_back(0);
        }
    };

    struct SumArray {
        bool is_double = false;
        std::vector<uint8_t> has_value;
        std::vector<int64_t> int_values;
        std::vector<long double> double_values;

        void Reserve(size_t n) {
            has_value.reserve(n);
            if (is_double) {
                double_values.reserve(n);
            } else {
                int_values.reserve(n);
            }
        }

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

        void Reserve(size_t n) {
            int_sums.reserve(n);
            counts.reserve(n);
        }

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

        void Reserve(size_t n) {
            has_value.reserve(n);
            if (value_type == core::DataType::String) {
                string_values.reserve(n);
            } else if (value_type == core::DataType::Double) {
                double_values.reserve(n);
            } else {
                int_values.reserve(n);
            }
        }

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

        void Reserve(size_t n) {
            if (is_string) {
                strings.reserve(n);
            } else {
                ints.reserve(n);
            }
        }

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

    struct CompositeKeyStorage {
        struct Batch {
            Batch(const CompositeKeyStorage& storage,
                  const std::vector<const core::Column*>& columns);

            bool HasNull(size_t row) const;
            size_t HashRow(size_t row) const noexcept;
            int64_t ReadInt(size_t key_index, size_t row) const;
            std::string_view ReadString(size_t key_index, size_t row) const;

        private:
            const CompositeKeyStorage& storage_;
            const std::vector<const core::Column*>& columns_;
            std::vector<const core::DictionaryStringColumn*> dictionary_columns_;
            std::vector<std::vector<size_t>> dictionary_hashes_;
        };

        struct ProbeKey {
            const Batch* batch = nullptr;
            size_t row = 0;
            size_t hash = 0;
        };

        explicit CompositeKeyStorage(const std::vector<ProjectionUnit>& keys);

        CompositeKeyStorage(const CompositeKeyStorage&) = delete;
        CompositeKeyStorage& operator=(const CompositeKeyStorage&) = delete;
        CompositeKeyStorage(CompositeKeyStorage&&) = delete;
        CompositeKeyStorage& operator=(CompositeKeyStorage&&) = delete;

        ProbeKey MakeProbe(const Batch& batch, size_t row) const noexcept;
        bool LookupGroup(const ProbeKey& key, uint32_t& group_id) const;
        void InsertGroup(uint32_t group_id, const ProbeKey& key);
        void AppendKey(uint32_t group_id, size_t key_index, core::Column& out) const;

    private:
        enum class PartKind : uint8_t { Int64, String };

        struct GroupHash {
            using is_transparent = void;  // NOLINT(readability-identifier-naming)
            const CompositeKeyStorage* storage = nullptr;
            size_t operator()(uint32_t group_id) const noexcept;
            size_t operator()(const ProbeKey& key) const noexcept;
        };

        struct GroupEq {
            using is_transparent = void;  // NOLINT(readability-identifier-naming)
            const CompositeKeyStorage* storage = nullptr;
            bool operator()(uint32_t lhs, uint32_t rhs) const noexcept;
            bool operator()(uint32_t lhs, const ProbeKey& rhs) const noexcept;
            bool operator()(const ProbeKey& lhs, uint32_t rhs) const noexcept;
            bool operator()(const ProbeKey& lhs, const ProbeKey& rhs) const noexcept;
        };

        using Groups = absl::flat_hash_set<uint32_t, GroupHash, GroupEq>;

        size_t HashGroup(uint32_t group_id) const noexcept;
        bool GroupsEqual(uint32_t lhs, uint32_t rhs) const noexcept;
        bool GroupEqualsProbe(uint32_t group_id, const ProbeKey& key) const noexcept;
        bool ProbesEqual(const ProbeKey& lhs, const ProbeKey& rhs) const noexcept;

        std::vector<PartKind> part_kinds_;
        std::vector<std::vector<int64_t>> int_group_keys_;
        std::vector<std::vector<std::string>> string_group_keys_;
        std::vector<size_t> group_hashes_;
        Groups groups_;
    };

    static KeyMode SelectKeyMode(const std::vector<ProjectionUnit>& keys);

    uint32_t EmplaceGroup();
    void ReserveStringGroupsForBatch(size_t selected_rows, size_t max_new_groups);

    void UpdateAggsForRow(uint32_t group_id, const std::vector<const core::Column*>& agg_cols,
                          size_t row);

    void AppendAggResult(size_t agg_index, uint32_t group_id, core::Column& out) const;

    void ConsumeInt64(const core::Column& key_col, const std::vector<const core::Column*>& agg_cols,
                      const std::vector<uint32_t>* selection, size_t rows);
    void ConsumeString(const core::Column& key_col,
                       const std::vector<const core::Column*>& agg_cols,
                       const std::vector<uint32_t>* selection, size_t rows);
    void ConsumeDictionaryString(const core::DictionaryStringColumn& key_col,
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
    size_t input_rows_seen_ = 0;
    size_t reserved_groups_ = 0;
    std::vector<AggArray> agg_arrays_;
    std::vector<int64_t> int64_group_keys_;
    std::vector<std::string> string_group_keys_;
    std::vector<Int64Pair> int64_pair_group_keys_;
    CompositeKeyStorage composite_keys_;
    Int64Groups int64_groups_;
    StringGroups string_groups_;
    Int64PairGroups int64_pair_groups_;
};
}  // namespace columnar::exec

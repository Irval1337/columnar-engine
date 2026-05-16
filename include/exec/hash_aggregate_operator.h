#pragma once

#include <core/schema.h>
#include <exec/aggregation.h>
#include <exec/operator.h>

#include <absl/container/flat_hash_map.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace columnar::exec {
class HashAggregationSink final : public IOperator {
public:
    HashAggregationSink(IOperator& downstream, std::vector<ProjectionUnit> keys,
                        std::vector<AggregationUnit> aggregations);

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

    struct CompositeKeyHash {
        size_t operator()(const CompositeKey& key) const noexcept;
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

    using States = std::vector<AggregationState>;
    using Int64Groups = absl::flat_hash_map<int64_t, States>;
    using StringGroups = absl::flat_hash_map<std::string, States, StringHash, StringEq>;
    using Int64PairGroups = absl::flat_hash_map<Int64Pair, States, Int64PairHash>;
    using CompositeGroups = absl::flat_hash_map<CompositeKey, States, CompositeKeyHash>;

    static KeyMode SelectKeyMode(const std::vector<ProjectionUnit>& keys);

    void ConsumeInt64(const core::Column& key_col, const std::vector<const core::Column*>& agg_cols,
                      size_t rows);
    void ConsumeString(const core::Column& key_col,
                       const std::vector<const core::Column*>& agg_cols, size_t rows);
    void ConsumeInt64Pair(const core::Column& first_key_col, const core::Column& second_key_col,
                          const std::vector<const core::Column*>& agg_cols, size_t rows);
    void ConsumeComposite(const std::vector<const core::Column*>& key_cols,
                          const std::vector<const core::Column*>& agg_cols, size_t rows);

    void UpdateAggsForRow(States& states, const std::vector<const core::Column*>& agg_cols,
                          size_t row);

    IOperator& downstream_;
    std::vector<ProjectionUnit> keys_;
    std::vector<AggregationUnit> aggregations_;
    core::Schema output_schema_;
    KeyMode mode_;
    Int64Groups int64_groups_;
    StringGroups string_groups_;
    Int64PairGroups int64_pair_groups_;
    CompositeGroups composite_groups_;
};
}  // namespace columnar::exec

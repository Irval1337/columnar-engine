#pragma once

#include <core/schema.h>
#include <exec/aggregation.h>
#include <exec/operator.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace columnar::exec {
class HashAggregationSink final : public IOperator {
public:
    HashAggregationSink(IOperator& downstream, std::shared_ptr<Expression> key,
                        std::string key_name, std::vector<AggregationUnit> aggregations);

    void Consume(core::Batch batch) override;

    void Finalize() override;

private:
    enum class KeyMode { Int64, String };

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
    using Int64Groups = std::unordered_map<int64_t, States>;
    using StringGroups = std::unordered_map<std::string, States, StringHash, StringEq>;

    static KeyMode SelectKeyMode(const Expression& key);

    void ConsumeInt64(const core::Column& key_col,
                      const std::vector<const core::Column*>& agg_cols, size_t rows);
    void ConsumeString(const core::Column& key_col,
                       const std::vector<const core::Column*>& agg_cols, size_t rows);

    void UpdateAggsForRow(States& states, const std::vector<const core::Column*>& agg_cols,
                          size_t row);

    IOperator& downstream_;
    std::shared_ptr<Expression> key_;
    std::string key_name_;
    std::vector<AggregationUnit> aggregations_;
    core::Schema output_schema_;
    KeyMode mode_;
    Int64Groups int64_groups_;
    StringGroups string_groups_;
};
}  // namespace columnar::exec

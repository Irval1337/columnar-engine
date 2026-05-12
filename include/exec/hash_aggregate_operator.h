#pragma once

#include <core/schema.h>
#include <exec/aggregation.h>
#include <exec/operator.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
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
    using GroupKey = std::vector<std::variant<int64_t, std::string>>;

    struct GroupKeyHash {
        size_t operator()(const GroupKey& key) const noexcept;
    };

    IOperator& downstream_;
    std::vector<ProjectionUnit> keys_;
    std::vector<AggregationUnit> aggregations_;
    core::Schema output_schema_;
    std::unordered_map<GroupKey, std::vector<AggregationState>, GroupKeyHash> groups_;
};
}  // namespace columnar::exec

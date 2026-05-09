#pragma once

#include <core/schema.h>
#include <exec/aggregation.h>
#include <exec/operator.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace columnar::exec {
class HashAggregationSink final : public IOperator {
public:
    HashAggregationSink(IOperator& downstream, std::shared_ptr<Expression> key,
                        std::string key_name, std::vector<AggregationUnit> aggregations);

    void Consume(core::Batch batch) override;

    void Finalize() override;

private:
    using GroupKey = std::variant<int64_t, std::string>;

    struct GroupKeyHash {
        size_t operator()(const GroupKey& k) const noexcept;
    };

    IOperator& downstream_;
    std::shared_ptr<Expression> key_;
    std::string key_name_;
    std::vector<AggregationUnit> aggregations_;
    core::Schema output_schema_;
    std::unordered_map<GroupKey, std::vector<AggregationState>, GroupKeyHash> groups_;
};
}  // namespace columnar::exec

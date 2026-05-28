#pragma once

#include <core/schema.h>
#include <exec/agg_state_buffer.h>
#include <exec/group_key_table.h>
#include <exec/operator.h>
#include <util/string_arena.h>

#include <cstddef>
#include <memory>
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
    void ReserveForBatch(size_t selected_rows, size_t max_new_groups);

    IOperator& downstream_;
    std::vector<ProjectionUnit> keys_;
    std::vector<AggregationUnit> aggregations_;
    core::Schema output_schema_;
    bool needs_dense_;
    size_t input_rows_seen_ = 0;
    size_t reserved_groups_ = 0;
    util::StringArena string_arena_;
    AggStateBuffer state_;
    std::unique_ptr<GroupKeyTable> key_table_;
};
}  // namespace columnar::exec

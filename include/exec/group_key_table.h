#pragma once

#include <core/batch.h>
#include <core/column.h>
#include <exec/agg_state_buffer.h>
#include <exec/operator.h>
#include <util/macro.h>
#include <util/string_arena.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace columnar::exec {
class GroupKeyTable {
public:
    static std::unique_ptr<GroupKeyTable> Make(const std::vector<ProjectionUnit>& keys,
                                               util::StringArena& arena);

    virtual ~GroupKeyTable() = default;

    virtual void Consume(const std::vector<const core::Column*>& key_cols,
                         const std::vector<const core::Column*>& agg_cols,
                         const std::vector<uint32_t>* selection, size_t rows,
                         AggStateBuffer& state) = 0;

    virtual void AppendKeys(uint32_t group_id, core::Batch& out) const = 0;

    virtual void ReserveBuckets(size_t n) = 0;

    virtual std::optional<size_t> MaxNewGroupsForBatch(
        const std::vector<const core::Column*>& key_cols, size_t selected_rows) const {
        UNUSED(key_cols);
        UNUSED(selected_rows);
        return std::nullopt;
    }
};
}  // namespace columnar::exec

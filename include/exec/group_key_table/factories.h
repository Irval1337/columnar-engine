#pragma once

#include <exec/group_key_table.h>
#include <util/string_arena.h>

#include <memory>
#include <vector>

namespace columnar::exec {
std::unique_ptr<GroupKeyTable> MakeInt64KeyTable();
std::unique_ptr<GroupKeyTable> MakeStringKeyTable(util::StringArena& arena);
std::unique_ptr<GroupKeyTable> MakeInt64PairKeyTable();
std::unique_ptr<GroupKeyTable> MakeInt64StringKeyTable(util::StringArena& arena);
std::unique_ptr<GroupKeyTable> MakeInt64Int64StringKeyTable(util::StringArena& arena);
std::unique_ptr<GroupKeyTable> MakeCompositeKeyTable(const std::vector<ProjectionUnit>& keys,
                                                     util::StringArena& arena);
}  // namespace columnar::exec

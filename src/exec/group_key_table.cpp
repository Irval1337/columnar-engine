#include <exec/group_key_table.h>

#include <core/datatype.h>
#include <exec/column_dispatch.h>
#include <exec/expression/eval.h>
#include <exec/expression/types.h>
#include <exec/group_key_table/factories.h>

#include <memory>
#include <vector>

namespace columnar::exec {
std::unique_ptr<GroupKeyTable> GroupKeyTable::Make(const std::vector<ProjectionUnit>& keys,
                                                   util::StringArena& arena) {
    if (keys.size() == 3 && HasIntegerValue(GetExpressionType(*keys[0].expression)) &&
        HasIntegerValue(GetExpressionType(*keys[1].expression)) &&
        GetExpressionType(*keys[2].expression) == core::DataType::String) {
        return MakeInt64Int64StringKeyTable(arena);
    }
    if (keys.size() == 2 && HasIntegerValue(GetExpressionType(*keys[0].expression)) &&
        GetExpressionType(*keys[1].expression) == core::DataType::String) {
        return MakeInt64StringKeyTable(arena);
    }
    if (keys.size() == 2 && HasIntegerValue(GetExpressionType(*keys[0].expression)) &&
        HasIntegerValue(GetExpressionType(*keys[1].expression))) {
        return MakeInt64PairKeyTable();
    }
    if (keys.size() == 1) {
        auto type = GetExpressionType(*keys[0].expression);
        if (type == core::DataType::String) {
            return MakeStringKeyTable(arena);
        }
        if (HasIntegerValue(type)) {
            return MakeInt64KeyTable();
        }
    }
    return MakeCompositeKeyTable(keys, arena);
}
}  // namespace columnar::exec

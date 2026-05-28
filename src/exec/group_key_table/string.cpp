#include <core/batch.h>
#include <core/columns/dictionary_string_column.h>
#include <core/columns/string_column.h>
#include <exec/group_key_table.h>
#include <exec/group_key_table/factories.h>
#include <exec/selection.h>
#include <util/string_arena.h>

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace columnar::exec {
namespace {
class StringKeyTable final : public GroupKeyTable {
public:
    explicit StringKeyTable(util::StringArena& arena) : arena_(arena) {
    }

    void Consume(const std::vector<const core::Column*>& key_cols,
                 const std::vector<const core::Column*>& agg_cols,
                 const std::vector<uint32_t>* selection, size_t rows,
                 AggStateBuffer& state) override {
        if (auto* dict = core::AsDictionaryString(key_cols[0])) {
            ConsumeDictionary(*dict, agg_cols, selection, rows, state);
            return;
        }
        auto& s = static_cast<const core::StringColumn&>(*key_cols[0]);
        ForSelectedRows(selection, rows, [&](size_t row) {
            if (s.IsNull(row)) {
                return;
            }
            auto key = s.Get(row);
            uint32_t group_id;
            auto it = table_.find(key);
            if (it == table_.end()) {
                auto interned = arena_.Intern(key);
                group_id = state.EmplaceGroup();
                table_.emplace(interned, group_id);
                keys_.push_back(interned);
            } else {
                group_id = it->second;
            }
            state.OnRow(group_id, agg_cols, row);
        });
    }

    void AppendKeys(uint32_t group_id, core::Batch& out) const override {
        static_cast<core::StringColumn&>(out.ColumnAt(0)).Append(keys_[group_id]);
    }

    void ReserveBuckets(size_t n) override {
        table_.reserve(n);
        keys_.reserve(n);
    }

    std::optional<size_t> MaxNewGroupsForBatch(const std::vector<const core::Column*>& key_cols,
                                               size_t selected_rows) const override {
        if (auto* dict = core::AsDictionaryString(key_cols[0])) {
            return std::min(selected_rows, dict->DictSize());
        }
        return std::nullopt;
    }

private:
    void ConsumeDictionary(const core::DictionaryStringColumn& key_col,
                           const std::vector<const core::Column*>& agg_cols,
                           const std::vector<uint32_t>* selection, size_t rows,
                           AggStateBuffer& state) {
        constexpr uint32_t kUnknownGroup = std::numeric_limits<uint32_t>::max();
        std::vector<uint32_t> id_to_group(key_col.DictSize(), kUnknownGroup);

        auto resolve_group = [&](uint32_t local_id) {
            uint32_t group_id = id_to_group[local_id];
            if (group_id != kUnknownGroup) {
                return group_id;
            }
            auto key = key_col.DictValue(local_id);
            auto it = table_.find(key);
            if (it == table_.end()) {
                auto interned = arena_.Intern(key);
                group_id = state.EmplaceGroup();
                table_.emplace(interned, group_id);
                keys_.push_back(interned);
            } else {
                group_id = it->second;
            }
            id_to_group[local_id] = group_id;
            return group_id;
        };

        ForSelectedRows(selection, rows, [&](size_t row) {
            if (key_col.IsNull(row)) {
                return;
            }
            state.OnRow(resolve_group(key_col.GetId(row)), agg_cols, row);
        });
    }

    util::StringArena& arena_;
    absl::flat_hash_map<std::string_view, uint32_t> table_;
    std::vector<std::string_view> keys_;
};
}  // namespace

std::unique_ptr<GroupKeyTable> MakeStringKeyTable(util::StringArena& arena) {
    return std::make_unique<StringKeyTable>(arena);
}
}  // namespace columnar::exec

#include <core/batch.h>
#include <core/columns/dictionary_string_column.h>
#include <core/columns/string_column.h>
#include <exec/column_dispatch.h>
#include <exec/column_row_access.h>
#include <exec/group_key_table.h>
#include <exec/group_key_table/factories.h>
#include <exec/group_key_table/hash.h>
#include <exec/selection.h>
#include <util/string_arena.h>

#include <absl/container/flat_hash_set.h>

#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>
#include <vector>

namespace columnar::exec {
namespace {
class Int64StringKeyTable final : public GroupKeyTable {
public:
    explicit Int64StringKeyTable(util::StringArena& arena)
        : arena_(arena), groups_(0, GroupHash{this}, GroupEq{this}) {
    }

    void Consume(const std::vector<const core::Column*>& key_cols,
                 const std::vector<const core::Column*>& agg_cols,
                 const std::vector<uint32_t>* selection, size_t rows,
                 AggStateBuffer& state) override {
        VisitIntegerCol(*key_cols[0], [&](const auto& first_typed) {
            const util::BitVector* first_mask =
                first_typed.IsNullable() ? &first_typed.GetNullMask() : nullptr;
            const auto* dict = core::AsDictionaryString(key_cols[1]);
            const core::StringColumn* strings =
                dict == nullptr ? &static_cast<const core::StringColumn&>(*key_cols[1]) : nullptr;
            std::vector<size_t> dict_hashes;
            if (dict != nullptr) {
                dict_hashes.resize(dict->DictSize());
                for (uint32_t id = 0; id < dict_hashes.size(); ++id) {
                    dict_hashes[id] = std::hash<std::string_view>{}(dict->DictValue(id));
                }
            }

            ForSelectedRows(selection, rows, [&](size_t row) {
                if ((first_mask != nullptr && first_mask->Get(row)) || key_cols[1]->IsNull(row)) {
                    return;
                }
                int64_t first = static_cast<int64_t>(ReadTypedValue(first_typed, row));
                std::string_view second;
                size_t second_hash;
                if (dict != nullptr) {
                    uint32_t id = dict->GetId(row);
                    second = dict->DictValue(id);
                    second_hash = dict_hashes[id];
                } else {
                    second = strings->Get(row);
                    second_hash = std::hash<std::string_view>{}(second);
                }

                ProbeKey probe{first, second, group_key::HashIntString(first, second_hash)};
                uint32_t group_id;
                if (!LookupGroup(probe, group_id)) {
                    group_id = state.EmplaceGroup();
                    InsertGroup(group_id, probe);
                }
                state.OnRow(group_id, agg_cols, row);
            });
        });
    }

    void AppendKeys(uint32_t group_id, core::Batch& out) const override {
        const auto& key = keys_[group_id];
        AppendInteger(out.ColumnAt(0), key.first);
        out.ColumnAt(1).AppendFromString(key.second);
    }

    void ReserveBuckets(size_t n) override {
        groups_.reserve(n);
        hashes_.reserve(n);
        keys_.reserve(n);
    }

private:
    struct Key {
        int64_t first = 0;
        std::string_view second;

        bool operator==(const Key&) const = default;
    };

    struct ProbeKey {
        int64_t first = 0;
        std::string_view second;
        size_t hash = 0;
    };

    struct GroupHash {
        using is_transparent = void;  // NOLINT(readability-identifier-naming)

        const Int64StringKeyTable* table = nullptr;

        size_t operator()(uint32_t group_id) const noexcept {
            return table->hashes_[group_id];
        }

        size_t operator()(const ProbeKey& key) const noexcept {
            return key.hash;
        }
    };

    struct GroupEq {
        using is_transparent = void;  // NOLINT(readability-identifier-naming)

        const Int64StringKeyTable* table = nullptr;

        bool operator()(uint32_t lhs, uint32_t rhs) const noexcept {
            return table->keys_[lhs] == table->keys_[rhs];
        }

        bool operator()(uint32_t lhs, const ProbeKey& rhs) const noexcept {
            const auto& key = table->keys_[lhs];
            return table->hashes_[lhs] == rhs.hash && key.first == rhs.first &&
                   key.second == rhs.second;
        }

        bool operator()(const ProbeKey& lhs, uint32_t rhs) const noexcept {
            return (*this)(rhs, lhs);
        }

        bool operator()(const ProbeKey& lhs, const ProbeKey& rhs) const noexcept {
            return lhs.hash == rhs.hash && lhs.first == rhs.first && lhs.second == rhs.second;
        }
    };

    using Groups = absl::flat_hash_set<uint32_t, GroupHash, GroupEq>;

    bool LookupGroup(const ProbeKey& key, uint32_t& group_id) const {
        auto it = groups_.find(key);
        if (it == groups_.end()) {
            return false;
        }
        group_id = *it;
        return true;
    }

    void InsertGroup(uint32_t group_id, const ProbeKey& key) {
        assert(group_id == hashes_.size());
        auto interned = arena_.Intern(key.second);
        hashes_.push_back(key.hash);
        keys_.push_back(Key{key.first, interned});
        groups_.insert(group_id);
    }

    util::StringArena& arena_;
    std::vector<size_t> hashes_;
    std::vector<Key> keys_;
    Groups groups_;
};
}  // namespace

std::unique_ptr<GroupKeyTable> MakeInt64StringKeyTable(util::StringArena& arena) {
    return std::make_unique<Int64StringKeyTable>(arena);
}
}  // namespace columnar::exec

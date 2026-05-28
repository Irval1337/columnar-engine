#include <core/batch.h>
#include <exec/column_dispatch.h>
#include <exec/column_row_access.h>
#include <exec/group_key_table.h>
#include <exec/group_key_table/factories.h>
#include <exec/group_key_table/hash.h>
#include <exec/selection.h>

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace columnar::exec {
namespace {
class Int64PairKeyTable final : public GroupKeyTable {
public:
    void Consume(const std::vector<const core::Column*>& key_cols,
                 const std::vector<const core::Column*>& agg_cols,
                 const std::vector<uint32_t>* selection, size_t rows,
                 AggStateBuffer& state) override {
        VisitIntegerCol(*key_cols[0], [&](const auto& first_typed) {
            VisitIntegerCol(*key_cols[1], [&](const auto& second_typed) {
                const util::BitVector* first_mask =
                    first_typed.IsNullable() ? &first_typed.GetNullMask() : nullptr;
                const util::BitVector* second_mask =
                    second_typed.IsNullable() ? &second_typed.GetNullMask() : nullptr;
                ForSelectedRows(selection, rows, [&](size_t row) {
                    if ((first_mask != nullptr && first_mask->Get(row)) ||
                        (second_mask != nullptr && second_mask->Get(row))) {
                        return;
                    }
                    Key key{static_cast<int64_t>(ReadTypedValue(first_typed, row)),
                            static_cast<int64_t>(ReadTypedValue(second_typed, row))};
                    auto it = table_.find(key);
                    uint32_t group_id;
                    if (it == table_.end()) {
                        group_id = state.EmplaceGroup();
                        table_.emplace(key, group_id);
                        keys_.push_back(key);
                    } else {
                        group_id = it->second;
                    }
                    state.OnRow(group_id, agg_cols, row);
                });
            });
        });
    }

    void AppendKeys(uint32_t group_id, core::Batch& out) const override {
        const auto& key = keys_[group_id];
        AppendInteger(out.ColumnAt(0), key.first);
        AppendInteger(out.ColumnAt(1), key.second);
    }

    void ReserveBuckets(size_t n) override {
        table_.reserve(n);
        keys_.reserve(n);
    }

private:
    struct Key {
        int64_t first = 0;
        int64_t second = 0;

        bool operator==(const Key&) const = default;
    };

    struct Hash {
        size_t operator()(const Key& key) const noexcept {
            size_t seed = 2;
            group_key::HashCombine(seed, std::hash<int64_t>{}(key.first));
            group_key::HashCombine(seed, std::hash<int64_t>{}(key.second));
            return seed;
        }
    };

    absl::flat_hash_map<Key, uint32_t, Hash> table_;
    std::vector<Key> keys_;
};
}  // namespace

std::unique_ptr<GroupKeyTable> MakeInt64PairKeyTable() {
    return std::make_unique<Int64PairKeyTable>();
}
}  // namespace columnar::exec

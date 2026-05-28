#include <core/batch.h>
#include <exec/column_dispatch.h>
#include <exec/column_row_access.h>
#include <exec/group_key_table.h>
#include <exec/group_key_table/factories.h>
#include <exec/selection.h>

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace columnar::exec {
namespace {
class Int64KeyTable final : public GroupKeyTable {
public:
    void Consume(const std::vector<const core::Column*>& key_cols,
                 const std::vector<const core::Column*>& agg_cols,
                 const std::vector<uint32_t>* selection, size_t rows,
                 AggStateBuffer& state) override {
        VisitIntegerCol(*key_cols[0], [&](const auto& typed) {
            const util::BitVector* mask = typed.IsNullable() ? &typed.GetNullMask() : nullptr;
            ForSelectedRows(selection, rows, [&](size_t row) {
                if (mask != nullptr && mask->Get(row)) {
                    return;
                }
                int64_t key = static_cast<int64_t>(ReadTypedValue(typed, row));
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
    }

    void AppendKeys(uint32_t group_id, core::Batch& out) const override {
        AppendInteger(out.ColumnAt(0), keys_[group_id]);
    }

    void ReserveBuckets(size_t n) override {
        table_.reserve(n);
        keys_.reserve(n);
    }

private:
    absl::flat_hash_map<int64_t, uint32_t> table_;
    std::vector<int64_t> keys_;
};
}  // namespace

std::unique_ptr<GroupKeyTable> MakeInt64KeyTable() {
    return std::make_unique<Int64KeyTable>();
}
}  // namespace columnar::exec

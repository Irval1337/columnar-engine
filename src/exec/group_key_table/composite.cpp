#include <core/batch.h>
#include <core/columns/dictionary_string_column.h>
#include <core/columns/string_column.h>
#include <exec/column_row_access.h>
#include <exec/expression/eval.h>
#include <exec/expression/types.h>
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
class CompositeKeyTable final : public GroupKeyTable {
public:
    CompositeKeyTable(const std::vector<ProjectionUnit>& keys, util::StringArena& arena)
        : arena_(arena), groups_(0, GroupHash{this}, GroupEq{this}) {
        part_kinds_.reserve(keys.size());
        int_keys_.resize(keys.size());
        string_keys_.resize(keys.size());
        for (auto& key : keys) {
            auto type = GetExpressionType(*key.expression);
            part_kinds_.push_back(type == core::DataType::String ? PartKind::String
                                                                 : PartKind::Int64);
        }
    }

    void Consume(const std::vector<const core::Column*>& key_cols,
                 const std::vector<const core::Column*>& agg_cols,
                 const std::vector<uint32_t>* selection, size_t rows,
                 AggStateBuffer& state) override {
        BatchView batch_view(*this, key_cols);
        ForSelectedRows(selection, rows, [&](size_t row) {
            if (batch_view.HasNull(row)) {
                return;
            }
            ProbeKey probe{&batch_view, row, batch_view.HashRow(row)};
            uint32_t group_id;
            if (!LookupGroup(probe, group_id)) {
                group_id = state.EmplaceGroup();
                InsertGroup(group_id, probe);
            }
            state.OnRow(group_id, agg_cols, row);
        });
    }

    void AppendKeys(uint32_t group_id, core::Batch& out) const override {
        for (size_t i = 0; i < part_kinds_.size(); ++i) {
            auto& column = out.ColumnAt(i);
            if (part_kinds_[i] == PartKind::String) {
                column.AppendFromString(string_keys_[i][group_id]);
            } else {
                AppendInteger(column, int_keys_[i][group_id]);
            }
        }
    }

    void ReserveBuckets(size_t n) override {
        groups_.reserve(n);
        group_hashes_.reserve(n);
        for (auto& keys : int_keys_) {
            keys.reserve(n);
        }
        for (auto& keys : string_keys_) {
            keys.reserve(n);
        }
    }

private:
    enum class PartKind : uint8_t { Int64, String };

    class BatchView {
    public:
        BatchView(const CompositeKeyTable& storage, const std::vector<const core::Column*>& columns)
            : storage_(storage),
              columns_(columns),
              dictionary_columns_(columns.size(), nullptr),
              dictionary_hashes_(columns.size()) {
            for (size_t i = 0; i < columns_.size(); ++i) {
                if (storage_.part_kinds_[i] != PartKind::String) {
                    continue;
                }
                dictionary_columns_[i] = core::AsDictionaryString(columns_[i]);
                if (dictionary_columns_[i] == nullptr) {
                    continue;
                }
                auto& hashes = dictionary_hashes_[i];
                hashes.resize(dictionary_columns_[i]->DictSize());
                for (uint32_t id = 0; id < hashes.size(); ++id) {
                    hashes[id] =
                        std::hash<std::string_view>{}(dictionary_columns_[i]->DictValue(id));
                }
            }
        }

        bool HasNull(size_t row) const {
            for (auto* col : columns_) {
                if (col->IsNull(row)) {
                    return true;
                }
            }
            return false;
        }

        size_t HashRow(size_t row) const noexcept {
            size_t seed = storage_.part_kinds_.size();
            for (size_t i = 0; i < storage_.part_kinds_.size(); ++i) {
                if (storage_.part_kinds_[i] == PartKind::String) {
                    size_t part_hash;
                    if (dictionary_columns_[i] != nullptr) {
                        part_hash = dictionary_hashes_[i][dictionary_columns_[i]->GetId(row)];
                    } else {
                        part_hash = std::hash<std::string_view>{}(ReadString(i, row));
                    }
                    group_key::HashCombine(seed, part_hash);
                } else {
                    group_key::HashCombine(seed, std::hash<int64_t>{}(ReadInt(i, row)));
                }
            }
            return seed;
        }

        int64_t ReadInt(size_t key_index, size_t row) const {
            return ReadIntegerRow(*columns_[key_index], row);
        }

        std::string_view ReadString(size_t key_index, size_t row) const {
            if (dictionary_columns_[key_index] != nullptr) {
                return dictionary_columns_[key_index]->Get(row);
            }
            return static_cast<const core::StringColumn&>(*columns_[key_index]).Get(row);
        }

    private:
        const CompositeKeyTable& storage_;
        const std::vector<const core::Column*>& columns_;
        std::vector<const core::DictionaryStringColumn*> dictionary_columns_;
        std::vector<std::vector<size_t>> dictionary_hashes_;
    };

    struct ProbeKey {
        const BatchView* batch = nullptr;
        size_t row = 0;
        size_t hash = 0;
    };

    struct GroupHash {
        using is_transparent = void;  // NOLINT(readability-identifier-naming)

        const CompositeKeyTable* storage = nullptr;

        size_t operator()(uint32_t group_id) const noexcept {
            return storage->group_hashes_[group_id];
        }

        size_t operator()(const ProbeKey& key) const noexcept {
            return key.hash;
        }
    };

    struct GroupEq {
        using is_transparent = void;  // NOLINT(readability-identifier-naming)

        const CompositeKeyTable* storage = nullptr;

        bool operator()(uint32_t lhs, uint32_t rhs) const noexcept {
            return storage->GroupsEqual(lhs, rhs);
        }

        bool operator()(uint32_t lhs, const ProbeKey& rhs) const noexcept {
            return storage->GroupEqualsProbe(lhs, rhs);
        }

        bool operator()(const ProbeKey& lhs, uint32_t rhs) const noexcept {
            return storage->GroupEqualsProbe(rhs, lhs);
        }

        bool operator()(const ProbeKey& lhs, const ProbeKey& rhs) const noexcept {
            return storage->ProbesEqual(lhs, rhs);
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
        assert(group_id == group_hashes_.size());
        group_hashes_.push_back(key.hash);
        for (size_t i = 0; i < part_kinds_.size(); ++i) {
            if (part_kinds_[i] == PartKind::String) {
                auto value = key.batch->ReadString(i, key.row);
                string_keys_[i].push_back(arena_.Intern(value));
            } else {
                int_keys_[i].push_back(key.batch->ReadInt(i, key.row));
            }
        }
        groups_.insert(group_id);
    }

    bool GroupsEqual(uint32_t lhs, uint32_t rhs) const noexcept {
        if (group_hashes_[lhs] != group_hashes_[rhs]) {
            return false;
        }
        for (size_t i = 0; i < part_kinds_.size(); ++i) {
            if (part_kinds_[i] == PartKind::String) {
                if (string_keys_[i][lhs] != string_keys_[i][rhs]) {
                    return false;
                }
            } else if (int_keys_[i][lhs] != int_keys_[i][rhs]) {
                return false;
            }
        }
        return true;
    }

    bool GroupEqualsProbe(uint32_t group_id, const ProbeKey& key) const noexcept {
        if (group_hashes_[group_id] != key.hash) {
            return false;
        }
        for (size_t i = 0; i < part_kinds_.size(); ++i) {
            if (part_kinds_[i] == PartKind::String) {
                if (string_keys_[i][group_id] != key.batch->ReadString(i, key.row)) {
                    return false;
                }
            } else if (int_keys_[i][group_id] != key.batch->ReadInt(i, key.row)) {
                return false;
            }
        }
        return true;
    }

    bool ProbesEqual(const ProbeKey& lhs, const ProbeKey& rhs) const noexcept {
        if (lhs.hash != rhs.hash) {
            return false;
        }
        for (size_t i = 0; i < part_kinds_.size(); ++i) {
            if (part_kinds_[i] == PartKind::String) {
                if (lhs.batch->ReadString(i, lhs.row) != rhs.batch->ReadString(i, rhs.row)) {
                    return false;
                }
            } else if (lhs.batch->ReadInt(i, lhs.row) != rhs.batch->ReadInt(i, rhs.row)) {
                return false;
            }
        }
        return true;
    }

    util::StringArena& arena_;
    std::vector<PartKind> part_kinds_;
    std::vector<std::vector<int64_t>> int_keys_;
    std::vector<std::vector<std::string_view>> string_keys_;
    std::vector<size_t> group_hashes_;
    Groups groups_;
};
}  // namespace

std::unique_ptr<GroupKeyTable> MakeCompositeKeyTable(const std::vector<ProjectionUnit>& keys,
                                                     util::StringArena& arena) {
    return std::make_unique<CompositeKeyTable>(keys, arena);
}
}  // namespace columnar::exec

#pragma once

#include <core/column.h>
#include <core/datatype.h>
#include <util/bit_vector.h>
#include <util/macro.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace columnar::core {
class DictionaryStringColumn final : public Column {
public:
    DictionaryStringColumn(bool nullable = false) : dict_offsets_(1, 0), nullable_(nullable) {
    }

    DictionaryStringColumn(std::vector<char>&& dict_data,
                           std::vector<size_t>&& dict_offsets,
                           std::vector<uint32_t>&& ids,
                           util::BitVector&& is_null,
                           bool nullable)
        : dict_data_(std::move(dict_data)),
          dict_offsets_(std::move(dict_offsets)),
          ids_(std::move(ids)),
          nullable_(nullable),
          is_null_(std::move(is_null)) {
        Validate();
    }

    DataType GetDataType() const override {
        return DataType::String;
    }

    size_t Size() const override {
        return ids_.size();
    }

    void Reserve(size_t n) override {
        ids_.reserve(n);
        if (nullable_) {
            is_null_.Reserve(n);
        }
    }

    bool IsNullable() const override {
        return nullable_;
    }

    bool IsNull(size_t i) const override {
        return nullable_ && is_null_.Get(i);
    }

    void Append(std::string_view s) {
        uint32_t id = AppendDictValue(s);
        ids_.push_back(id);
        if (nullable_) {
            is_null_.PushBack(false);
        }
    }

    void AppendFromString(std::string_view s) override {
        Append(s);
    }

    void AppendNull() override {
        if (!nullable_) {
            THROW_RUNTIME_ERROR("Cannot set not nullable value to null");
        }
        ids_.push_back(EnsureEmptyValue());
        is_null_.PushBack(true);
    }

    void AppendDefault() override {
        ids_.push_back(EnsureEmptyValue());
        if (nullable_) {
            is_null_.PushBack(true);
        }
    }

    void Clear() override {
        dict_data_.clear();
        dict_offsets_.clear();
        dict_offsets_.push_back(0);
        ids_.clear();
        is_null_.Clear();
    }

    std::string_view Get(size_t i) const {
        return DictValue(ids_[i]);
    }

    uint32_t GetId(size_t i) const {
        return ids_[i];
    }

    std::string_view DictValue(uint32_t id) const {
        size_t start = dict_offsets_[id];
        size_t len = dict_offsets_[id + 1] - start;
        if (len == 0) {
            return {};
        }
        return std::string_view(dict_data_.data() + start, len);
    }

    size_t DictSize() const {
        return dict_offsets_.size() - 1;
    }

    std::string GetAsString(size_t i) const override {
        if (IsNull(i)) {
            return "";
        }
        auto value = Get(i);
        return std::string(value.data(), value.size());
    }

    void AppendToString(size_t i, std::string& out) const override {
        if (IsNull(i)) {
            return;
        }
        auto value = Get(i);
        out.append(value.data(), value.size());
    }

    const std::vector<char>& GetDictData() const {
        return dict_data_;
    }

    const std::vector<size_t>& GetDictOffsets() const {
        return dict_offsets_;
    }

    const std::vector<uint32_t>& GetIds() const {
        return ids_;
    }

    const util::BitVector& GetNullMask() const {
        return is_null_;
    }

private:
    void Validate() const {
        if (dict_offsets_.empty()) {
            THROW_RUNTIME_ERROR("Corrupted dictionary string column");
        }
        if (dict_offsets_.front() != 0 || dict_offsets_.back() != dict_data_.size()) {
            THROW_RUNTIME_ERROR("Corrupted dictionary string column");
        }
        for (size_t i = 1; i < dict_offsets_.size(); ++i) {
            if (dict_offsets_[i] < dict_offsets_[i - 1]) {
                THROW_RUNTIME_ERROR("Corrupted dictionary string column");
            }
        }
        size_t dict_size = DictSize();
        for (uint32_t id : ids_) {
            if (static_cast<size_t>(id) >= dict_size) {
                THROW_RUNTIME_ERROR("Dictionary string id out of range");
            }
        }
        if (nullable_ && is_null_.Size() != ids_.size()) {
            THROW_RUNTIME_ERROR("Dictionary string null mask size mismatch");
        }
    }

    uint32_t AppendDictValue(std::string_view s) {
        size_t pos = dict_data_.size();
        dict_data_.resize(pos + s.size());
        if (!s.empty()) {
            std::memcpy(dict_data_.data() + pos, s.data(), s.size());
        }
        dict_offsets_.push_back(dict_data_.size());
        return static_cast<uint32_t>(dict_offsets_.size() - 2);
    }

    uint32_t EnsureEmptyValue() {
        if (DictSize() > 0 && dict_offsets_[0] == 0 && dict_offsets_[1] == 0) {
            return 0;
        }
        return AppendDictValue("");
    }

    std::vector<char> dict_data_;
    std::vector<size_t> dict_offsets_;
    std::vector<uint32_t> ids_;
    bool nullable_ = false;
    util::BitVector is_null_;
};
}  // namespace columnar::core

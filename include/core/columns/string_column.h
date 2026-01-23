#pragma once

#include <core/columns/abstract_column.h>
#include <core/datatype.h>
#include <util/macro.h>
#include <util/bit_vector.h>

#include <cstddef>
#include <string>
#include <vector>

namespace columnar::core {
// Serializing schema:
// [is_null][offsets][lengths][data]
class StringColumn final : public Column {
public:
    StringColumn(bool nullable = false) : nullable_(nullable) {
    }

    StringColumn(std::vector<char>&& data, std::vector<std::size_t>&& offsets,
                 std::vector<std::size_t>&& lengths, util::BitVector&& is_null, bool nullable)
        : data_(std::move(data)),
          offsets_(std::move(offsets)),
          lengths_(std::move(lengths)),
          nullable_(nullable),
          is_null_(std::move(is_null)) {
    }

    DataType GetDataType() const override {
        return DataType::String;
    }

    std::size_t Size() const override {
        return lengths_.size();
    }

    void Reserve(std::size_t n) override {
        lengths_.reserve(n);
        offsets_.reserve(n);
        data_.reserve(n * 6);  // Looks like an average string length :)
        if (nullable_) {
            is_null_.Reserve(n);
        }
    }

    bool IsNullable() const override {
        return nullable_;
    }

    bool IsNull(std::size_t i) const override {
        return nullable_ && is_null_.Get(i);
    }

    void AppendFromString(std::string_view s) override {
        AppendString(s);
        offsets_.push_back(data_.size() - s.size());
        lengths_.push_back(s.size());
        if (nullable_) {
            is_null_.PushBack(false);
        }
    }

    void AppendNull() override {
        if (!nullable_) {
            THROW_RUNTIME_ERROR("Cannot set not nullable value to null");
        }
        offsets_.push_back(data_.size());
        lengths_.push_back(0);
        is_null_.PushBack(true);
    }

    void AppendDefault() override {
        offsets_.push_back(data_.size());
        lengths_.push_back(0);
        if (nullable_) {
            is_null_.PushBack(true);
        }
    }

    std::unique_ptr<Column> CloneEmpty() const override {
        return std::make_unique<StringColumn>(nullable_);
    }

    void Clear() override {
        data_.clear();
        offsets_.clear();
        lengths_.clear();
        is_null_.Clear();
    }

    std::string_view Get(size_t i) const {
        return std::string_view(data_.data() + offsets_[i], lengths_[i]);
    }

    std::string GetAsString(std::size_t i) const override {
        if (IsNull(i)) {
            return "";
        }
        return std::string(data_.data() + offsets_[i], lengths_[i]);
    }

    const std::vector<char>& GetData() const {
        return data_;
    }

    const std::vector<std::size_t>& GetOffsets() const {
        return offsets_;
    }

    const std::vector<std::size_t>& GetLengths() const {
        return lengths_;
    }

    const util::BitVector& GetNullMask() const {
        return is_null_;
    }

private:
    void AppendString(std::string_view s) {
        data_.insert(data_.end(), s.begin(), s.end());
    }

    std::vector<char> data_;
    std::vector<std::size_t> offsets_;
    std::vector<std::size_t> lengths_;
    bool nullable_ = false;
    util::BitVector is_null_;
};
}  // namespace columnar::core

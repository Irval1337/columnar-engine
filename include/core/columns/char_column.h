#pragma once

#include <core/column.h>
#include <core/datatype.h>
#include <util/bit_vector.h>
#include <util/macro.h>

#include <string>
#include <string_view>
#include <vector>

namespace columnar::core {
class CharColumn final : public Column {
public:
    CharColumn(bool nullable = false) : nullable_(nullable) {
    }

    CharColumn(std::vector<char>&& data, util::BitVector&& is_null, bool nullable)
        : data_(std::move(data)), nullable_(nullable), is_null_(std::move(is_null)) {
    }

    DataType GetDataType() const override {
        return DataType::Char;
    }

    ColumnKind GetKind() const override {
        return ColumnKind::Char;
    }

    size_t Size() const override {
        return data_.size();
    }

    void Reserve(size_t n) override {
        data_.reserve(n);
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

    void Append(char value) {
        data_.push_back(value);
        if (nullable_) {
            is_null_.PushBack(false);
        }
    }

    void AppendFromString(std::string_view s) override {
        if (s.size() != 1) {
            THROW_RUNTIME_ERROR("Char value must have length 1");
        }
        Append(s[0]);
    }

    void AppendNull() override {
        if (!nullable_) {
            THROW_RUNTIME_ERROR("Cannot set not nullable value to null");
        }
        data_.push_back(0);
        is_null_.PushBack(true);
    }

    void AppendDefault() override {
        data_.push_back(0);
        if (nullable_) {
            is_null_.PushBack(true);
        }
    }

    void Clear() override {
        data_.clear();
        is_null_.Clear();
    }

    char Get(size_t i) const {
        return data_[i];
    }

    std::string GetAsString(size_t i) const override {
        if (IsNull(i)) {
            return "";
        }
        return std::string(1, data_[i]);
    }

    void AppendToString(size_t i, std::string& out) const override {
        if (IsNull(i)) {
            return;
        }
        out += data_[i];
    }

    const std::vector<char>& GetData() const {
        return data_;
    }

    const util::BitVector& GetNullMask() const {
        return is_null_;
    }

private:
    std::vector<char> data_;
    bool nullable_ = false;
    util::BitVector is_null_;
};
}  // namespace columnar::core

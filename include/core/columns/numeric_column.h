#pragma once

#include <core/columns/abstract_column.h>
#include <core/datatype.h>
#include <util/macro.h>
#include <util/parse.h>
#include <util/bit_vector.h>

#include <string>
#include <vector>

namespace columnar::core {
// Serializing schema:
// [is_null][data]
template <typename T, DataType type>
class NumericColumn final : public Column {
public:
    NumericColumn(bool nullable = false) : nullable_(nullable) {
    }

    NumericColumn(std::vector<T>&& data, util::BitVector&& is_null, bool nullable)
        : nullable_(nullable), is_null_(std::move(is_null)), data_(std::move(data)) {
    }

    DataType GetDataType() const override {
        return type;
    }

    std::size_t Size() const override {
        return data_.size();
    }

    void Reserve(std::size_t n) override {
        data_.reserve(n);
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
        data_.emplace_back(util::ParseFromString<T>(s));
        if (nullable_) {
            is_null_.PushBack(false);
        }
    }

    void AppendNull() override {
        if (!nullable_) {
            THROW_RUNTIME_ERROR("Cannot set not nullable value to null");
        }
        data_.emplace_back();
        is_null_.PushBack(true);
    }

    void AppendDefault() override {
        data_.emplace_back();
        if (nullable_) {
            is_null_.PushBack(true);
        }
    }

    std::unique_ptr<Column> CloneEmpty() const override {
        return std::make_unique<NumericColumn<T, type>>(nullable_);
    }

    void Clear() override {
        data_.clear();
        is_null_.Clear();
    }

    const T& Get(std::size_t i) const {
        return data_[i];
    }

    std::string GetAsString(std::size_t i) const override {
        if (IsNull(i)) {
            return "";
        }
        return std::to_string(data_[i]);
    }

    const std::vector<T>& GetData() const {
        return data_;
    }

    const util::BitVector& GetNullMask() const {
        return is_null_;
    }

private:
    bool nullable_ = false;
    util::BitVector is_null_;
    std::vector<T> data_;
};
}  // namespace columnar::core

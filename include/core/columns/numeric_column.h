#pragma once

#include <core/columns/abstract_column.h>
#include <core/datatype.h>
#include <util/macro.h>
#include <util/parse.h>

#include <string>
#include <vector>

namespace columnar::core {
template <typename T, DataType type>
class NumericColumn final : public Column {
public:
    NumericColumn(bool nullable = false) : nullable_(nullable) {
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
            is_null_.reserve(n);
        }
    }

    bool IsNullable() const override {
        return nullable_;
    }

    bool IsNull(std::size_t i) const override {
        return nullable_ && is_null_[i];
    }

    void AppendFromString(std::string_view s) override {
        data_.emplace_back(util::ParseFromString<T>(s));
        if (nullable_) {
            is_null_.push_back(false);
        }
    }

    void AppendNull() override {
        if (!nullable_) {
            THROW_RUNTIME_ERROR("Cannot set not nullable value to null");
        }
        data_.emplace_back();
        is_null_.push_back(true);
    }

    void AppendDefault() override {
        data_.emplace_back();
        if (nullable_) {
            is_null_.push_back(true);
        }
    }

    std::unique_ptr<Column> CloneEmpty() const override {
        return std::make_unique<NumericColumn<T, type>>(nullable_);
    }

    void Clear() override {
        data_.clear();
        is_null_.clear();
    }

    const T& Get(std::size_t i) const {
        return data_[i];
    }

    std::string GetAsString(std::size_t i) const override {
        if (IsNull(i)) {
            return "";
        }
        if constexpr (std::is_same_v<T, bool>) {
            return data_[i] ? "true" : "false";
        } else {
            return std::to_string(data_[i]);
        }
    }

private:
    std::vector<T> data_;
    bool nullable_ = false;
    std::vector<bool> is_null_;
};
}  // namespace columnar::core

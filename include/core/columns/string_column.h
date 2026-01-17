#pragma once

#include <core/columns/abstract_column.h>
#include <core/datatype.h>
#include <util/macro.h>
#include <util/parse.h>

#include <vector>

namespace columnar::core {
// TODO (@Irval1337): continuous string storage
class StringColumn final : public Column {
public:
    StringColumn(bool nullable = false) : nullable_(nullable) {
    }

    DataType GetDataType() const override {
        return DataType::String;
    }

    size_t Size() const override {
        return data_.size();
    }

    void Reserve(size_t n) override {
        data_.reserve(n);
        if (nullable_) {
            is_null_.reserve(n);
        }
    }

    bool IsNullable() const override {
        return nullable_;
    }

    bool IsNull(size_t i) const override {
        if (!nullable_) {
            return false;
        }
        return is_null_[i];
    }

    void AppendDefault() override {
        data_.emplace_back();
        if (nullable_) {
            is_null_.push_back(true);
        }
    }

    void AppendFromString(std::string_view s, bool is_null) override {
        if (nullable_) {
            is_null_.push_back(is_null);
        } else if (is_null) {
            THROW_RUNTIME_ERROR("Cannot set not nullable value to null");
        }

        if (is_null) {
            data_.emplace_back();
            return;
        }
        data_.emplace_back(s);
    }

    std::unique_ptr<Column> CloneEmpty() const override {
        return std::make_unique<StringColumn>(nullable_);
    }

    const std::string& Get(size_t i) const {
        return data_[i];
    }

private:
    std::vector<std::string> data_;
    bool nullable_ = false;
    std::vector<bool> is_null_;
};
}  // namespace columnar::core

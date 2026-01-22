#pragma once

#include <core/columns/abstract_column.h>
#include <core/datatype.h>
#include <util/macro.h>
#include <util/parse.h>
#include <util/bit_vector.h>

#include <cstddef>
#include <string>
#include <vector>

namespace columnar::core {
// Serializing schema:
// [is_null][data]
class BoolColumn : public Column {
public:
    BoolColumn(bool nullable = false) : nullable_(nullable) {
    }

    BoolColumn(util::BitVector&& data, util::BitVector&& is_null, bool nullable, std::size_t size)
        : data_(std::move(data)), nullable_(nullable), is_null_(std::move(is_null)), size_(size) {
    }

    DataType GetDataType() const override {
        return DataType::Bool;
    }

    std::size_t Size() const override {
        return size_;
    }

    void Reserve(std::size_t n) override {
        data_.Reserve(n);
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
        data_.PushBack(util::ParseFromString<bool>(s));
        if (nullable_) {
            is_null_.PushBack(false);
        }
        ++size_;
    }

    void AppendNull() override {
        if (!nullable_) {
            THROW_RUNTIME_ERROR("Cannot set not nullable value to null");
        }
        data_.PushBack(false);
        is_null_.PushBack(true);
        ++size_;
    }

    void AppendDefault() override {
        data_.PushBack(false);
        if (nullable_) {
            is_null_.PushBack(true);
        }
        ++size_;
    }

    std::unique_ptr<Column> CloneEmpty() const override {
        return std::make_unique<BoolColumn>(nullable_);
    }

    void Clear() override {
        data_.Clear();
        is_null_.Clear();
        size_ = 0;
    }

    bool Get(std::size_t i) const {
        return data_.Get(i);
    }

    std::string GetAsString(std::size_t i) const override {
        if (IsNull(i)) {
            return "";
        }
        return Get(i) ? "true" : "false";
    }

    const util::BitVector& GetData() const {
        return data_;
    }

    const util::BitVector& GetNullMask() const {
        return is_null_;
    }

private:
    util::BitVector data_;
    bool nullable_ = false;
    util::BitVector is_null_;
    std::size_t size_ = 0;
};
}  // namespace columnar::core

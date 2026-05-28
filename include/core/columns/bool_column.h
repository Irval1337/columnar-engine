#pragma once

#include <core/column.h>
#include <core/datatype.h>
#include <util/macro.h>
#include <util/parse.h>
#include <util/bit_vector.h>

#include <cstddef>
#include <string>

namespace columnar::core {
class BoolColumn : public Column {
public:
    BoolColumn(bool nullable = false) : nullable_(nullable) {
    }

    BoolColumn(util::BitVector&& data, util::BitVector&& is_null, bool nullable, size_t size)
        : data_(std::move(data)), nullable_(nullable), is_null_(std::move(is_null)), size_(size) {
    }

    DataType GetDataType() const override {
        return DataType::Bool;
    }

    ColumnKind GetKind() const override {
        return ColumnKind::Bool;
    }

    size_t Size() const override {
        return size_;
    }

    void Reserve(size_t n) override {
        data_.Reserve(n);
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

    void Append(bool value) {
        data_.PushBack(value);
        if (nullable_) {
            is_null_.PushBack(false);
        }
        ++size_;
    }

    void AppendFromString(std::string_view s) override {
        Append(util::ParseFromString<bool>(s));
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

    void Clear() override {
        data_.Clear();
        is_null_.Clear();
        size_ = 0;
    }

    bool Get(size_t i) const {
        return data_.Get(i);
    }

    std::string GetAsString(size_t i) const override {
        if (IsNull(i)) {
            return "";
        }
        return Get(i) ? "true" : "false";
    }

    void AppendToString(size_t i, std::string& out) const override {
        if (IsNull(i)) {
            return;
        }
        out += Get(i) ? "true" : "false";
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
    size_t size_ = 0;
};
}  // namespace columnar::core

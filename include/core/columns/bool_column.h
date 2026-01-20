#pragma once

#include <core/columns/abstract_column.h>
#include <core/datatype.h>

#include <cstddef>
#include <string>
#include <vector>

namespace columnar::core {
// I'm planning to add simd optimizations in the future
class BoolColumn : public Column {
public:
    BoolColumn(bool nullable = false) : nullable_(nullable) {
    }

    DataType GetDataType() const override {
        return DataType::Bool;
    }

    std::size_t Size() const override {
        return size_;
    }

    void Reserve(std::size_t n) override {
        auto compact_size = (n + 63) / 64;
        data_.reserve(compact_size);
        if (nullable_) {
            is_null_.reserve(compact_size);
        }
    }

    bool IsNullable() const override {
        return nullable_;
    }

    bool IsNull(std::size_t i) const override {
        return nullable_ && GetBit(is_null_, i);
    }

    void AppendFromString(std::string_view s) override {
        if (s == "true" || s == "1") {
            AppendBit(data_, true);
        } else if (s == "false" || s == "0") {
            AppendBit(data_, false);
        } else {
            THROW_RUNTIME_ERROR("Invalid bool value");
        }
        if (nullable_) {
            AppendBit(is_null_, false);
        }
        ++size_;
    }

    void AppendNull() override {
        if (!nullable_) {
            THROW_RUNTIME_ERROR("Cannot set not nullable value to null");
        }
        AppendBit(data_, false);
        AppendBit(is_null_, true);
        ++size_;
    }

    void AppendDefault() override {
        AppendBit(data_, false);
        if (nullable_) {
            AppendBit(is_null_, true);
        }
        ++size_;
    }

    std::unique_ptr<Column> CloneEmpty() const override {
        return std::make_unique<BoolColumn>(nullable_);
    }

    void Clear() override {
        data_.clear();
        is_null_.clear();
        size_ = 0;
    }

    bool Get(std::size_t i) const {
        return GetBit(data_, i);
    }

    std::string GetAsString(std::size_t i) const override {
        if (IsNull(i)) {
            return "";
        }
        return Get(i) ? "true" : "false";
    }

private:
    bool GetBit(const std::vector<uint64_t>& vec, std::size_t i) const {
        return (vec[i / 64] >> (i % 64)) & 1;
    }

    // Warning: This method does not change size
    void AppendBit(std::vector<uint64_t>& vec, bool value) {
        auto idx = size_ % 64;
        if (idx == 0) {
            vec.push_back(0);
        }
        if (value) {
            vec.back() |= 1ULL << idx;
        }
    }

    std::vector<uint64_t> data_;
    std::size_t size_ = 0;
    bool nullable_ = false;
    std::vector<uint64_t> is_null_;
};
}  // namespace columnar::core
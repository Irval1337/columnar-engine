#pragma once

#include <core/column.h>
#include <core/datatype.h>
#include <util/macro.h>
#include <util/parse.h>
#include <util/bit_vector.h>

#include <charconv>
#include <string>
#include <type_traits>
#include <vector>

namespace columnar::core {
template <typename T, DataType type>
class NumericColumn : public Column {
public:
    NumericColumn(bool nullable = false) : nullable_(nullable) {
    }

    NumericColumn(std::vector<T>&& data, util::BitVector&& is_null, bool nullable)
        : nullable_(nullable), is_null_(std::move(is_null)), data_(std::move(data)) {
    }

    DataType GetDataType() const override {
        return type;
    }

    ColumnKind GetKind() const override {
        return ColumnKind::Numeric;
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

    void Append(T value) {
        data_.emplace_back(value);
        if (nullable_) {
            is_null_.PushBack(false);
        }
    }

    void AppendFromString(std::string_view s) override {
        Append(util::ParseFromString<T>(s));
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

    void Clear() override {
        data_.clear();
        is_null_.Clear();
    }

    const T& Get(size_t i) const {
        return data_[i];
    }

    std::string GetAsString(size_t i) const override {
        if (IsNull(i)) {
            return "";
        }
        std::string out;
        AppendToString(i, out);
        return out;
    }

    void AppendToString(size_t i, std::string& out) const override {
        if (IsNull(i)) {
            return;
        }
        char buf[64];
        std::to_chars_result res;
        if constexpr (std::is_floating_point_v<T>) {
            res = std::to_chars(buf, buf + sizeof(buf), data_[i], std::chars_format::fixed, 6);
        } else {
            res = std::to_chars(buf, buf + sizeof(buf), data_[i]);
        }
        if (res.ec == std::errc{}) {
            out.append(buf, static_cast<size_t>(res.ptr - buf));
        } else {
            out += std::to_string(data_[i]);
        }
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

using Int16Column = NumericColumn<int16_t, DataType::Int16>;
using Int32Column = NumericColumn<int32_t, DataType::Int32>;
using Int64Column = NumericColumn<int64_t, DataType::Int64>;
using DoubleColumn = NumericColumn<double, DataType::Double>;
}  // namespace columnar::core

#pragma once

#include <core/column.h>
#include <core/datatype.h>
#include <util/macro.h>
#include <util/bit_vector.h>

#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

namespace columnar::core {
class StringColumn final : public Column {
public:
    StringColumn(bool nullable = false) : offsets_(1, 0), nullable_(nullable) {
    }

    StringColumn(std::vector<char>&& data, std::vector<std::size_t>&& offsets,
                 util::BitVector&& is_null, bool nullable)
        : data_(std::move(data)),
          offsets_(std::move(offsets)),
          nullable_(nullable),
          is_null_(std::move(is_null)) {
        if (offsets_.empty()) {
            offsets_.push_back(0);
        } else if (offsets_.front() != 0 || offsets_.back() != data_.size()) {
            THROW_RUNTIME_ERROR("Corrupted string column");
        }
    }

    DataType GetDataType() const override {
        return DataType::String;
    }

    std::size_t Size() const override {
        return offsets_.size() - 1;
    }

    void Reserve(std::size_t n) override {
        offsets_.reserve(n + 1);
        data_.reserve(n * 12);
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

    void Append(std::string_view s) {
        AppendString(s);
        offsets_.push_back(data_.size());
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
        offsets_.push_back(data_.size());
        is_null_.PushBack(true);
    }

    void AppendDefault() override {
        offsets_.push_back(data_.size());
        if (nullable_) {
            is_null_.PushBack(true);
        }
    }

    void Clear() override {
        data_.clear();
        offsets_.clear();
        offsets_.push_back(0);
        is_null_.Clear();
    }

    std::string_view Get(size_t i) const {
        return std::string_view(data_.data() + offsets_[i], offsets_[i + 1] - offsets_[i]);
    }

    std::string GetAsString(std::size_t i) const override {
        if (IsNull(i)) {
            return "";
        }
        return std::string(data_.data() + offsets_[i], offsets_[i + 1] - offsets_[i]);
    }

    void AppendToString(std::size_t i, std::string& out) const override {
        if (IsNull(i)) {
            return;
        }
        out.append(data_.data() + offsets_[i], offsets_[i + 1] - offsets_[i]);
    }

    const std::vector<char>& GetData() const {
        return data_;
    }

    const std::vector<std::size_t>& GetOffsets() const {
        return offsets_;
    }

    const util::BitVector& GetNullMask() const {
        return is_null_;
    }

private:
    void AppendString(std::string_view s) {
        if (s.empty()) {
            return;
        }
        std::size_t pos = data_.size();
        data_.resize(pos + s.size());
        std::memcpy(data_.data() + pos, s.data(), s.size());
    }

    std::vector<char> data_;
    std::vector<std::size_t> offsets_;
    bool nullable_ = false;
    util::BitVector is_null_;
};
}  // namespace columnar::core

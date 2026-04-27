#pragma once

#include <core/columns/numeric_column.h>
#include <util/date_time.h>

namespace columnar::core {
class DateColumn final : public Int32Column {
public:
    using Int32Column::Int32Column;

    DataType GetDataType() const override {
        return DataType::Date;
    }

    void AppendFromString(std::string_view s) override {
        Append(util::ParseDate(s));
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
        util::AppendDate(Get(i), out);
    }
};
}  // namespace columnar::core

#pragma once

#include <core/columns/numeric_column.h>
#include <util/date_time.h>

namespace columnar::core {
class TimestampColumn final : public Int64Column {
public:
    using Int64Column::Int64Column;

    DataType GetDataType() const override {
        return DataType::Timestamp;
    }

    void AppendFromString(std::string_view s) override {
        Append(util::ParseTimestamp(s));
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
        util::AppendTimestamp(Get(i), out);
    }
};
}  // namespace columnar::core

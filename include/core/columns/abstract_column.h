#pragma once

#include <core/datatype.h>

#include <memory>
#include <string_view>
#include <cstdint>

namespace columnar::core {
class Column {
public:
    virtual ~Column() = default;

    virtual DataType GetDataType() const = 0;

    virtual std::size_t Size() const = 0;

    virtual void Reserve(std::size_t n) = 0;

    virtual void AppendFromString(std::string_view s, bool is_null) = 0;

    virtual void AppendDefault() = 0;

    virtual bool IsNullable() const = 0;

    virtual bool IsNull(std::size_t i) const = 0;

    virtual std::unique_ptr<Column> CloneEmpty() const = 0;
};
}  // namespace columnar::core

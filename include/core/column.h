#pragma once

#include <core/datatype.h>

#include <cstddef>
#include <string>
#include <string_view>

namespace columnar::core {
class Column {
public:
    virtual ~Column() = default;

    virtual DataType GetDataType() const = 0;

    virtual size_t Size() const = 0;

    virtual void Reserve(size_t n) = 0;

    virtual void AppendFromString(std::string_view s) = 0;

    virtual void AppendNull() = 0;

    virtual void AppendDefault() = 0;

    virtual bool IsNullable() const = 0;

    virtual bool IsNull(size_t i) const = 0;

    virtual std::string GetAsString(size_t i) const = 0;

    virtual void AppendToString(size_t i, std::string& out) const = 0;

    virtual void Clear() = 0;
};
}  // namespace columnar::core

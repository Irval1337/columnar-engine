#pragma once

#include <core/datatype.h>

#include <string>

namespace columnar::core {
struct Field {
    std::string name;
    DataType type;
    bool nullable = false;

    Field() = default;

    Field(std::string field_name, DataType field_type, bool is_nullable = false)
        : name(std::move(field_name)), type(field_type), nullable(is_nullable) {
    }
};
}  // namespace columnar::core

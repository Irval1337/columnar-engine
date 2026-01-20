#pragma once

#include <util/macro.h>

#include <cstdint>
#include <string_view>

namespace columnar::core {
enum class DataType : std::uint8_t {
    Int64,
    Double,
    Bool,
    String,
};

inline DataType StringToDataType(std::string_view s) {
    if (s == "int64") {
        return DataType::Int64;
    }
    if (s == "double") {
        return DataType::Double;
    }
    if (s == "bool") {
        return DataType::Bool;
    }
    if (s == "string") {
        return DataType::String;
    }
    THROW_RUNTIME_ERROR("Unsupported data type");
}

inline std::string DataTypeToString(DataType type) {
    switch (type) {
        case DataType::Int64:
            return "int64";
        case DataType::Double:
            return "double";
        case DataType::Bool:
            return "bool";
        case DataType::String:
            return "string";
        default:
            THROW_RUNTIME_ERROR("Unsupported data type");
    }
}
}  // namespace columnar::core

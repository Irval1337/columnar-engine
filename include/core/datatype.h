#pragma once

#include <util/macro.h>

#include <cstdint>
#include <string>
#include <string_view>

namespace columnar::core {
enum class DataType : uint8_t {
    Int16,
    Int32,
    Int64,
    Double,
    Bool,
    String,
    Date,
    Timestamp,
    Char,
};

enum class PhysicalType : uint8_t {
    Int16,
    Int32,
    Int64,
    Double,
    Bool,
    String,
    Char,
};

inline PhysicalType DataTypeToPhysical(DataType type) {
    switch (type) {
        case DataType::Int16:
            return PhysicalType::Int16;
        case DataType::Int32:
            return PhysicalType::Int32;
        case DataType::Int64:
            return PhysicalType::Int64;
        case DataType::Double:
            return PhysicalType::Double;
        case DataType::Bool:
            return PhysicalType::Bool;
        case DataType::String:
            return PhysicalType::String;
        case DataType::Date:
            return PhysicalType::Int32;
        case DataType::Timestamp:
            return PhysicalType::Int64;
        case DataType::Char:
            return PhysicalType::Char;
    }
    THROW_RUNTIME_ERROR("Unknown DataType value " + std::to_string(static_cast<int>(type)));
}

inline DataType StringToDataType(std::string_view s) {
    if (s == "int16") {
        return DataType::Int16;
    }
    if (s == "int32") {
        return DataType::Int32;
    }
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
    if (s == "date") {
        return DataType::Date;
    }
    if (s == "timestamp") {
        return DataType::Timestamp;
    }
    if (s == "char") {
        return DataType::Char;
    }
    THROW_RUNTIME_ERROR("Unsupported data type name: '" + std::string(s) + "'");
}

inline std::string DataTypeToString(DataType type) {
    switch (type) {
        case DataType::Int16:
            return "int16";
        case DataType::Int32:
            return "int32";
        case DataType::Int64:
            return "int64";
        case DataType::Double:
            return "double";
        case DataType::Bool:
            return "bool";
        case DataType::String:
            return "string";
        case DataType::Date:
            return "date";
        case DataType::Timestamp:
            return "timestamp";
        case DataType::Char:
            return "char";
    }
    THROW_RUNTIME_ERROR("Unsupported DataType value " + std::to_string(static_cast<int>(type)));
}
}  // namespace columnar::core

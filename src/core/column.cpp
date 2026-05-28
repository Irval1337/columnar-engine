#include <core/column_factory.h>
#include <core/columns/date_column.h>
#include <core/columns/timestamp_column.h>
#include <core/datatype.h>
#include <util/macro.h>

namespace columnar::core {
std::unique_ptr<Column> MakeColumn(DataType type, bool nullable) {
    switch (type) {
        case DataType::Int16:
            return std::make_unique<Int16Column>(nullable);
        case DataType::Int32:
            return std::make_unique<Int32Column>(nullable);
        case DataType::Int64:
            return std::make_unique<Int64Column>(nullable);
        case DataType::Double:
            return std::make_unique<DoubleColumn>(nullable);
        case DataType::Bool:
            return std::make_unique<BoolColumn>(nullable);
        case DataType::String:
            return std::make_unique<StringColumn>(nullable);
        case DataType::Date:
            return std::make_unique<DateColumn>(nullable);
        case DataType::Timestamp:
            return std::make_unique<TimestampColumn>(nullable);
        case DataType::Char:
            return std::make_unique<CharColumn>(nullable);
    }
    THROW_RUNTIME_ERROR("MakeColumn: unsupported DataType " +
                        std::to_string(static_cast<int>(type)));
}
}  // namespace columnar::core

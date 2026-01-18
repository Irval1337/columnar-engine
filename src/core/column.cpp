#include <core/column.h>
#include <core/datatype.h>
#include <util/macro.h>

namespace columnar::core {
std::unique_ptr<Column> MakeColumn(DataType id, bool nullable) {
    switch (id) {
        case DataType::Int64:
            return std::make_unique<Int64Column>(nullable);
        case DataType::Double:
            return std::make_unique<DoubleColumn>(nullable);
        case DataType::String:
            return std::make_unique<StringColumn>(nullable);
        default:
            THROW_NOT_IMPLEMENTED;
    }
}
}  // namespace columnar::core

#include <core/columns/numeric_column.h>
#include <core/columns/timestamp_column.h>
#include <exec/column_row_access.h>
#include <exec/kernel.h>
#include <util/macro.h>

#include <cstdint>
#include <memory>

namespace columnar::exec::kernel {
namespace {
template <typename Transform>
std::unique_ptr<core::Column> MapTimestamp(const core::Column& operand, bool to_timestamp,
                                           Transform&& transform) {
    if (operand.GetDataType() != core::DataType::Timestamp) {
        THROW_RUNTIME_ERROR("expected a timestamp column");
    }
    size_t rows = operand.Size();
    std::unique_ptr<core::Column> out =
        to_timestamp ? std::unique_ptr<core::Column>(
                           std::make_unique<core::TimestampColumn>(operand.IsNullable()))
                     : std::unique_ptr<core::Column>(
                           std::make_unique<core::Int64Column>(operand.IsNullable()));
    for (size_t i = 0; i < rows; ++i) {
        if (operand.IsNull(i)) {
            out->AppendNull();
        } else {
            AppendInteger(*out, transform(ReadIntegerRow(operand, i)));
        }
    }
    return out;
}
}  // namespace

std::unique_ptr<core::Column> ExtractMinute(const core::Column& operand) {
    return MapTimestamp(operand, /*to_timestamp=*/false,
                        [](int64_t seconds) { return (seconds / 60) % 60; });
}

std::unique_ptr<core::Column> TruncMinute(const core::Column& operand) {
    return MapTimestamp(operand, /*to_timestamp=*/true,
                        [](int64_t seconds) { return seconds - seconds % 60; });
}
}  // namespace columnar::exec::kernel

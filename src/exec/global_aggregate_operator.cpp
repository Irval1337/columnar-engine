#include <exec/global_aggregate_operator.h>

#include <core/columns/numeric_column.h>
#include <core/columns/string_column.h>
#include <exec/column_helpers.h>
#include <exec/expression.h>
#include <exec/kernel.h>
#include <util/macro.h>

#include <utility>

namespace columnar::exec {
namespace {
bool IsNumeric(core::DataType type) {
    return type == core::DataType::Double || HasIntegerValue(type);
}

core::DataType OutputType(const AggregationUnit& unit) {
    switch (unit.type) {
        case AggregationType::Count:
        case AggregationType::Distinct:
            return core::DataType::Int64;
        case AggregationType::Sum:
            return GetExpressionType(*unit.expression) == core::DataType::Double
                       ? core::DataType::Double
                       : core::DataType::Int64;
        case AggregationType::Avg:
            return core::DataType::Double;
        case AggregationType::Min:
        case AggregationType::Max:
            return GetExpressionType(*unit.expression);
    }
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}

bool OutputNullable(const AggregationUnit& unit) {
    switch (unit.type) {
        case AggregationType::Count:
        case AggregationType::Distinct:
            return false;
        case AggregationType::Sum:
        case AggregationType::Avg:
        case AggregationType::Min:
        case AggregationType::Max:
            return true;
    }
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}

template <typename T>
void MergeMinMax(bool is_min, const kernel::ScalarReduction<T>& part, T& acc, bool& has_value) {
    if (!part.has_value) {
        return;
    }
    if (!has_value) {
        acc = part.value;
        has_value = true;
    } else if (is_min ? part.value < acc : part.value > acc) {
        acc = part.value;
    }
}
}  // namespace

core::Schema MakeGlobalAggregateSchema(const std::vector<AggregationUnit>& aggregations) {
    std::vector<core::Field> fields;
    fields.reserve(aggregations.size());
    for (auto& unit : aggregations) {
        fields.emplace_back(unit.name, OutputType(unit), OutputNullable(unit));
    }
    return core::Schema(std::move(fields));
}

GlobalAggregationSink::GlobalAggregationSink(IOperator& downstream,
                                             std::vector<AggregationUnit> aggregations)
    : downstream_(downstream),
      output_schema_(MakeGlobalAggregateSchema(aggregations)),
      aggregations_(std::move(aggregations)),
      states_(aggregations_.size()) {
}

void GlobalAggregationSink::Consume(core::Batch batch) {
    for (size_t unit = 0; unit < aggregations_.size(); ++unit) {
        UpdateState(batch, unit);
    }
}

void GlobalAggregationSink::Finalize() {
    core::Batch result(output_schema_, 1);
    for (size_t unit = 0; unit < aggregations_.size(); ++unit) {
        AppendResult(unit, result.ColumnAt(unit));
    }
    downstream_.Consume(std::move(result));
    downstream_.Finalize();
}

void GlobalAggregationSink::UpdateState(const core::Batch& batch, size_t unit_index) {
    auto& unit = aggregations_[unit_index];
    auto& state = states_[unit_index];

    if (unit.type == AggregationType::Count) {
        state.count += batch.RowsCount();
        return;
    }

    auto eval = Evaluate(batch, *unit.expression);
    auto& col = eval.Get();
    auto type = col.GetDataType();

    switch (unit.type) {
        case AggregationType::Sum: {
            if (!IsNumeric(type)) {
                THROW_RUNTIME_ERROR("SUM supports only numeric columns");
            }
            if (type == core::DataType::Double) {
                auto part = kernel::SumDouble(col);
                if (part.has_value) {
                    state.double_sum += part.value;
                    state.has_value = true;
                }
            } else {
                auto part = kernel::SumInt(col);
                if (part.has_value) {
                    state.int_sum += part.value;
                    state.has_value = true;
                }
            }
            return;
        }
        case AggregationType::Avg: {
            if (!IsNumeric(type)) {
                THROW_RUNTIME_ERROR("AVG supports only numeric columns");
            }
            auto part = kernel::Avg(col);
            state.double_sum += part.sum;
            state.count += part.count;
            return;
        }
        case AggregationType::Distinct: {
            if (type == core::DataType::String) {
                kernel::DistinctStrings(col, state.strings);
            } else if (HasIntegerValue(type)) {
                kernel::DistinctInts(col, state.ints);
            } else {
                THROW_RUNTIME_ERROR("COUNT DISTINCT supports only integer-like and string columns");
            }
            return;
        }
        case AggregationType::Min:
        case AggregationType::Max: {
            bool is_min = unit.type == AggregationType::Min;
            if (type == core::DataType::String) {
                auto part = is_min ? kernel::MinString(col) : kernel::MaxString(col);
                MergeMinMax(is_min, part, state.string_value, state.has_value);
            } else if (type == core::DataType::Double) {
                auto part = is_min ? kernel::MinDouble(col) : kernel::MaxDouble(col);
                MergeMinMax(is_min, part, state.double_value, state.has_value);
            } else if (HasIntegerValue(type)) {
                auto part = is_min ? kernel::MinInt(col) : kernel::MaxInt(col);
                MergeMinMax(is_min, part, state.int_value, state.has_value);
            } else {
                THROW_RUNTIME_ERROR("MIN/MAX does not support this column type");
            }
            return;
        }
        case AggregationType::Count:
            return;
    }
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}

void GlobalAggregationSink::AppendResult(size_t unit_index, core::Column& out) const {
    auto& unit = aggregations_[unit_index];
    auto& state = states_[unit_index];

    switch (unit.type) {
        case AggregationType::Count:
            AppendInteger(out, static_cast<int64_t>(state.count));
            return;
        case AggregationType::Distinct:
            AppendInteger(out, static_cast<int64_t>(state.ints.size() + state.strings.size()));
            return;
        case AggregationType::Sum:
            if (!state.has_value) {
                out.AppendNull();
                return;
            }
            if (out.GetDataType() == core::DataType::Double) {
                AppendDouble(out, static_cast<double>(state.double_sum));
            } else {
                AppendInteger(out, state.int_sum);
            }
            return;
        case AggregationType::Avg:
            if (state.count == 0) {
                out.AppendNull();
                return;
            }
            AppendDouble(
                out, static_cast<double>(state.double_sum / static_cast<long double>(state.count)));
            return;
        case AggregationType::Min:
        case AggregationType::Max:
            if (!state.has_value) {
                out.AppendNull();
                return;
            }
            if (out.GetDataType() == core::DataType::String) {
                out.AppendFromString(state.string_value);
            } else if (out.GetDataType() == core::DataType::Double) {
                AppendDouble(out, state.double_value);
            } else {
                AppendInteger(out, state.int_value);
            }
            return;
    }
    THROW_RUNTIME_ERROR("Unsupported aggregate type");
}
}  // namespace columnar::exec

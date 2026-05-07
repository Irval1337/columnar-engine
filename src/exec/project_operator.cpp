#include <exec/project_operator.h>

#include <core/field.h>
#include <exec/column_helpers.h>
#include <exec/expression.h>

#include <utility>

namespace columnar::exec {
namespace {
core::Schema MakeProjectionSchema(const std::vector<ProjectionUnit>& projections) {
    std::vector<core::Field> fields;
    fields.reserve(projections.size());
    for (auto& unit : projections) {
        fields.emplace_back(unit.name, GetExpressionType(*unit.expression), true);
    }
    return core::Schema(std::move(fields));
}
}  // namespace

ProjectSink::ProjectSink(IOperator& downstream, std::vector<ProjectionUnit> projections)
    : downstream_(downstream),
      output_schema_(MakeProjectionSchema(projections)),
      projections_(std::move(projections)) {
}

void ProjectSink::Consume(core::Batch batch) {
    size_t rows = batch.RowsCount();
    core::Batch out(output_schema_, rows);
    for (size_t i = 0; i < projections_.size(); ++i) {
        auto eval = Evaluate(batch, *projections_[i].expression);
        auto& src = eval.Get();
        auto& dst = out.ColumnAt(i);
        for (size_t row = 0; row < rows; ++row) {
            AppendRow(dst, src, row);
        }
    }
    downstream_.Consume(std::move(out));
}
}  // namespace columnar::exec

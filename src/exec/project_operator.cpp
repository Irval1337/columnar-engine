#include <exec/project_operator.h>

#include <core/field.h>
#include <exec/column_row_access.h>
#include <exec/expression/types.h>
#include <exec/expression/eval.h>
#include <exec/expression/utils.h>
#include <exec/kernel.h>

#include <utility>

namespace columnar::exec {
namespace {
bool ProjectionNullable(const ProjectionUnit& unit, const core::Schema& input_schema) {
    if (unit.expression->type == ExpressionType::Column) {
        auto& column = static_cast<const ColumnExpr&>(*unit.expression);
        return input_schema.GetFields()[input_schema.GetIndex(column.name)].nullable;
    }
    return unit.expression->type != ExpressionType::ConstInt64 &&
           unit.expression->type != ExpressionType::ConstString;
}

core::Schema MakeProjectionSchema(const std::vector<ProjectionUnit>& projections,
                                  const core::Schema& input_schema) {
    std::vector<core::Field> fields;
    fields.reserve(projections.size());
    for (auto& unit : projections) {
        fields.emplace_back(unit.name, GetExpressionType(*unit.expression),
                            ProjectionNullable(unit, input_schema));
    }
    return core::Schema(std::move(fields));
}
}  // namespace

ProjectSink::ProjectSink(IOperator& downstream, std::vector<ProjectionUnit> projections)
    : downstream_(downstream),
      projections_(std::move(projections)),
      needs_dense_(RequiresDenseBatch(projections_)) {
}

void ProjectSink::Consume(core::Batch batch) {
    if (batch.HasSelection() && needs_dense_) {
        batch = kernel::Materialize(batch);
    }
    size_t rows = batch.RowsCount();
    const std::vector<uint32_t>* selection = batch.HasSelection() ? &batch.Selection() : nullptr;
    auto output_schema = MakeProjectionSchema(projections_, batch.GetSchema());
    core::Batch out(std::move(output_schema), batch.SelectedRowsCount());
    for (size_t i = 0; i < projections_.size(); ++i) {
        auto eval = Evaluate(batch, *projections_[i].expression);
        auto& src = eval.Get();
        auto& dst = out.ColumnAt(i);
        if (selection != nullptr) {
            for (uint32_t row : *selection) {
                AppendRow(dst, src, row);
            }
        } else {
            for (size_t row = 0; row < rows; ++row) {
                AppendRow(dst, src, row);
            }
        }
    }
    downstream_.Consume(std::move(out));
}
}  // namespace columnar::exec

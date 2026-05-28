#include <exec/operator.h>

#include <core/columns/numeric_column.h>
#include <exec/expression/eval.h>
#include <exec/filter_operator.h>
#include <exec/global_aggregate_operator.h>
#include <exec/hash_aggregate_operator.h>
#include <exec/kernel.h>
#include <exec/metadata_pruning.h>
#include <exec/operator_visit.h>
#include <exec/project_operator.h>
#include <exec/topn_operator.h>
#include <util/macro.h>

namespace columnar::exec {
void CollectSink::Consume(core::Batch batch) {
    if (batch.HasSelection()) {
        batch = kernel::Materialize(batch);
    }
    batches_.push_back(std::move(batch));
}

namespace {
std::vector<std::string> ScanColumnNames(const ScanOperator& scan) {
    std::vector<std::string> columns;
    columns.reserve(scan.schema.FieldsCount());
    for (auto& field : scan.schema.GetFields()) {
        columns.push_back(field.name);
    }
    return columns;
}

core::Batch MakeCountBatch(const CountTableOperator& op, uint64_t rows) {
    core::Schema schema({core::Field(op.output_name, core::DataType::Int64)});
    core::Batch batch(schema, 1);
    static_cast<core::Int64Column&>(batch.ColumnAt(0)).Append(static_cast<int64_t>(rows));
    return batch;
}

void ExecuteScanInto(bruh::BruhBatchReader& reader, const ScanOperator& scan,
                     IOperator& downstream) {
    auto column_indexes = reader.ResolveColumnNames(ScanColumnNames(scan));
    for (size_t group = 0; group < reader.NumRowGroups(); ++group) {
        downstream.Consume(reader.ReadRowGroup(group, column_indexes));
    }
    downstream.Finalize();
}

void ExecuteFilterScanInto(bruh::BruhBatchReader& reader, const ScanOperator& scan,
                           const std::shared_ptr<Expression>& condition, IOperator& downstream) {
    auto column_indexes = reader.ResolveColumnNames(ScanColumnNames(scan));
    FilterSink sink(downstream, condition);
    for (size_t group = 0; group < reader.NumRowGroups(); ++group) {
        if (!PredicateMayMatch(reader, group, *condition)) {
            continue;
        }
        sink.Consume(reader.ReadRowGroup(group, column_indexes));
    }
    sink.Finalize();
}

void PlanRec(const std::shared_ptr<Operator>& op, const core::Schema& table_schema,
             std::vector<std::string> required_columns);

struct PlanVisitor {
    const core::Schema& table_schema;
    std::vector<std::string>& required_columns;

    void Visit(ScanOperator& scan) const {
        std::vector<core::Field> fields;
        fields.reserve(required_columns.size());
        for (auto& name : required_columns) {
            auto* field = table_schema.FindField(name);
            if (field == nullptr) {
                THROW_RUNTIME_ERROR("Unknown field: " + name);
            }
            fields.push_back(*field);
        }
        scan.schema = core::Schema(std::move(fields));
    }

    void Visit(CountTableOperator&) const {
    }

    void Visit(FilterOperator& filter) const {
        CollectColumns(*filter.condition, required_columns);
        PlanRec(filter.child, table_schema, std::move(required_columns));
    }

    void Visit(GlobalAggregationOperator& aggregate) const {
        std::vector<std::string> child_required;
        for (auto& unit : aggregate.aggregations) {
            if (unit.expression != nullptr) {
                CollectColumns(*unit.expression, child_required);
            }
        }
        PlanRec(aggregate.child, table_schema, std::move(child_required));
    }

    void Visit(HashAggregationOperator& aggregate) const {
        std::vector<std::string> child_required;
        for (auto& key : aggregate.keys) {
            CollectColumns(*key.expression, child_required);
        }
        for (auto& unit : aggregate.aggregations) {
            if (unit.expression != nullptr) {
                CollectColumns(*unit.expression, child_required);
            }
        }
        PlanRec(aggregate.child, table_schema, std::move(child_required));
    }

    void Visit(ProjectOperator& project) const {
        std::vector<std::string> child_required;
        for (auto& unit : project.projections) {
            CollectColumns(*unit.expression, child_required);
        }
        if (child_required.empty() && table_schema.FieldsCount() > 0) {
            child_required.push_back(table_schema.GetFields()[0].name);
        }
        PlanRec(project.child, table_schema, std::move(child_required));
    }

    void Visit(TopNOperator& topn) const {
        for (auto& unit : topn.sort_units) {
            CollectColumns(*unit.expression, required_columns);
        }
        PlanRec(topn.child, table_schema, std::move(required_columns));
    }
};

void PlanRec(const std::shared_ptr<Operator>& op, const core::Schema& table_schema,
             std::vector<std::string> required_columns) {
    PlanVisitor visitor{table_schema, required_columns};
    VisitOperator(*op, visitor);
}

void ExecuteInto(bruh::BruhBatchReader& reader, const std::shared_ptr<Operator>& op,
                 IOperator& downstream);

struct ExecuteVisitor {
    bruh::BruhBatchReader& reader;
    IOperator& downstream;

    void Visit(const ScanOperator& scan) const {
        ExecuteScanInto(reader, scan, downstream);
    }

    void Visit(const CountTableOperator& count) const {
        downstream.Consume(MakeCountBatch(count, reader.GetMetaData().rows_count));
        downstream.Finalize();
    }

    void Visit(const GlobalAggregationOperator& aggregate) const {
        GlobalAggregationSink sink(downstream, aggregate.aggregations);
        ExecuteInto(reader, aggregate.child, sink);
    }

    void Visit(const HashAggregationOperator& aggregate) const {
        HashAggregationSink sink(downstream, aggregate.keys, aggregate.aggregations);
        ExecuteInto(reader, aggregate.child, sink);
    }

    void Visit(const FilterOperator& filter) const {
        if (filter.child->type == OperatorType::Scan) {
            ExecuteFilterScanInto(reader, static_cast<const ScanOperator&>(*filter.child),
                                  filter.condition, downstream);
            return;
        }
        FilterSink sink(downstream, filter.condition);
        ExecuteInto(reader, filter.child, sink);
    }

    void Visit(const ProjectOperator& project) const {
        ProjectSink sink(downstream, project.projections);
        ExecuteInto(reader, project.child, sink);
    }

    void Visit(const TopNOperator& topn) const {
        TopNSink sink(downstream, topn.sort_units, topn.limit, topn.offset);
        ExecuteInto(reader, topn.child, sink);
    }
};

void ExecuteInto(bruh::BruhBatchReader& reader, const std::shared_ptr<Operator>& op,
                 IOperator& downstream) {
    ExecuteVisitor visitor{reader, downstream};
    VisitOperator(*op, visitor);
}
}  // namespace

std::vector<core::Batch> Execute(bruh::BruhBatchReader& reader, std::shared_ptr<Operator> op) {
    PlanRec(op, reader.GetSchema(), {});
    CollectSink sink;
    ExecuteInto(reader, op, sink);
    return sink.TakeBatches();
}
}  // namespace columnar::exec

#include <exec/operator.h>

#include <core/columns/numeric_column.h>
#include <exec/filter_operator.h>
#include <exec/global_aggregate_operator.h>
#include <exec/hash_aggregate_operator.h>
#include <exec/project_operator.h>
#include <exec/topn_operator.h>
#include <util/macro.h>

namespace columnar::exec {
namespace {
template <typename T>
const T& As(const std::shared_ptr<Operator>& op) {
    return static_cast<const T&>(*op);
}

void PlanRec(const std::shared_ptr<Operator>& op, const core::Schema& table_schema,
             std::vector<std::string> required_columns) {
    switch (op->type) {
        case OperatorType::Scan: {
            auto& scan = static_cast<ScanOperator&>(*op);
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
            return;
        }
        case OperatorType::CountTable:
            return;
        case OperatorType::Filter: {
            auto& filter = As<FilterOperator>(op);
            CollectColumns(*filter.condition, required_columns);
            PlanRec(filter.child, table_schema, std::move(required_columns));
            return;
        }
        case OperatorType::Aggregation: {
            auto& aggregate = As<AggregationOperator>(op);
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
            return;
        }
        case OperatorType::Project: {
            auto& project = As<ProjectOperator>(op);
            std::vector<std::string> child_required;
            for (auto& unit : project.projections) {
                CollectColumns(*unit.expression, child_required);
            }
            if (child_required.empty() && table_schema.FieldsCount() > 0) {
                child_required.push_back(table_schema.GetFields()[0].name);
            }
            PlanRec(project.child, table_schema, std::move(child_required));
            return;
        }
        case OperatorType::TopN: {
            auto& topn = As<TopNOperator>(op);
            for (auto& unit : topn.sort_units) {
                CollectColumns(*unit.expression, required_columns);
            }
            PlanRec(topn.child, table_schema, std::move(required_columns));
            return;
        }
    }
    THROW_RUNTIME_ERROR("Unsupported operator type");
}

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

void ExecuteInto(bruh::BruhBatchReader& reader, const std::shared_ptr<Operator>& op,
                 IOperator& downstream) {
    switch (op->type) {
        case OperatorType::Scan: {
            auto& scan = As<ScanOperator>(op);
            auto column_indexes = reader.ResolveColumnNames(ScanColumnNames(scan));
            for (size_t group = 0; group < reader.NumRowGroups(); ++group) {
                downstream.Consume(reader.ReadRowGroup(group, column_indexes));
            }
            downstream.Finalize();
            return;
        }
        case OperatorType::CountTable: {
            downstream.Consume(
                MakeCountBatch(As<CountTableOperator>(op), reader.GetMetaData().rows_count));
            downstream.Finalize();
            return;
        }
        case OperatorType::Aggregation: {
            auto& aggregate = As<AggregationOperator>(op);
            if (aggregate.keys.empty()) {
                GlobalAggregationSink sink(downstream, aggregate.aggregations);
                ExecuteInto(reader, aggregate.child, sink);
            } else {
                HashAggregationSink sink(downstream, aggregate.keys, aggregate.aggregations);
                ExecuteInto(reader, aggregate.child, sink);
            }
            return;
        }
        case OperatorType::Filter: {
            auto& filter = As<FilterOperator>(op);
            FilterSink sink(downstream, filter.condition);
            ExecuteInto(reader, filter.child, sink);
            return;
        }
        case OperatorType::Project: {
            auto& project = As<ProjectOperator>(op);
            ProjectSink sink(downstream, project.projections);
            ExecuteInto(reader, project.child, sink);
            return;
        }
        case OperatorType::TopN: {
            auto& topn = As<TopNOperator>(op);
            TopNSink sink(downstream, topn.sort_units, topn.limit);
            ExecuteInto(reader, topn.child, sink);
            return;
        }
    }
    THROW_RUNTIME_ERROR("Unsupported operator type");
}
}  // namespace

std::vector<core::Batch> Execute(bruh::BruhBatchReader& reader, std::shared_ptr<Operator> op) {
    PlanRec(op, reader.GetSchema(), {});
    CollectSink sink;
    ExecuteInto(reader, op, sink);
    return sink.TakeBatches();
}
}  // namespace columnar::exec

#pragma once

#include <exec/operator.h>
#include <util/macro.h>

namespace columnar::exec {
template <typename Visitor>
void VisitOperator(const Operator& op, Visitor& visitor) {
    switch (op.type) {
        case OperatorType::Scan:
            visitor.Visit(static_cast<const ScanOperator&>(op));
            return;
        case OperatorType::CountTable:
            visitor.Visit(static_cast<const CountTableOperator&>(op));
            return;
        case OperatorType::GlobalAggregation:
            visitor.Visit(static_cast<const GlobalAggregationOperator&>(op));
            return;
        case OperatorType::HashAggregation:
            visitor.Visit(static_cast<const HashAggregationOperator&>(op));
            return;
        case OperatorType::Filter:
            visitor.Visit(static_cast<const FilterOperator&>(op));
            return;
        case OperatorType::Project:
            visitor.Visit(static_cast<const ProjectOperator&>(op));
            return;
        case OperatorType::TopN:
            visitor.Visit(static_cast<const TopNOperator&>(op));
            return;
    }
    THROW_RUNTIME_ERROR("Unsupported operator type " + std::to_string(static_cast<int>(op.type)));
}

template <typename Visitor>
void VisitOperator(Operator& op, Visitor& visitor) {
    switch (op.type) {
        case OperatorType::Scan:
            visitor.Visit(static_cast<ScanOperator&>(op));
            return;
        case OperatorType::CountTable:
            visitor.Visit(static_cast<CountTableOperator&>(op));
            return;
        case OperatorType::GlobalAggregation:
            visitor.Visit(static_cast<GlobalAggregationOperator&>(op));
            return;
        case OperatorType::HashAggregation:
            visitor.Visit(static_cast<HashAggregationOperator&>(op));
            return;
        case OperatorType::Filter:
            visitor.Visit(static_cast<FilterOperator&>(op));
            return;
        case OperatorType::Project:
            visitor.Visit(static_cast<ProjectOperator&>(op));
            return;
        case OperatorType::TopN:
            visitor.Visit(static_cast<TopNOperator&>(op));
            return;
    }
    THROW_RUNTIME_ERROR("Unsupported operator type " + std::to_string(static_cast<int>(op.type)));
}
}  // namespace columnar::exec

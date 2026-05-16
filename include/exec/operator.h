#pragma once

#include <bruh/bruh_batch_reader.h>
#include <core/batch.h>
#include <core/schema.h>
#include <exec/aggregation.h>
#include <exec/expression.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace columnar::exec {
class IOperator {
public:
    virtual ~IOperator() = default;

    virtual void Consume(core::Batch batch) = 0;

    virtual void Finalize() = 0;
};

class CollectSink final : public IOperator {
public:
    void Consume(core::Batch batch) override;

    void Finalize() override {
    }

    std::vector<core::Batch> TakeBatches() {
        return std::move(batches_);
    }

private:
    std::vector<core::Batch> batches_;
};

enum class OperatorType {
    Scan,
    CountTable,
    Aggregation,
    Filter,
    Project,
    TopN,
};

struct ProjectionUnit {
    std::shared_ptr<Expression> expression;
    std::string name;
};

struct SortUnit {
    std::shared_ptr<Expression> expression;
    bool ascending = true;
};

struct Operator {
    explicit Operator(OperatorType type) : type(type) {
    }

    virtual ~Operator() = default;

    OperatorType type;
};

struct ScanOperator final : public Operator {
    ScanOperator() : Operator(OperatorType::Scan) {
    }

    core::Schema schema;
};

struct CountTableOperator final : public Operator {
    explicit CountTableOperator(std::string output_name = "count")
        : Operator(OperatorType::CountTable), output_name(std::move(output_name)) {
    }

    std::string output_name;
};

struct AggregationOperator final : public Operator {
    AggregationOperator(std::shared_ptr<Operator> child, std::vector<ProjectionUnit> keys,
                        std::vector<AggregationUnit> aggregations)
        : Operator(OperatorType::Aggregation),
          child(std::move(child)),
          keys(std::move(keys)),
          aggregations(std::move(aggregations)) {
    }

    std::shared_ptr<Operator> child;
    std::vector<ProjectionUnit> keys;  // empty => global aggregation
    std::vector<AggregationUnit> aggregations;
};

struct FilterOperator final : public Operator {
    FilterOperator(std::shared_ptr<Operator> child, std::shared_ptr<Expression> condition)
        : Operator(OperatorType::Filter), child(std::move(child)), condition(std::move(condition)) {
    }

    std::shared_ptr<Operator> child;
    std::shared_ptr<Expression> condition;
};

struct ProjectOperator final : public Operator {
    ProjectOperator(std::shared_ptr<Operator> child, std::vector<ProjectionUnit> projections)
        : Operator(OperatorType::Project),
          child(std::move(child)),
          projections(std::move(projections)) {
    }

    std::shared_ptr<Operator> child;
    std::vector<ProjectionUnit> projections;
};

struct TopNOperator final : public Operator {
    TopNOperator(std::shared_ptr<Operator> child, std::vector<SortUnit> sort_units,
                 std::optional<size_t> limit, std::optional<size_t> offset)
        : Operator(OperatorType::TopN),
          child(std::move(child)),
          sort_units(std::move(sort_units)),
          limit(limit),
          offset(offset) {
    }

    std::shared_ptr<Operator> child;
    std::vector<SortUnit> sort_units;
    std::optional<size_t> limit;
    std::optional<size_t> offset;
};

inline std::shared_ptr<ScanOperator> MakeScan() {
    return std::make_shared<ScanOperator>();
}

inline std::shared_ptr<CountTableOperator> MakeCountTable(std::string output_name = "count") {
    return std::make_shared<CountTableOperator>(std::move(output_name));
}

inline std::shared_ptr<AggregationOperator> MakeAggregation(
    std::shared_ptr<Operator> child, std::vector<ProjectionUnit> keys,
    std::vector<AggregationUnit> aggregations) {
    return std::make_shared<AggregationOperator>(std::move(child), std::move(keys),
                                                 std::move(aggregations));
}

inline std::shared_ptr<AggregationOperator> MakeGlobalAggregation(
    std::shared_ptr<Operator> child, std::vector<AggregationUnit> aggregations) {
    return MakeAggregation(std::move(child), {}, std::move(aggregations));
}

inline std::shared_ptr<FilterOperator> MakeFilter(std::shared_ptr<Operator> child,
                                                  std::shared_ptr<Expression> condition) {
    return std::make_shared<FilterOperator>(std::move(child), std::move(condition));
}

inline std::shared_ptr<ProjectOperator> MakeProject(std::shared_ptr<Operator> child,
                                                    std::vector<ProjectionUnit> projections) {
    return std::make_shared<ProjectOperator>(std::move(child), std::move(projections));
}

inline std::shared_ptr<AggregationOperator> MakeHashAggregation(
    std::shared_ptr<Operator> child, std::vector<ProjectionUnit> keys,
    std::vector<AggregationUnit> aggregations) {
    return MakeAggregation(std::move(child), std::move(keys), std::move(aggregations));
}

inline std::shared_ptr<AggregationOperator> MakeHashAggregation(
    std::shared_ptr<Operator> child, std::shared_ptr<Expression> key, std::string key_name,
    std::vector<AggregationUnit> aggregations) {
    std::vector<ProjectionUnit> keys;
    keys.push_back(ProjectionUnit{std::move(key), std::move(key_name)});
    return MakeAggregation(std::move(child), std::move(keys), std::move(aggregations));
}

inline std::shared_ptr<TopNOperator> MakeTopN(std::shared_ptr<Operator> child,
                                              std::vector<SortUnit> sort_units,
                                              std::optional<size_t> limit = std::nullopt,
                                              std::optional<size_t> offset = std::nullopt) {
    return std::make_shared<TopNOperator>(std::move(child), std::move(sort_units), limit, offset);
}

std::vector<core::Batch> Execute(bruh::BruhBatchReader& reader, std::shared_ptr<Operator> op);
}  // namespace columnar::exec

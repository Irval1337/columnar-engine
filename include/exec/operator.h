#pragma once

#include <bruh/bruh_batch_reader.h>
#include <core/batch.h>
#include <core/schema.h>
#include <exec/aggregation.h>
#include <exec/expression.h>

#include <memory>
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
    void Consume(core::Batch batch) override {
        batches_.push_back(std::move(batch));
    }

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
    GlobalAggregation,
    Filter,
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

struct GlobalAggregationOperator final : public Operator {
    GlobalAggregationOperator(std::shared_ptr<Operator> child,
                              std::vector<AggregationUnit> aggregations)
        : Operator(OperatorType::GlobalAggregation),
          child(std::move(child)),
          aggregations(std::move(aggregations)) {
    }

    std::shared_ptr<Operator> child;
    std::vector<AggregationUnit> aggregations;
};

struct FilterOperator final : public Operator {
    FilterOperator(std::shared_ptr<Operator> child, std::shared_ptr<Expression> condition)
        : Operator(OperatorType::Filter), child(std::move(child)), condition(std::move(condition)) {
    }

    std::shared_ptr<Operator> child;
    std::shared_ptr<Expression> condition;
};

inline std::shared_ptr<ScanOperator> MakeScan() {
    return std::make_shared<ScanOperator>();
}

inline std::shared_ptr<CountTableOperator> MakeCountTable(std::string output_name = "count") {
    return std::make_shared<CountTableOperator>(std::move(output_name));
}

inline std::shared_ptr<GlobalAggregationOperator> MakeGlobalAggregation(
    std::shared_ptr<Operator> child, std::vector<AggregationUnit> aggregations) {
    return std::make_shared<GlobalAggregationOperator>(std::move(child), std::move(aggregations));
}

inline std::shared_ptr<FilterOperator> MakeFilter(std::shared_ptr<Operator> child,
                                                  std::shared_ptr<Expression> condition) {
    return std::make_shared<FilterOperator>(std::move(child), std::move(condition));
}

std::vector<core::Batch> Execute(bruh::BruhBatchReader& reader, std::shared_ptr<Operator> op);
}  // namespace columnar::exec

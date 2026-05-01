#pragma once

#include <core/batch.h>
#include <core/schema.h>
#include <exec/operator.h>

#include <cstddef>
#include <istream>
#include <memory>
#include <utility>
#include <vector>

namespace columnar::exec {
class QueryMaker {
public:
    explicit QueryMaker(core::Schema table_schema) : table_schema_(std::move(table_schema)) {
    }

    std::shared_ptr<Operator> Make(size_t query_id) const;

private:
    std::shared_ptr<Operator> MakeQ0() const;
    std::shared_ptr<Operator> MakeQ1() const;
    std::shared_ptr<Operator> MakeQ2() const;
    std::shared_ptr<Operator> MakeQ3() const;
    std::shared_ptr<Operator> MakeQ4() const;
    std::shared_ptr<Operator> MakeQ5() const;
    std::shared_ptr<Operator> MakeQ6() const;

    core::Schema table_schema_;
};

std::vector<core::Batch> ExecuteClickBenchQuery(std::istream& is, size_t query_id);
}  // namespace columnar::exec

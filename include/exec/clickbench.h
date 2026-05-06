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
    std::shared_ptr<Operator> MakeQ7() const;
    std::shared_ptr<Operator> MakeQ8() const;
    std::shared_ptr<Operator> MakeQ9() const;
    std::shared_ptr<Operator> MakeQ10() const;
    // TODO: MakeQ11
    std::shared_ptr<Operator> MakeQ12() const;
    std::shared_ptr<Operator> MakeQ13() const;
    // TODO: MakeQ14
    std::shared_ptr<Operator> MakeQ15() const;
    // TODO: MakeQ16-18
    std::shared_ptr<Operator> MakeQ19() const;
    std::shared_ptr<Operator> MakeQ20() const;
    std::shared_ptr<Operator> MakeQ21() const;
    std::shared_ptr<Operator> MakeQ22() const;
    // TODO: MakeQ23
    std::shared_ptr<Operator> MakeQ24() const;
    std::shared_ptr<Operator> MakeQ25() const;
    std::shared_ptr<Operator> MakeQ26() const;
    // TODO: MakeQ27-32
    std::shared_ptr<Operator> MakeQ33() const;
    // TODO: MakeQ34-35
    std::shared_ptr<Operator> MakeQ36() const;
    std::shared_ptr<Operator> MakeQ37() const;
    // TODO: MakeQ38-42

    core::Schema table_schema_;
};

std::vector<core::Batch> ExecuteClickBenchQuery(std::istream& is, size_t query_id);
}  // namespace columnar::exec

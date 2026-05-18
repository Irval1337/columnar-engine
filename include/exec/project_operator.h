#pragma once

#include <core/schema.h>
#include <exec/operator.h>

#include <utility>
#include <vector>

namespace columnar::exec {
class ProjectSink final : public IOperator {
public:
    ProjectSink(IOperator& downstream, std::vector<ProjectionUnit> projections);

    void Consume(core::Batch batch) override;

    void Finalize() override {
        downstream_.Finalize();
    }

private:
    IOperator& downstream_;
    std::vector<ProjectionUnit> projections_;
    bool needs_dense_;
};
}  // namespace columnar::exec

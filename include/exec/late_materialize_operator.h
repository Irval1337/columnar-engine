#pragma once

#include <bruh/bruh_batch_reader.h>
#include <exec/operator.h>
#include <exec/topn_common.h>

#include <cstdint>
#include <optional>
#include <vector>

namespace columnar::exec {
class LateMaterializeSink final : public IOperator {
public:
    LateMaterializeSink(IOperator& downstream, bruh::BruhBatchReader& reader,
                        std::vector<ProjectionUnit> projections);

    void Consume(core::Batch batch) override;

    void Finalize() override;

private:
    IOperator& downstream_;
    bruh::BruhBatchReader& reader_;
    std::vector<ProjectionUnit> projections_;
    std::vector<int64_t> row_ids_;
};

class TopNRowIdSink final : public IOperator {
public:
    TopNRowIdSink(IOperator& downstream, std::vector<SortUnit> sort_units,
                  std::optional<size_t> limit, std::optional<size_t> offset);

    void Consume(core::Batch batch) override;

    void Finalize() override;

private:
    struct RowIdRef {
        int64_t row_id = 0;
        std::vector<SortValue> sort_values;
    };

    void Init(const core::Batch& batch);

    bool Less(const RowIdRef& lhs, const RowIdRef& rhs) const;

    IOperator& downstream_;
    std::vector<SortUnit> sort_units_;
    std::optional<size_t> limit_;
    std::optional<size_t> offset_;
    size_t prefix_ = 0;
    bool initialized_ = false;
    size_t row_id_index_ = 0;
    std::vector<size_t> sort_indexes_;
    std::vector<RowIdRef> refs_;
};
}  // namespace columnar::exec

#include <exec/topn_operator.h>

#include <core/columns/string_column.h>
#include <core/datatype.h>
#include <core/schema.h>
#include <exec/column_helpers.h>
#include <exec/expression.h>
#include <exec/kernel.h>
#include <util/macro.h>

#include <algorithm>
#include <utility>
#include <vector>

namespace columnar::exec {
namespace {
struct RowRef {
    uint32_t batch_idx;
    uint32_t row_idx;
};

bool RequiresDenseBatch(const std::vector<SortUnit>& sort_units) {
    for (auto& unit : sort_units) {
        if (!IsTrivialExpression(*unit.expression)) {
            return true;
        }
    }
    return false;
}

int CompareRowRefs(const core::Column& col_a, size_t row_a, const core::Column& col_b,
                   size_t row_b) {
    bool a_null = col_a.IsNull(row_a);
    bool b_null = col_b.IsNull(row_b);
    if (a_null || b_null) {
        if (a_null && b_null) {
            return 0;
        }
        return a_null ? -1 : 1;
    }
    auto type = col_a.GetDataType();
    if (type == core::DataType::String) {
        auto av = ReadStringRow(col_a, row_a);
        auto bv = ReadStringRow(col_b, row_b);
        if (av < bv) {
            return -1;
        }
        if (av > bv) {
            return 1;
        }
        return 0;
    }
    if (type == core::DataType::Double) {
        auto av = ReadDoubleRow(col_a, row_a);
        auto bv = ReadDoubleRow(col_b, row_b);
        if (av < bv) {
            return -1;
        }
        if (av > bv) {
            return 1;
        }
        return 0;
    }
    auto av = ReadIntegerRow(col_a, row_a);
    auto bv = ReadIntegerRow(col_b, row_b);
    if (av < bv) {
        return -1;
    }
    if (av > bv) {
        return 1;
    }
    return 0;
}
}  // namespace

TopNSink::TopNSink(IOperator& downstream, std::vector<SortUnit> sort_units,
                   std::optional<size_t> limit, std::optional<size_t> offset)
    : downstream_(downstream),
      sort_units_(std::move(sort_units)),
      limit_(limit),
      offset_(offset),
      needs_dense_(RequiresDenseBatch(sort_units_)) {
}

void TopNSink::Consume(core::Batch batch) {
    if (batch.SelectedRowsCount() == 0) {
        return;
    }
    if (batch.HasSelection() && needs_dense_) {
        batch = kernel::Materialize(batch);
    }
    buffer_.push_back(std::move(batch));
}

void TopNSink::Finalize() {
    if (buffer_.empty()) {
        downstream_.Finalize();
        return;
    }
    size_t total_rows = 0;
    for (auto& b : buffer_) {
        total_rows += b.SelectedRowsCount();
    }

    auto for_each_input_row = [&](auto&& emit) {
        for (size_t b = 0; b < buffer_.size(); ++b) {
            const auto& batch = buffer_[b];
            if (batch.HasSelection()) {
                for (uint32_t r : batch.Selection()) {
                    emit(static_cast<uint32_t>(b), r);
                }
            } else {
                size_t rows = batch.RowsCount();
                for (size_t r = 0; r < rows; ++r) {
                    emit(static_cast<uint32_t>(b), static_cast<uint32_t>(r));
                }
            }
        }
    };

    std::vector<std::vector<EvalResult>> sort_evals(sort_units_.size());
    std::vector<std::vector<const core::Column*>> sort_cols(sort_units_.size());
    for (size_t s = 0; s < sort_units_.size(); ++s) {
        sort_evals[s].reserve(buffer_.size());
        sort_cols[s].reserve(buffer_.size());
        for (auto& batch : buffer_) {
            sort_evals[s].emplace_back(Evaluate(batch, *sort_units_[s].expression));
            sort_cols[s].push_back(&sort_evals[s].back().Get());
        }
    }

    auto less = [&](const RowRef& a, const RowRef& b) {
        for (size_t s = 0; s < sort_units_.size(); ++s) {
            int cmp = CompareRowRefs(*sort_cols[s][a.batch_idx], a.row_idx,
                                     *sort_cols[s][b.batch_idx], b.row_idx);
            if (cmp == 0) {
                continue;
            }
            return sort_units_[s].ascending ? cmp < 0 : cmp > 0;
        }
        if (a.batch_idx != b.batch_idx) {
            return a.batch_idx < b.batch_idx;
        }
        return a.row_idx < b.row_idx;
    };

    size_t offset = offset_.value_or(0);

    size_t prefix = total_rows;
    if (limit_ && offset < total_rows) {
        prefix = offset + std::min(*limit_, total_rows - offset);
    }

    std::vector<RowRef> refs;
    if (limit_ && prefix < total_rows) {
        refs.reserve(prefix);
        if (prefix > 0) {
            for_each_input_row([&](uint32_t b, uint32_t r) {
                RowRef ref{b, r};
                if (refs.size() < prefix) {
                    refs.push_back(ref);
                    std::push_heap(refs.begin(), refs.end(), less);
                } else if (less(ref, refs.front())) {
                    std::pop_heap(refs.begin(), refs.end(), less);
                    refs.back() = ref;
                    std::push_heap(refs.begin(), refs.end(), less);
                }
            });
            std::sort_heap(refs.begin(), refs.end(), less);
        }
    } else {
        refs.reserve(total_rows);
        for_each_input_row([&](uint32_t b, uint32_t r) { refs.push_back({b, r}); });
        std::sort(refs.begin(), refs.end(), less);
    }

    if (offset >= refs.size()) {
        refs.clear();
    } else if (offset > 0) {
        refs.erase(refs.begin(), refs.begin() + offset);
    }

    core::Batch out(buffer_.front().GetSchema(), refs.size());
    size_t cols = buffer_.front().ColumnsCount();
    for (size_t c = 0; c < cols; ++c) {
        auto& dst = out.ColumnAt(c);
        for (auto& ref : refs) {
            AppendRow(dst, buffer_[ref.batch_idx].ColumnAt(c), ref.row_idx);
        }
    }

    downstream_.Consume(std::move(out));
    downstream_.Finalize();
}
}  // namespace columnar::exec

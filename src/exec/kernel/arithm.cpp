#include <core/columns/bool_column.h>
#include <core/columns/numeric_column.h>
#include <exec/column_dispatch.h>
#include <exec/kernel.h>
#include <exec/kernel/internal.h>
#include <util/bit_vector.h>
#include <util/macro.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace columnar::exec::kernel {
namespace {
template <typename Op>
std::unique_ptr<core::Column> ArithmeticIntImpl(const core::Column& lhs, const core::Column& rhs,
                                                Op op) {
    size_t rows = lhs.Size();
    if (rhs.Size() != rows) {
        THROW_RUNTIME_ERROR("Arithmetic: row count mismatch");
    }
    bool nullable = lhs.IsNullable() || rhs.IsNullable();
    std::vector<int64_t> data(rows);
    util::BitVector mask = nullable ? util::BitVector(rows) : util::BitVector();
    VisitIntegerCol(lhs, [&](const auto& l) {
        VisitIntegerCol(rhs, [&](const auto& r) {
            const util::BitVector* lmask = l.IsNullable() ? &l.GetNullMask() : nullptr;
            const util::BitVector* rmask = r.IsNullable() ? &r.GetNullMask() : nullptr;
            if (lmask == nullptr && rmask == nullptr) {
                for (size_t i = 0; i < rows; ++i) {
                    data[i] = op(static_cast<int64_t>(ReadTypedValue(l, i)),
                                 static_cast<int64_t>(ReadTypedValue(r, i)));
                }
                return;
            }
            for (size_t i = 0; i < rows; ++i) {
                if ((lmask != nullptr && lmask->Get(i)) || (rmask != nullptr && rmask->Get(i))) {
                    mask.Set(i);
                    continue;
                }
                data[i] = op(static_cast<int64_t>(ReadTypedValue(l, i)),
                             static_cast<int64_t>(ReadTypedValue(r, i)));
            }
        });
    });
    return std::make_unique<core::Int64Column>(std::move(data), std::move(mask), nullable);
}

template <typename Op>
std::unique_ptr<core::Column> BoolBinaryImpl(const core::Column& lhs, const core::Column& rhs,
                                             const char* name, Op op) {
    if (lhs.GetDataType() != core::DataType::Bool || rhs.GetDataType() != core::DataType::Bool) {
        THROW_RUNTIME_ERROR(std::string(name) + " operands must be boolean");
    }
    auto& l = static_cast<const core::BoolColumn&>(lhs);
    auto& r = static_cast<const core::BoolColumn&>(rhs);
    size_t rows = l.Size();
    if (r.Size() != rows) {
        THROW_RUNTIME_ERROR(std::string(name) + ": row count mismatch");
    }
    const auto& ld = l.GetData();
    const auto& rd = r.GetData();
    auto out = MakeBoolColumn(rows);
    if (!l.IsNullable() && !r.IsNullable()) {
        for (size_t i = 0; i < rows; ++i) {
            out->Append(op(ld.Get(i), rd.Get(i)));
        }
        return out;
    }
    const util::BitVector* lmask = l.IsNullable() ? &l.GetNullMask() : nullptr;
    const util::BitVector* rmask = r.IsNullable() ? &r.GetNullMask() : nullptr;
    for (size_t i = 0; i < rows; ++i) {
        bool lv = ld.Get(i) && !(lmask && lmask->Get(i));
        bool rv = rd.Get(i) && !(rmask && rmask->Get(i));
        out->Append(op(lv, rv));
    }
    return out;
}
}  // namespace

std::unique_ptr<core::Column> And(const core::Column& lhs, const core::Column& rhs) {
    return BoolBinaryImpl(lhs, rhs, "AND", [](bool a, bool b) { return a && b; });
}

std::unique_ptr<core::Column> Or(const core::Column& lhs, const core::Column& rhs) {
    return BoolBinaryImpl(lhs, rhs, "OR", [](bool a, bool b) { return a || b; });
}

std::unique_ptr<core::Column> Add(const core::Column& lhs, const core::Column& rhs) {
    return ArithmeticIntImpl(lhs, rhs, [](int64_t a, int64_t b) { return a + b; });
}

std::unique_ptr<core::Column> Subtract(const core::Column& lhs, const core::Column& rhs) {
    return ArithmeticIntImpl(lhs, rhs, [](int64_t a, int64_t b) { return a - b; });
}

std::unique_ptr<core::Column> Multiply(const core::Column& lhs, const core::Column& rhs) {
    return ArithmeticIntImpl(lhs, rhs, [](int64_t a, int64_t b) { return a * b; });
}
}  // namespace columnar::exec::kernel

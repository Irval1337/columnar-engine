#include <core/columns/dictionary_string_column.h>
#include <core/columns/string_column.h>
#include <exec/column_dispatch.h>
#include <exec/column_row_access.h>
#include <exec/kernel.h>
#include <exec/kernel/internal.h>
#include <util/macro.h>

#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

namespace columnar::exec::kernel {
namespace {
template <typename L, typename R, typename Cmp>
std::unique_ptr<core::Column> ComparePair(const L& lhs, const R& rhs, Cmp&& cmp) {
    size_t rows = TypedColumnSize(lhs);
    if (TypedColumnSize(rhs) != rows) {
        THROW_RUNTIME_ERROR("Compare: row count mismatch");
    }
    auto out = MakeBoolColumn(rows);
    if (!lhs.IsNullable() && !rhs.IsNullable()) {
        for (size_t i = 0; i < rows; ++i) {
            out->Append(cmp(ReadTypedValue(lhs, i), ReadTypedValue(rhs, i)));
        }
        return out;
    }
    const util::BitVector* lmask = lhs.IsNullable() ? &lhs.GetNullMask() : nullptr;
    const util::BitVector* rmask = rhs.IsNullable() ? &rhs.GetNullMask() : nullptr;
    for (size_t i = 0; i < rows; ++i) {
        bool null = (lmask && lmask->Get(i)) || (rmask && rmask->Get(i));
        out->Append(!null && cmp(ReadTypedValue(lhs, i), ReadTypedValue(rhs, i)));
    }
    return out;
}

template <typename Cmp>
std::unique_ptr<core::Column> CompareIntegers(const core::Column& lhs, const core::Column& rhs,
                                              Cmp cmp) {
    return VisitIntegerCol(lhs, [&](const auto& l) {
        return VisitIntegerCol(rhs, [&](const auto& r) {
            return ComparePair(l, r, [&](auto a, auto b) {
                return cmp(static_cast<int64_t>(a), static_cast<int64_t>(b));
            });
        });
    });
}

template <typename Cmp>
std::unique_ptr<core::Column> CompareDoubles(const core::Column& lhs, const core::Column& rhs,
                                             Cmp cmp) {
    return VisitNumericCol(lhs, [&](const auto& l) {
        return VisitNumericCol(rhs, [&](const auto& r) {
            return ComparePair(l, r, [&](auto a, auto b) {
                return cmp(static_cast<double>(a), static_cast<double>(b));
            });
        });
    });
}

template <typename Cmp>
std::unique_ptr<core::Column> CompareStrings(const core::Column& lhs, const core::Column& rhs,
                                             Cmp cmp) {
    if (lhs.GetDataType() != core::DataType::String ||
        rhs.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("Compare: expected string columns on both sides");
    }
    size_t rows = lhs.Size();
    if (rhs.Size() != rows) {
        THROW_RUNTIME_ERROR("Compare: row count mismatch");
    }
    auto out = MakeBoolColumn(rows);
    for (size_t i = 0; i < rows; ++i) {
        if (lhs.IsNull(i) || rhs.IsNull(i)) {
            out->Append(false);
        } else {
            out->Append(cmp(ReadStringRow(lhs, i), ReadStringRow(rhs, i)));
        }
    }
    return out;
}

template <typename Cmp>
std::unique_ptr<core::Column> CompareDispatch(const core::Column& lhs, const core::Column& rhs,
                                              Cmp cmp) {
    if (lhs.GetDataType() == core::DataType::String ||
        rhs.GetDataType() == core::DataType::String) {
        return CompareStrings(lhs, rhs, cmp);
    }
    if (lhs.GetDataType() == core::DataType::Double ||
        rhs.GetDataType() == core::DataType::Double) {
        return CompareDoubles(lhs, rhs, cmp);
    }
    return CompareIntegers(lhs, rhs, cmp);
}

template <typename Cmp>
std::unique_ptr<core::Column> CompareIntConst(const core::Column& col, int64_t value, Cmp cmp) {
    return VisitIntegerCol(col, [&](const auto& typed) -> std::unique_ptr<core::Column> {
        size_t rows = TypedColumnSize(typed);
        auto out = MakeBoolColumn(rows);
        if (!typed.IsNullable()) {
            for (size_t i = 0; i < rows; ++i) {
                out->Append(cmp(static_cast<int64_t>(ReadTypedValue(typed, i)), value));
            }
            return out;
        }
        const auto& mask = typed.GetNullMask();
        for (size_t i = 0; i < rows; ++i) {
            out->Append(!mask.Get(i) && cmp(static_cast<int64_t>(ReadTypedValue(typed, i)), value));
        }
        return out;
    });
}

template <typename Cmp>
std::unique_ptr<core::Column> CompareStringConst(const core::Column& col, std::string_view value,
                                                 Cmp cmp) {
    if (col.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("CompareStringConst: expected string column");
    }
    if (auto* dict = core::AsDictionaryString(col)) {
        std::vector<uint8_t> dict_results(dict->DictSize(), 0);
        for (uint32_t id = 0; id < dict_results.size(); ++id) {
            dict_results[id] = cmp(dict->DictValue(id), value) ? 1 : 0;
        }
        size_t rows = dict->Size();
        auto out = MakeBoolColumn(rows);
        for (size_t i = 0; i < rows; ++i) {
            out->Append(!dict->IsNull(i) && dict_results[dict->GetId(i)] != 0);
        }
        return out;
    }
    auto& s = static_cast<const core::StringColumn&>(col);
    size_t rows = s.Size();
    auto out = MakeBoolColumn(rows);
    if (!s.IsNullable()) {
        for (size_t i = 0; i < rows; ++i) {
            out->Append(cmp(s.Get(i), value));
        }
        return out;
    }
    for (size_t i = 0; i < rows; ++i) {
        out->Append(!s.IsNull(i) && cmp(s.Get(i), value));
    }
    return out;
}
}  // namespace

std::unique_ptr<core::Column> Equal(const core::Column& lhs, const core::Column& rhs) {
    return CompareDispatch(lhs, rhs, [](auto a, auto b) { return a == b; });
}

std::unique_ptr<core::Column> NotEqual(const core::Column& lhs, const core::Column& rhs) {
    return CompareDispatch(lhs, rhs, [](auto a, auto b) { return a != b; });
}

std::unique_ptr<core::Column> Less(const core::Column& lhs, const core::Column& rhs) {
    return CompareDispatch(lhs, rhs, [](auto a, auto b) { return a < b; });
}

std::unique_ptr<core::Column> LessOrEqual(const core::Column& lhs, const core::Column& rhs) {
    return CompareDispatch(lhs, rhs, [](auto a, auto b) { return a <= b; });
}

std::unique_ptr<core::Column> Greater(const core::Column& lhs, const core::Column& rhs) {
    return CompareDispatch(lhs, rhs, [](auto a, auto b) { return a > b; });
}

std::unique_ptr<core::Column> GreaterOrEqual(const core::Column& lhs, const core::Column& rhs) {
    return CompareDispatch(lhs, rhs, [](auto a, auto b) { return a >= b; });
}

std::unique_ptr<core::Column> EqualConstInt(const core::Column& col, int64_t value) {
    return CompareIntConst(col, value, [](int64_t a, int64_t b) { return a == b; });
}

std::unique_ptr<core::Column> NotEqualConstInt(const core::Column& col, int64_t value) {
    return CompareIntConst(col, value, [](int64_t a, int64_t b) { return a != b; });
}

std::unique_ptr<core::Column> LessConstInt(const core::Column& col, int64_t value) {
    return CompareIntConst(col, value, [](int64_t a, int64_t b) { return a < b; });
}

std::unique_ptr<core::Column> LessOrEqualConstInt(const core::Column& col, int64_t value) {
    return CompareIntConst(col, value, [](int64_t a, int64_t b) { return a <= b; });
}

std::unique_ptr<core::Column> GreaterConstInt(const core::Column& col, int64_t value) {
    return CompareIntConst(col, value, [](int64_t a, int64_t b) { return a > b; });
}

std::unique_ptr<core::Column> GreaterOrEqualConstInt(const core::Column& col, int64_t value) {
    return CompareIntConst(col, value, [](int64_t a, int64_t b) { return a >= b; });
}

std::unique_ptr<core::Column> EqualConstString(const core::Column& col, std::string_view value) {
    return CompareStringConst(col, value,
                              [](std::string_view a, std::string_view b) { return a == b; });
}

std::unique_ptr<core::Column> NotEqualConstString(const core::Column& col, std::string_view value) {
    return CompareStringConst(col, value,
                              [](std::string_view a, std::string_view b) { return a != b; });
}

std::unique_ptr<core::Column> LessConstString(const core::Column& col, std::string_view value) {
    return CompareStringConst(col, value,
                              [](std::string_view a, std::string_view b) { return a < b; });
}

std::unique_ptr<core::Column> LessOrEqualConstString(const core::Column& col,
                                                     std::string_view value) {
    return CompareStringConst(col, value,
                              [](std::string_view a, std::string_view b) { return a <= b; });
}

std::unique_ptr<core::Column> GreaterConstString(const core::Column& col, std::string_view value) {
    return CompareStringConst(col, value,
                              [](std::string_view a, std::string_view b) { return a > b; });
}

std::unique_ptr<core::Column> GreaterOrEqualConstString(const core::Column& col,
                                                        std::string_view value) {
    return CompareStringConst(col, value,
                              [](std::string_view a, std::string_view b) { return a >= b; });
}
}  // namespace columnar::exec::kernel

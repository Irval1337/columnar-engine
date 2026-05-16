#include <exec/kernel.h>

#include <core/column_factory.h>
#include <core/columns/bool_column.h>
#include <core/columns/char_column.h>
#include <core/columns/date_column.h>
#include <core/columns/numeric_column.h>
#include <core/columns/string_column.h>
#include <core/columns/timestamp_column.h>
#include <exec/column_helpers.h>
#include <util/macro.h>

#include <regex>
#include <string>
#include <type_traits>
#include <utility>

namespace columnar::exec::kernel {
namespace {
using ::columnar::exec::VisitIntegerCol;

template <typename V>
decltype(auto) VisitNumericCol(const core::Column& col, V&& v) {
    if (col.GetDataType() == core::DataType::Double) {
        return v(static_cast<const core::DoubleColumn&>(col));
    }
    return VisitIntegerCol(col, std::forward<V>(v));
}

std::unique_ptr<core::BoolColumn> MakeBoolColumn(size_t rows) {
    auto out = std::make_unique<core::BoolColumn>(false);
    out->Reserve(rows);
    return out;
}

template <typename Col, typename F>
void ForEachNonNull(const Col& col, F&& f) {
    const auto& data = col.GetData();
    if (col.IsNullable()) {
        const auto& mask = col.GetNullMask();
        for (size_t i = 0; i < data.size(); ++i) {
            if (!mask.Get(i)) {
                f(data[i]);
            }
        }
    } else {
        for (size_t i = 0; i < data.size(); ++i) {
            f(data[i]);
        }
    }
}

template <typename F>
void ForEachNonNull(const core::BoolColumn& col, F&& f) {
    size_t rows = col.Size();
    if (col.IsNullable()) {
        const auto& mask = col.GetNullMask();
        for (size_t i = 0; i < rows; ++i) {
            if (!mask.Get(i)) {
                f(col.Get(i));
            }
        }
    } else {
        for (size_t i = 0; i < rows; ++i) {
            f(col.Get(i));
        }
    }
}

template <typename Acc, typename Col>
ScalarReduction<Acc> SumImpl(const Col& col) {
    ScalarReduction<Acc> r;
    ForEachNonNull(col, [&](auto v) {
        r.value += static_cast<Acc>(v);
        r.has_value = true;
    });
    return r;
}

template <typename Acc, typename Col, typename Better>
ScalarReduction<Acc> MinMaxImpl(const Col& col, Better&& better) {
    ScalarReduction<Acc> r;
    ForEachNonNull(col, [&](auto v) {
        Acc cv = static_cast<Acc>(v);
        if (!r.has_value || better(cv, r.value)) {
            r.value = cv;
            r.has_value = true;
        }
    });
    return r;
}

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
    auto& l = static_cast<const core::StringColumn&>(lhs);
    auto& r = static_cast<const core::StringColumn&>(rhs);
    size_t rows = l.Size();
    if (r.Size() != rows) {
        THROW_RUNTIME_ERROR("Compare: row count mismatch");
    }
    auto out = MakeBoolColumn(rows);
    for (size_t i = 0; i < rows; ++i) {
        if (l.IsNull(i) || r.IsNull(i)) {
            out->Append(false);
        } else {
            out->Append(cmp(l.Get(i), r.Get(i)));
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

std::unique_ptr<core::Column> ArithmeticInt(const core::Column& lhs, const core::Column& rhs,
                                            bool subtract) {
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
                if (subtract) {
                    for (size_t i = 0; i < rows; ++i) {
                        data[i] = static_cast<int64_t>(ReadTypedValue(l, i)) -
                                  static_cast<int64_t>(ReadTypedValue(r, i));
                    }
                } else {
                    for (size_t i = 0; i < rows; ++i) {
                        data[i] = static_cast<int64_t>(ReadTypedValue(l, i)) +
                                  static_cast<int64_t>(ReadTypedValue(r, i));
                    }
                }
                return;
            }
            for (size_t i = 0; i < rows; ++i) {
                if ((lmask != nullptr && lmask->Get(i)) || (rmask != nullptr && rmask->Get(i))) {
                    mask.Set(i);
                    continue;
                }
                auto a = static_cast<int64_t>(ReadTypedValue(l, i));
                auto b = static_cast<int64_t>(ReadTypedValue(r, i));
                data[i] = subtract ? a - b : a + b;
            }
        });
    });
    return std::make_unique<core::Int64Column>(std::move(data), std::move(mask), nullable);
}

template <typename Col>
void FilterNumeric(const Col& src, const core::BoolColumn& mask, Col& dst) {
    const auto& data = src.GetData();
    const auto& m = mask.GetData();
    if (src.IsNullable()) {
        const auto& null_mask = src.GetNullMask();
        for (size_t i = 0; i < data.size(); ++i) {
            if (!m.Get(i)) {
                continue;
            }
            if (null_mask.Get(i)) {
                dst.AppendNull();
            } else {
                dst.Append(data[i]);
            }
        }
    } else {
        for (size_t i = 0; i < data.size(); ++i) {
            if (m.Get(i)) {
                dst.Append(data[i]);
            }
        }
    }
}

void FilterString(const core::StringColumn& src, const core::BoolColumn& mask,
                  core::StringColumn& dst) {
    const auto& m = mask.GetData();
    size_t rows = src.Size();
    for (size_t i = 0; i < rows; ++i) {
        if (!m.Get(i)) {
            continue;
        }
        if (src.IsNull(i)) {
            dst.AppendNull();
        } else {
            dst.Append(src.Get(i));
        }
    }
}

void FilterBool(const core::BoolColumn& src, const core::BoolColumn& mask, core::BoolColumn& dst) {
    const auto& m = mask.GetData();
    size_t rows = src.Size();
    for (size_t i = 0; i < rows; ++i) {
        if (!m.Get(i)) {
            continue;
        }
        if (src.IsNull(i)) {
            dst.AppendNull();
        } else {
            dst.Append(src.Get(i));
        }
    }
}

void FilterColumn(const core::Column& src, const core::BoolColumn& mask, core::Column& dst) {
    switch (src.GetDataType()) {
        case core::DataType::Int16:
            return FilterNumeric(static_cast<const core::Int16Column&>(src), mask,
                                 static_cast<core::Int16Column&>(dst));
        case core::DataType::Int32:
            return FilterNumeric(static_cast<const core::Int32Column&>(src), mask,
                                 static_cast<core::Int32Column&>(dst));
        case core::DataType::Int64:
            return FilterNumeric(static_cast<const core::Int64Column&>(src), mask,
                                 static_cast<core::Int64Column&>(dst));
        case core::DataType::Double:
            return FilterNumeric(static_cast<const core::DoubleColumn&>(src), mask,
                                 static_cast<core::DoubleColumn&>(dst));
        case core::DataType::Date:
            return FilterNumeric(static_cast<const core::DateColumn&>(src), mask,
                                 static_cast<core::DateColumn&>(dst));
        case core::DataType::Timestamp:
            return FilterNumeric(static_cast<const core::TimestampColumn&>(src), mask,
                                 static_cast<core::TimestampColumn&>(dst));
        case core::DataType::Char:
            return FilterNumeric(static_cast<const core::CharColumn&>(src), mask,
                                 static_cast<core::CharColumn&>(dst));
        case core::DataType::String:
            return FilterString(static_cast<const core::StringColumn&>(src), mask,
                                static_cast<core::StringColumn&>(dst));
        case core::DataType::Bool:
            return FilterBool(static_cast<const core::BoolColumn&>(src), mask,
                              static_cast<core::BoolColumn&>(dst));
    }
    THROW_RUNTIME_ERROR("Unsupported column type for filter");
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

std::unique_ptr<core::Column> ConstInt64(int64_t value, size_t rows) {
    auto out = std::make_unique<core::Int64Column>(false);
    out->Reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        out->Append(value);
    }
    return out;
}

std::unique_ptr<core::Column> ConstString(std::string_view value, size_t rows) {
    auto out = std::make_unique<core::StringColumn>(false);
    out->Reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        out->Append(value);
    }
    return out;
}

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

std::unique_ptr<core::Column> And(const core::Column& lhs, const core::Column& rhs) {
    if (lhs.GetDataType() != core::DataType::Bool || rhs.GetDataType() != core::DataType::Bool) {
        THROW_RUNTIME_ERROR("AND operands must be boolean");
    }
    auto& l = static_cast<const core::BoolColumn&>(lhs);
    auto& r = static_cast<const core::BoolColumn&>(rhs);
    size_t rows = l.Size();
    if (r.Size() != rows) {
        THROW_RUNTIME_ERROR("AND: row count mismatch");
    }
    const auto& ld = l.GetData();
    const auto& rd = r.GetData();
    auto out = MakeBoolColumn(rows);
    if (!l.IsNullable() && !r.IsNullable()) {
        for (size_t i = 0; i < rows; ++i) {
            out->Append(ld.Get(i) && rd.Get(i));
        }
        return out;
    }
    const util::BitVector* lmask = l.IsNullable() ? &l.GetNullMask() : nullptr;
    const util::BitVector* rmask = r.IsNullable() ? &r.GetNullMask() : nullptr;
    for (size_t i = 0; i < rows; ++i) {
        bool lv = ld.Get(i) && !(lmask && lmask->Get(i));
        bool rv = rd.Get(i) && !(rmask && rmask->Get(i));
        out->Append(lv && rv);
    }
    return out;
}

std::unique_ptr<core::Column> Or(const core::Column& lhs, const core::Column& rhs) {
    if (lhs.GetDataType() != core::DataType::Bool || rhs.GetDataType() != core::DataType::Bool) {
        THROW_RUNTIME_ERROR("OR operands must be boolean");
    }
    auto& l = static_cast<const core::BoolColumn&>(lhs);
    auto& r = static_cast<const core::BoolColumn&>(rhs);
    size_t rows = l.Size();
    if (r.Size() != rows) {
        THROW_RUNTIME_ERROR("OR: row count mismatch");
    }
    const auto& ld = l.GetData();
    const auto& rd = r.GetData();
    auto out = MakeBoolColumn(rows);
    if (!l.IsNullable() && !r.IsNullable()) {
        for (size_t i = 0; i < rows; ++i) {
            out->Append(ld.Get(i) || rd.Get(i));
        }
        return out;
    }
    const util::BitVector* lmask = l.IsNullable() ? &l.GetNullMask() : nullptr;
    const util::BitVector* rmask = r.IsNullable() ? &r.GetNullMask() : nullptr;
    for (size_t i = 0; i < rows; ++i) {
        bool lv = ld.Get(i) && !(lmask && lmask->Get(i));
        bool rv = rd.Get(i) && !(rmask && rmask->Get(i));
        out->Append(lv || rv);
    }
    return out;
}

std::unique_ptr<core::Column> Add(const core::Column& lhs, const core::Column& rhs) {
    return ArithmeticInt(lhs, rhs, false);
}

std::unique_ptr<core::Column> Subtract(const core::Column& lhs, const core::Column& rhs) {
    return ArithmeticInt(lhs, rhs, true);
}

std::unique_ptr<core::Column> StrContains(const core::Column& operand, std::string_view substring,
                                          bool negated) {
    if (operand.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("StrContains operand must be a string column");
    }
    auto& s = static_cast<const core::StringColumn&>(operand);
    size_t rows = s.Size();
    auto out = MakeBoolColumn(rows);
    for (size_t i = 0; i < rows; ++i) {
        if (s.IsNull(i)) {
            out->Append(false);
            continue;
        }
        bool found = s.Get(i).find(substring) != std::string_view::npos;
        out->Append(negated ? !found : found);
    }
    return out;
}

std::unique_ptr<core::Column> StrLength(const core::Column& operand) {
    if (operand.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("length() operand must be a string column");
    }
    auto& s = static_cast<const core::StringColumn&>(operand);
    size_t rows = s.Size();
    auto out = std::make_unique<core::Int64Column>(s.IsNullable());
    out->Reserve(rows);
    for (size_t i = 0; i < rows; ++i) {
        if (s.IsNull(i)) {
            out->AppendNull();
        } else {
            out->Append(static_cast<int64_t>(s.Get(i).size()));
        }
    }
    return out;
}

namespace {
template <typename Transform>
std::unique_ptr<core::Column> MapTimestamp(const core::Column& operand, bool to_timestamp,
                                           Transform&& transform) {
    if (operand.GetDataType() != core::DataType::Timestamp) {
        THROW_RUNTIME_ERROR("expected a timestamp column");
    }
    size_t rows = operand.Size();
    std::unique_ptr<core::Column> out =
        to_timestamp ? std::unique_ptr<core::Column>(
                           std::make_unique<core::TimestampColumn>(operand.IsNullable()))
                     : std::unique_ptr<core::Column>(
                           std::make_unique<core::Int64Column>(operand.IsNullable()));
    for (size_t i = 0; i < rows; ++i) {
        if (operand.IsNull(i)) {
            out->AppendNull();
        } else {
            AppendInteger(*out, transform(ReadIntegerRow(operand, i)));
        }
    }
    return out;
}
}  // namespace

std::unique_ptr<core::Column> ExtractMinute(const core::Column& operand) {
    return MapTimestamp(operand, /*to_timestamp=*/false,
                        [](int64_t seconds) { return (seconds / 60) % 60; });
}

std::unique_ptr<core::Column> TruncMinute(const core::Column& operand) {
    return MapTimestamp(operand, /*to_timestamp=*/true,
                        [](int64_t seconds) { return seconds - seconds % 60; });
}

std::unique_ptr<core::Column> RegexReplace(const core::Column& operand, const std::regex& regex,
                                           const std::string& replacement) {
    if (operand.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("REGEXP_REPLACE operand must be a string column");
    }
    auto& s = static_cast<const core::StringColumn&>(operand);
    size_t rows = s.Size();
    auto out = std::make_unique<core::StringColumn>(s.IsNullable());
    for (size_t i = 0; i < rows; ++i) {
        if (s.IsNull(i)) {
            out->AppendNull();
            continue;
        }
        auto value = s.Get(i);
        out->Append(std::regex_replace(std::string(value), regex, replacement));
    }
    return out;
}

std::unique_ptr<core::Column> CaseSelect(const core::BoolColumn& mask,
                                         const core::Column& when_true,
                                         const core::Column& when_false) {
    size_t rows = mask.Size();
    if (when_true.Size() != rows || when_false.Size() != rows) {
        THROW_RUNTIME_ERROR("CASE: row count mismatch");
    }
    auto out = core::MakeColumn(when_true.GetDataType(),
                                when_true.IsNullable() || when_false.IsNullable());
    for (size_t i = 0; i < rows; ++i) {
        bool take_true = !mask.IsNull(i) && mask.Get(i);
        AppendRow(*out, take_true ? when_true : when_false, i);
    }
    return out;
}

core::Batch ApplyFilter(const core::Batch& batch, const core::BoolColumn& mask) {
    size_t rows = batch.RowsCount();
    if (mask.Size() != rows) {
        THROW_RUNTIME_ERROR("ApplyFilter: mask size mismatch");
    }
    size_t selected = mask.GetData().PopCount();
    core::Batch out(batch.GetSchema(), selected);
    for (size_t col = 0; col < batch.ColumnsCount(); ++col) {
        FilterColumn(batch.ColumnAt(col), mask, out.ColumnAt(col));
    }
    return out;
}

ScalarReduction<int64_t> SumInt(const core::Column& col) {
    return VisitIntegerCol(col, [](const auto& typed) { return SumImpl<int64_t>(typed); });
}

ScalarReduction<long double> SumDouble(const core::Column& col) {
    return VisitNumericCol(col, [](const auto& typed) { return SumImpl<long double>(typed); });
}

ScalarReduction<int64_t> MinInt(const core::Column& col) {
    return VisitIntegerCol(col, [](const auto& typed) {
        return MinMaxImpl<int64_t>(typed, [](int64_t a, int64_t b) { return a < b; });
    });
}

ScalarReduction<int64_t> MaxInt(const core::Column& col) {
    return VisitIntegerCol(col, [](const auto& typed) {
        return MinMaxImpl<int64_t>(typed, [](int64_t a, int64_t b) { return a > b; });
    });
}

ScalarReduction<double> MinDouble(const core::Column& col) {
    return VisitNumericCol(col, [](const auto& typed) {
        return MinMaxImpl<double>(typed, [](double a, double b) { return a < b; });
    });
}

ScalarReduction<double> MaxDouble(const core::Column& col) {
    return VisitNumericCol(col, [](const auto& typed) {
        return MinMaxImpl<double>(typed, [](double a, double b) { return a > b; });
    });
}

namespace {
template <typename Better>
ScalarReduction<std::string> MinMaxStringImpl(const core::Column& col, Better better) {
    if (col.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("MIN/MAX string: not a string column");
    }
    auto& s = static_cast<const core::StringColumn&>(col);
    ScalarReduction<std::string> r;
    size_t rows = s.Size();
    for (size_t i = 0; i < rows; ++i) {
        if (s.IsNull(i)) {
            continue;
        }
        auto v = s.Get(i);
        if (!r.has_value || better(v, std::string_view(r.value))) {
            r.value.assign(v.data(), v.size());
            r.has_value = true;
        }
    }
    return r;
}
}  // namespace

ScalarReduction<std::string> MinString(const core::Column& col) {
    return MinMaxStringImpl(col, [](std::string_view a, std::string_view b) { return a < b; });
}

ScalarReduction<std::string> MaxString(const core::Column& col) {
    return MinMaxStringImpl(col, [](std::string_view a, std::string_view b) { return a > b; });
}

AvgPartial Avg(const core::Column& col) {
    return VisitIntegerCol(col, [](const auto& typed) {
        AvgPartial r;
        ForEachNonNull(typed, [&](auto v) {
            r.int_sum += static_cast<__int128>(v);
            ++r.count;
        });
        return r;
    });
}

uint64_t CountNonNull(const core::Column& col) {
    if (!col.IsNullable()) {
        return col.Size();
    }
    size_t rows = col.Size();
    uint64_t c = 0;
    for (size_t i = 0; i < rows; ++i) {
        if (!col.IsNull(i)) {
            ++c;
        }
    }
    return c;
}

void DistinctInts(const core::Column& col, std::unordered_set<int64_t>& out) {
    VisitIntegerCol(col, [&](const auto& typed) {
        ForEachNonNull(typed, [&](auto v) { out.insert(static_cast<int64_t>(v)); });
    });
}

void DistinctStrings(const core::Column& col, std::unordered_set<std::string>& out) {
    if (col.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("DistinctStrings: not a string column");
    }
    auto& s = static_cast<const core::StringColumn&>(col);
    size_t rows = s.Size();
    for (size_t i = 0; i < rows; ++i) {
        if (!s.IsNull(i)) {
            out.emplace(s.Get(i));
        }
    }
}
}  // namespace columnar::exec::kernel

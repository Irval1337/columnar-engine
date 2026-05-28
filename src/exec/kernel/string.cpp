#include <core/columns/dictionary_string_column.h>
#include <core/columns/numeric_column.h>
#include <core/columns/string_column.h>
#include <exec/kernel.h>
#include <exec/kernel/internal.h>
#include <util/bit_vector.h>
#include <util/macro.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace columnar::exec::kernel {
namespace {
std::string_view ApplyPrefixCapture(std::string_view value,
                                    const std::vector<std::string>& prefixes, char delimiter,
                                    bool require_non_empty, bool single_line_tail) {
    for (const auto& prefix : prefixes) {
        if (!value.starts_with(prefix)) {
            continue;
        }
        size_t capture_begin = prefix.size();
        size_t capture_end = value.find(delimiter, capture_begin);
        if (capture_end == std::string_view::npos ||
            (require_non_empty && capture_end == capture_begin)) {
            continue;
        }
        if (single_line_tail && value.find('\n', capture_end + 1) != std::string_view::npos) {
            continue;
        }
        return value.substr(capture_begin, capture_end - capture_begin);
    }
    return value;
}

template <typename Transform>
std::unique_ptr<core::Column> MapDictionaryString(const core::DictionaryStringColumn& dict,
                                                  Transform&& transform) {
    std::vector<char> out_data;
    std::vector<size_t> out_offsets;
    std::vector<uint32_t> remap(dict.DictSize());
    std::unordered_map<std::string, uint32_t> output_ids;
    out_offsets.reserve(dict.DictSize() + 1);
    out_offsets.push_back(0);

    for (uint32_t id = 0; id < dict.DictSize(); ++id) {
        std::string value = transform(dict.DictValue(id));
        auto it = output_ids.find(value);
        if (it == output_ids.end()) {
            uint32_t out_id = static_cast<uint32_t>(out_offsets.size() - 1);
            it = output_ids.emplace(value, out_id).first;
            out_data.insert(out_data.end(), value.begin(), value.end());
            out_offsets.push_back(out_data.size());
        }
        remap[id] = it->second;
    }

    std::vector<uint32_t> ids;
    ids.reserve(dict.Size());
    for (size_t i = 0; i < dict.Size(); ++i) {
        ids.push_back(remap[dict.GetId(i)]);
    }

    util::BitVector is_null;
    if (dict.IsNullable()) {
        is_null = dict.GetNullMask();
    }
    return std::make_unique<core::DictionaryStringColumn>(std::move(out_data),
                                                          std::move(out_offsets), std::move(ids),
                                                          std::move(is_null), dict.IsNullable());
}
}  // namespace

std::unique_ptr<core::Column> ConstString(std::string_view value, size_t rows) {
    auto out = std::make_unique<core::StringColumn>(false);
    out->Reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        out->Append(value);
    }
    return out;
}

std::unique_ptr<core::Column> StrContains(const core::Column& operand, std::string_view substring,
                                          bool negated) {
    if (operand.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("StrContains operand must be a string column");
    }
    if (auto* dict = core::AsDictionaryString(operand)) {
        std::vector<uint8_t> dict_results(dict->DictSize(), 0);
        for (uint32_t id = 0; id < dict_results.size(); ++id) {
            bool found = dict->DictValue(id).find(substring) != std::string_view::npos;
            dict_results[id] = (found != negated) ? 1 : 0;
        }
        size_t rows = dict->Size();
        auto out = MakeBoolColumn(rows);
        for (size_t i = 0; i < rows; ++i) {
            out->Append(!dict->IsNull(i) && dict_results[dict->GetId(i)] != 0);
        }
        return out;
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
        out->Append(found != negated);
    }
    return out;
}

std::unique_ptr<core::Column> StrLength(const core::Column& operand) {
    if (operand.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("length() operand must be a string column");
    }
    if (auto* dict = core::AsDictionaryString(operand)) {
        std::vector<int64_t> lengths(dict->DictSize(), 0);
        for (uint32_t id = 0; id < lengths.size(); ++id) {
            lengths[id] = static_cast<int64_t>(dict->DictValue(id).size());
        }
        size_t rows = dict->Size();
        auto out = std::make_unique<core::Int64Column>(dict->IsNullable());
        out->Reserve(rows);
        for (size_t i = 0; i < rows; ++i) {
            if (dict->IsNull(i)) {
                out->AppendNull();
            } else {
                out->Append(lengths[dict->GetId(i)]);
            }
        }
        return out;
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

std::unique_ptr<core::Column> RegexReplace(const core::Column& operand, const RE2& regex,
                                           const std::string& replacement) {
    if (operand.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("REGEXP_REPLACE operand must be a string column");
    }
    if (auto* dict = core::AsDictionaryString(operand)) {
        return MapDictionaryString(*dict, [&](std::string_view value) {
            std::string replaced(value);
            RE2::GlobalReplace(&replaced, regex, replacement);
            return replaced;
        });
    }
    auto& s = static_cast<const core::StringColumn&>(operand);
    size_t rows = s.Size();
    auto out = std::make_unique<core::StringColumn>(s.IsNullable());
    out->Reserve(rows);
    for (size_t i = 0; i < rows; ++i) {
        if (s.IsNull(i)) {
            out->AppendNull();
            continue;
        }
        auto value = s.Get(i);
        std::string replaced(value);
        RE2::GlobalReplace(&replaced, regex, replacement);
        out->Append(replaced);
    }
    return out;
}

std::unique_ptr<core::Column> PrefixCapture(const core::Column& operand,
                                            const std::vector<std::string>& prefixes,
                                            char delimiter, bool require_non_empty,
                                            bool single_line_tail) {
    if (operand.GetDataType() != core::DataType::String) {
        THROW_RUNTIME_ERROR("PrefixCapture operand must be a string column");
    }
    if (auto* dict = core::AsDictionaryString(operand)) {
        return MapDictionaryString(*dict, [&](std::string_view value) {
            auto captured =
                ApplyPrefixCapture(value, prefixes, delimiter, require_non_empty, single_line_tail);
            return std::string(captured.data(), captured.size());
        });
    }
    auto& s = static_cast<const core::StringColumn&>(operand);
    size_t rows = s.Size();
    auto out = std::make_unique<core::StringColumn>(s.IsNullable());
    out->Reserve(rows);
    for (size_t i = 0; i < rows; ++i) {
        if (s.IsNull(i)) {
            out->AppendNull();
            continue;
        }
        auto value = s.Get(i);
        out->Append(
            ApplyPrefixCapture(value, prefixes, delimiter, require_non_empty, single_line_tail));
    }
    return out;
}
}  // namespace columnar::exec::kernel

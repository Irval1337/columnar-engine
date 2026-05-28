#include <core/encoding/dictionary.h>
#include <util/macro.h>

#include <limits>
#include <string_view>
#include <unordered_map>

namespace columnar::core::encoding {
namespace {
template <typename Offset>
void WriteDictOffsets(util::BufWriter& w, const std::vector<std::string_view>& dict_values) {
    std::vector<Offset> offsets(dict_values.size() + 1);
    Offset cum = 0;
    for (size_t i = 0; i < dict_values.size(); ++i) {
        cum += static_cast<Offset>(dict_values[i].size());
        offsets[i + 1] = cum;
    }
    w.WriteArray(offsets);
}

template <typename Index>
void WriteIndexes(util::BufWriter& w, const std::vector<uint32_t>& indexes) {
    std::vector<Index> packed(indexes.size());
    for (size_t i = 0; i < indexes.size(); ++i) {
        packed[i] = static_cast<Index>(indexes[i]);
    }
    w.WriteArray(packed);
}

void WritePayload(util::BufWriter& w, const std::vector<std::string_view>& dict_values,
                  const std::vector<uint32_t>& indexes) {
    auto dict_count = static_cast<uint32_t>(dict_values.size());
    w.Write<uint32_t>(dict_count);

    size_t total_chars = 0;
    for (auto sv : dict_values) {
        total_chars += sv.size();
    }
    uint8_t offset_width = total_chars <= std::numeric_limits<uint32_t>::max() ? 4 : 8;
    w.Write<uint8_t>(offset_width);
    if (offset_width == 4) {
        WriteDictOffsets<uint32_t>(w, dict_values);
    } else {
        WriteDictOffsets<uint64_t>(w, dict_values);
    }

    for (auto sv : dict_values) {
        w.WriteRaw(sv.data(), sv.size());
    }

    uint8_t index_width = dict_count <= 255 ? 1 : 2;
    w.Write<uint8_t>(index_width);
    if (index_width == 1) {
        WriteIndexes<uint8_t>(w, indexes);
    } else {
        WriteIndexes<uint16_t>(w, indexes);
    }
}
}  // namespace

void EncodeStringDictionary(util::BufWriter& w, const std::vector<char>& data,
                            const std::vector<size_t>& offsets) {
    size_t n = offsets.size() - 1;

    std::unordered_map<std::string_view, uint32_t> dict;
    std::vector<std::string_view> dict_values;
    std::vector<uint32_t> indexes;
    indexes.reserve(n);

    for (size_t i = 0; i < n; ++i) {
        std::string_view sv(data.data() + offsets[i], offsets[i + 1] - offsets[i]);
        auto [it, inserted] = dict.emplace(sv, static_cast<uint32_t>(dict_values.size()));
        if (inserted) {
            if (dict_values.size() >= kMaxDictSize) {
                THROW_RUNTIME_ERROR("Too many dictionary values");
            }
            dict_values.push_back(sv);
        }
        indexes.push_back(it->second);
    }

    WritePayload(w, dict_values, indexes);
}

void EncodeStringDictionary(util::BufWriter& w, const std::vector<std::string_view>& dict_values,
                            const std::vector<uint32_t>& indexes) {
    if (dict_values.size() > kMaxDictSize) {
        THROW_RUNTIME_ERROR("Too many dictionary values");
    }
    WritePayload(w, dict_values, indexes);
}

DecodedStringDictionary DecodeStringDictionary(util::BufReader& r, size_t n) {
    auto dict_count = r.Read<uint32_t>();
    if (dict_count > kMaxDictSize) {
        THROW_RUNTIME_ERROR("Too many dictionary values");
    }

    auto offset_width = r.Read<uint8_t>();
    std::vector<size_t> dict_offsets(dict_count + 1);
    if (offset_width == 4) {
        auto raw = r.ReadArray<uint32_t>(dict_count + 1);
        for (size_t i = 0; i <= dict_count; ++i) {
            dict_offsets[i] = raw[i];
        }
    } else if (offset_width == 8) {
        auto raw = r.ReadArray<uint64_t>(dict_count + 1);
        for (size_t i = 0; i <= dict_count; ++i) {
            dict_offsets[i] = static_cast<size_t>(raw[i]);
        }
    } else {
        THROW_RUNTIME_ERROR("Unsupported dictionary offset width");
    }

    if (dict_offsets.front() != 0) {
        THROW_RUNTIME_ERROR("Corrupted dictionary offsets");
    }
    for (size_t i = 1; i <= dict_count; ++i) {
        if (dict_offsets[i] < dict_offsets[i - 1]) {
            THROW_RUNTIME_ERROR("Corrupted dictionary offsets");
        }
    }

    auto dict_chars = r.ReadArray<char>(dict_offsets.back());

    auto index_width = r.Read<uint8_t>();
    if (index_width != 1 && index_width != 2) {
        THROW_RUNTIME_ERROR("Unsupported dictionary index width");
    }

    std::vector<uint32_t> indexes(n);
    if (index_width == 1) {
        auto raw = r.ReadArray<uint8_t>(n);
        for (size_t i = 0; i < n; ++i) {
            indexes[i] = raw[i];
        }
    } else {
        auto raw = r.ReadArray<uint16_t>(n);
        for (size_t i = 0; i < n; ++i) {
            indexes[i] = raw[i];
        }
    }

    for (auto idx : indexes) {
        if (idx >= dict_count) {
            THROW_RUNTIME_ERROR("Dictionary index " + std::to_string(idx) + " out of range [0, " +
                                std::to_string(dict_count) + ")");
        }
    }

    DecodedStringDictionary out;
    out.dict_data = std::move(dict_chars);
    out.dict_offsets = std::move(dict_offsets);
    out.ids = std::move(indexes);
    return out;
}
}  // namespace columnar::core::encoding

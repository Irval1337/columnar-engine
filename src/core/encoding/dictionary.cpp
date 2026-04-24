#include <core/encoding/dictionary.h>
#include <util/macro.h>

#include <cstring>
#include <limits>
#include <string_view>
#include <unordered_map>

namespace columnar::core::encoding {
namespace {
template <typename Offset>
void WriteDictOffsets(std::ostream& os, const std::vector<std::string_view>& dict_values) {
    std::vector<Offset> offsets(dict_values.size() + 1);
    Offset cum = 0;
    for (size_t i = 0; i < dict_values.size(); ++i) {
        cum += static_cast<Offset>(dict_values[i].size());
        offsets[i + 1] = cum;
    }
    util::WriteArray(os, offsets);
}

template <typename Index>
void WriteIndexes(std::ostream& os, const std::vector<uint32_t>& indexes) {
    std::vector<Index> packed(indexes.size());
    for (size_t i = 0; i < indexes.size(); ++i) {
        packed[i] = static_cast<Index>(indexes[i]);
    }
    util::WriteArray(os, packed);
}

void WritePayload(std::ostream& os, const std::vector<std::string_view>& dict_values,
                  const std::vector<uint32_t>& indexes) {
    auto dict_count = static_cast<uint32_t>(dict_values.size());
    util::Write<uint32_t>(os, dict_count);

    size_t total_chars = 0;
    for (auto sv : dict_values) {
        total_chars += sv.size();
    }
    uint8_t offset_width = total_chars <= std::numeric_limits<uint32_t>::max() ? 4 : 8;
    util::Write<uint8_t>(os, offset_width);
    if (offset_width == 4) {
        WriteDictOffsets<uint32_t>(os, dict_values);
    } else {
        WriteDictOffsets<uint64_t>(os, dict_values);
    }

    for (auto sv : dict_values) {
        util::WriteRaw(os, sv.data(), sv.size());
    }

    uint8_t index_width = dict_count <= 255 ? 1 : 2;
    util::Write<uint8_t>(os, index_width);
    if (index_width == 1) {
        WriteIndexes<uint8_t>(os, indexes);
    } else {
        WriteIndexes<uint16_t>(os, indexes);
    }
}
}  // namespace

void EncodeStringDictionary(std::ostream& os, const std::vector<char>& data,
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

    WritePayload(os, dict_values, indexes);
}

void EncodeStringDictionary(std::ostream& os, const std::vector<std::string_view>& dict_values,
                            const std::vector<uint32_t>& indexes) {
    if (dict_values.size() > kMaxDictSize) {
        THROW_RUNTIME_ERROR("Too many dictionary values");
    }
    WritePayload(os, dict_values, indexes);
}

DecodedStringDictionary DecodeStringDictionary(std::istream& is, size_t n) {
    auto dict_count = util::Read<uint32_t>(is);
    if (dict_count > kMaxDictSize) {
        THROW_RUNTIME_ERROR("Too many dictionary values");
    }

    auto offset_width = util::Read<uint8_t>(is);
    std::vector<size_t> dict_offsets(dict_count + 1);
    if (offset_width == 4) {
        auto raw = util::ReadArray<uint32_t>(is, dict_count + 1);
        for (size_t i = 0; i <= dict_count; ++i) {
            dict_offsets[i] = raw[i];
        }
    } else if (offset_width == 8) {
        auto raw = util::ReadArray<uint64_t>(is, dict_count + 1);
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

    auto dict_chars = util::ReadArray<char>(is, dict_offsets.back());

    auto index_width = util::Read<uint8_t>(is);
    if (index_width != 1 && index_width != 2) {
        THROW_RUNTIME_ERROR("Unsupported dictionary index width");
    }

    std::vector<uint32_t> indexes(n);
    if (index_width == 1) {
        auto raw = util::ReadArray<uint8_t>(is, n);
        for (size_t i = 0; i < n; ++i) {
            indexes[i] = raw[i];
        }
    } else {
        auto raw = util::ReadArray<uint16_t>(is, n);
        for (size_t i = 0; i < n; ++i) {
            indexes[i] = raw[i];
        }
    }

    size_t total = 0;
    for (auto idx : indexes) {
        if (idx >= dict_count) {
            THROW_RUNTIME_ERROR("Dictionary index out of range");
        }
        total += dict_offsets[idx + 1] - dict_offsets[idx];
    }

    DecodedStringDictionary out;
    out.data.resize(total);
    out.offsets.reserve(n + 1);
    out.offsets.push_back(0);
    size_t pos = 0;
    for (auto idx : indexes) {
        size_t start = dict_offsets[idx];
        size_t len = dict_offsets[idx + 1] - start;
        if (len > 0) {
            std::memcpy(out.data.data() + pos, dict_chars.data() + start, len);
        }
        pos += len;
        out.offsets.push_back(pos);
    }
    return out;
}
}  // namespace columnar::core::encoding

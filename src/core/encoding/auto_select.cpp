#include <core/encoding/auto_select.h>
#include <core/encoding/bit_packing.h>
#include <core/encoding/dictionary.h>
#include <core/columns/bool_column.h>
#include <core/columns/char_column.h>
#include <core/columns/dictionary_string_column.h>
#include <core/columns/numeric_column.h>
#include <core/columns/string_column.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace columnar::core::encoding {
namespace {
constexpr size_t kDictMaxSize = 4 * 1024 * 1024;
constexpr size_t kMinRun = 4;
constexpr size_t kDictMinRows = 128;
constexpr size_t kDictMinBytes = 1024;
constexpr size_t kProbeRows = 1024;

template <typename T>
void PickIntegerEncoding(const std::vector<T>& data, bool prefer_delta, AutoEncoding& result) {
    if (data.empty()) {
        return;
    }

    T mn = data[0];
    T mx = data[0];
    bool sorted = true;
    for (size_t i = 1; i < data.size(); ++i) {
        if (data[i] < mn) {
            mn = data[i];
        }
        if (data[i] > mx) {
            mx = data[i];
        }
        if (data[i] < data[i - 1]) {
            sorted = false;
        }
    }
    result.has_int_stats = true;
    result.mn = mn;
    result.mx = mx;

    size_t best = sizeof(T) * data.size();
    Encoding encoding = Encoding::Plain;

    uint8_t for_width = BitWidth(static_cast<uint64_t>(mx) - static_cast<uint64_t>(mn));
    if (for_width <= kBitPackingMaxWidth) {
        size_t sz = sizeof(T) + 1 + BitPackedSize(data.size(), for_width);
        if (sz < best) {
            best = sz;
            encoding = Encoding::FrameOfReference;
        }
    }

    if (sorted && data.size() > 1) {
        T min_delta =
            static_cast<T>(static_cast<uint64_t>(data[1]) - static_cast<uint64_t>(data[0]));
        T max_delta = min_delta;
        for (size_t i = 1; i < data.size(); ++i) {
            T d =
                static_cast<T>(static_cast<uint64_t>(data[i]) - static_cast<uint64_t>(data[i - 1]));
            if (d < min_delta) {
                min_delta = d;
            }
            if (d > max_delta) {
                max_delta = d;
            }
        }
        result.min_delta = min_delta;
        result.max_delta = max_delta;

        uint8_t delta_width =
            BitWidth(static_cast<uint64_t>(max_delta) - static_cast<uint64_t>(min_delta));
        if (delta_width <= kBitPackingMaxWidth) {
            size_t sz = 2 * sizeof(T) + 1 + BitPackedSize(data.size() - 1, delta_width);
            if (prefer_delta ? sz <= best : sz < best) {
                encoding = Encoding::Delta;
            }
        }
    }
    result.encoding = encoding;
}

struct RunStats {
    size_t runs = 1;
    size_t longest = 1;
};

bool RunValue(const util::BitVector& bits, size_t i) {
    return bits.Get(i);
}

template <typename T>
const T& RunValue(const std::vector<T>& data, size_t i) {
    return data[i];
}

template <typename Values>
RunStats CountRuns(const Values& values, size_t n) {
    RunStats stats;
    size_t cur = 1;
    for (size_t i = 1; i < n; ++i) {
        if (RunValue(values, i) == RunValue(values, i - 1)) {
            ++cur;
        } else {
            if (cur > stats.longest) {
                stats.longest = cur;
            }
            cur = 1;
            ++stats.runs;
        }
    }
    if (cur > stats.longest) {
        stats.longest = cur;
    }
    return stats;
}

Encoding PickBoolEncoding(const BoolColumn& col) {
    size_t n = col.Size();
    if (n == 0) {
        return Encoding::BitPacking;
    }
    auto stats = CountRuns(col.GetData(), n);
    return (stats.longest >= kMinRun && 4 + stats.runs * 5 < BitPackedSize(n, 1))
               ? Encoding::RLE
               : Encoding::BitPacking;
}

Encoding PickCharEncoding(const CharColumn& col) {
    size_t n = col.Size();
    if (n == 0) {
        return Encoding::Plain;
    }
    auto stats = CountRuns(col.GetData(), n);
    return (stats.longest >= kMinRun && 4 + stats.runs * 5 < n) ? Encoding::RLE : Encoding::Plain;
}

void PickStringEncoding(const StringColumn& col, AutoEncoding& result) {
    size_t n = col.Size();
    if (n < kDictMinRows) {
        return;
    }
    auto& data = col.GetData();
    auto& offsets = col.GetOffsets();
    size_t total_bytes = offsets.back();
    if (total_bytes < kDictMinBytes) {
        return;
    }

    std::unordered_map<std::string_view, uint32_t> dict;
    std::vector<std::string_view> dict_values;
    std::vector<uint32_t> indexes;
    indexes.reserve(n);
    size_t distinct_bytes = 0;

    for (size_t i = 0; i < n; ++i) {
        std::string_view sv(data.data() + offsets[i], offsets[i + 1] - offsets[i]);
        auto [it, inserted] = dict.emplace(sv, static_cast<uint32_t>(dict_values.size()));
        if (inserted) {
            if (dict_values.size() >= kMaxDictSize || distinct_bytes + sv.size() > kDictMaxSize) {
                return;
            }
            dict_values.push_back(sv);
            distinct_bytes += sv.size();
        }
        indexes.push_back(it->second);

        if (i + 1 == kProbeRows && dict_values.size() * 2 > kProbeRows) {
            return;
        }
    }

    size_t plain_size =
        1 + (data.size() <= std::numeric_limits<uint32_t>::max() ? 4 : 8) * (n + 1) + total_bytes;
    size_t offset_width = distinct_bytes <= std::numeric_limits<uint32_t>::max() ? 4 : 8;
    size_t index_width = dict_values.size() <= 255 ? 1 : 2;
    size_t dict_size =
        4 + 1 + offset_width * (dict_values.size() + 1) + distinct_bytes + 1 + index_width * n;

    if (dict_size < plain_size) {
        result.encoding = Encoding::Dictionary;
        result.dict_values = std::move(dict_values);
        result.dict_indexes = std::move(indexes);
    }
}
}  // namespace

AutoEncoding SelectEncoding(const Column& col, const Field& field) {
    AutoEncoding result;
    switch (field.type) {
        case DataType::Double:
            return result;
        case DataType::Bool:
            result.encoding = PickBoolEncoding(static_cast<const BoolColumn&>(col));
            return result;
        case DataType::Char:
            result.encoding = PickCharEncoding(static_cast<const CharColumn&>(col));
            return result;
        case DataType::String:
            if (AsDictionaryString(col) != nullptr) {
                result.encoding = Encoding::Dictionary;
            } else {
                PickStringEncoding(static_cast<const StringColumn&>(col), result);
            }
            return result;
        case DataType::Int16:
            PickIntegerEncoding(static_cast<const Int16Column&>(col).GetData(), false, result);
            return result;
        case DataType::Int32:
            PickIntegerEncoding(static_cast<const Int32Column&>(col).GetData(), false, result);
            return result;
        case DataType::Int64:
            PickIntegerEncoding(static_cast<const Int64Column&>(col).GetData(), false, result);
            return result;
        case DataType::Date:
            PickIntegerEncoding(static_cast<const Int32Column&>(col).GetData(), true, result);
            return result;
        case DataType::Timestamp:
            PickIntegerEncoding(static_cast<const Int64Column&>(col).GetData(), true, result);
            return result;
    }
    return result;
}
}  // namespace columnar::core::encoding

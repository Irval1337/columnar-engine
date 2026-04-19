#include <util/date_time.h>
#include <util/macro.h>

#include <cstddef>

namespace columnar::util {
namespace {
int DateToDays(int year, int month, int day) noexcept {
    if (month <= 2) {
        --year;
        month += 12;
    }
    int era400 = year / 400;
    int year_in_era = year - era400 * 400;
    int day_in_year = (153 * (month - 3) + 2) / 5 + day - 1;
    int day_in_era = year_in_era * 365 + year_in_era / 4 - year_in_era / 100 + day_in_year;
    return era400 * 146097 + day_in_era - 719468;
}

Date DaysToDate(int days) noexcept {
    days += 719468;
    int era400 = days / 146097;
    int day_in_era = days - era400 * 146097;
    int year_in_era =
        (day_in_era - day_in_era / 1460 + day_in_era / 36524 - day_in_era / 146096) / 365;
    int day_in_year = day_in_era - (365 * year_in_era + year_in_era / 4 - year_in_era / 100);
    int month_idx = (5 * day_in_year + 2) / 153;
    int month = month_idx < 10 ? month_idx + 3 : month_idx - 9;
    int year = year_in_era + era400 * 400 + (month <= 2);
    int day = day_in_year - (153 * month_idx + 2) / 5 + 1;
    return {year, month, day};
}

int ParseDigits(const char* p, size_t n) {
    int value = 0;
    for (size_t i = 0; i < n; ++i) {
        if (p[i] < '0' || p[i] > '9') {
            THROW_RUNTIME_ERROR("Invalid digit in datetime value");
        }
        value = value * 10 + (p[i] - '0');
    }
    return value;
}

void AppendDigits(std::string& out, int value, size_t n) {
    size_t pos = out.size();
    out.resize(pos + n);
    for (int i = n - 1; i >= 0; --i) {
        out[pos + i] = '0' + value % 10;
        value /= 10;
    }
}
}  // namespace

int32_t ParseDate(std::string_view s) {
    if (s.size() != 10 || s[4] != '-' || s[7] != '-') {
        THROW_RUNTIME_ERROR("Invalid date value: " + std::string(s));
    }
    Date date{
        ParseDigits(s.data(), 4),
        ParseDigits(s.data() + 5, 2),
        ParseDigits(s.data() + 8, 2),
    };
    if (date.month < 1 || date.month > 12 || date.day < 1 || date.day > 31) {
        THROW_RUNTIME_ERROR("Invalid date value: " + std::string(s));
    }
    return DateToDays(date.year, date.month, date.day);
}

void AppendDate(int32_t days, std::string& out) {
    auto [year, month, day] = DaysToDate(days);
    AppendDigits(out, year, 4);
    out += '-';
    AppendDigits(out, month, 2);
    out += '-';
    AppendDigits(out, day, 2);
}

int64_t ParseTimestamp(std::string_view s) {
    if (s.size() != 19 || (s[10] != ' ' && s[10] != 'T') || s[13] != ':' || s[16] != ':') {
        THROW_RUNTIME_ERROR("Invalid timestamp value: " + std::string(s));
    }
    int32_t date_days = ParseDate(s.substr(0, 10));
    Time time{
        ParseDigits(s.data() + 11, 2),
        ParseDigits(s.data() + 14, 2),
        ParseDigits(s.data() + 17, 2),
    };
    if (time.hour > 23 || time.minute > 59 || time.second > 59) {
        THROW_RUNTIME_ERROR("Invalid timestamp value: " + std::string(s));
    }
    return static_cast<int64_t>(date_days) * 86400 + time.hour * 3600 + time.minute * 60 +
           time.second;
}

void AppendTimestamp(int64_t seconds, std::string& out) {
    int64_t day_count = seconds / 86400;
    int64_t tod = seconds % 86400;
    if (tod < 0) {
        --day_count;
        tod += 86400;
    }
    AppendDate(static_cast<int32_t>(day_count), out);
    out += ' ';
    AppendDigits(out, static_cast<int>(tod / 3600), 2);
    out += ':';
    AppendDigits(out, static_cast<int>((tod / 60) % 60), 2);
    out += ':';
    AppendDigits(out, static_cast<int>(tod % 60), 2);
}
}  // namespace columnar::util

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace columnar::util {
struct Date {
    int year;
    int month;
    int day;
};

struct Time {
    int hour;
    int minute;
    int second;
};

int32_t ParseDate(std::string_view s);
void AppendDate(int32_t days, std::string& out);

int64_t ParseTimestamp(std::string_view s);
void AppendTimestamp(int64_t seconds, std::string& out);
}  // namespace columnar::util

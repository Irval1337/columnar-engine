#pragma once

#include <core/schema.h>
#include <csv/csv_row_reader.h>
#include <util/macro.h>

#include <fstream>
#include <string>

namespace columnar::csv {
class SchemaManager {
public:
    static core::Schema ReadFromFile(const std::string& path);

    static core::Schema ReadFromStream(std::istream& is);

    static void WriteToFile(const std::string& path, const core::Schema& schema);

    static void WriteToStream(std::ostream& os, const core::Schema& schema);
};
}  // namespace columnar::csv

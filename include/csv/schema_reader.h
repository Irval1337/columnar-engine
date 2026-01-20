#pragma once

#include <core/schema.h>
#include <csv/csv_row_reader.h>
#include <util/macro.h>

#include <fstream>
#include <string>

namespace columnar::csv {
class SchemaManager {
public:
    static core::Schema ReadFromFile(const std::string& path) {
        std::ifstream is(path, std::ios::binary);
        if (!is.is_open() || !is.good()) {
            THROW_RUNTIME_ERROR("Cannot open file");
        }
        return ReadFromStream(is);
    }

    static core::Schema ReadFromStream(std::istream& is) {
        CSVRowReader reader(is);
        std::vector<core::Field> fields;
        while (auto row = reader.ReadRow()) {
            if (row->size() < 2) {
                THROW_RUNTIME_ERROR("Invalid schema row");
            }
            auto type = core::StringToDataType((*row)[1]);
            bool nullable = row->size() >= 3 && ((*row)[2] == "nullable" || (*row)[2] == "true");
            fields.emplace_back((*row)[0], type, nullable);
        }
        return core::Schema(std::move(fields));
    }

    static void WriteToFile(const std::string& path, const core::Schema& schema) {
        std::ofstream os(path, std::ios::binary);
        if (!os.is_open() || !os.good()) {
            THROW_RUNTIME_ERROR("Cannot open file");
        }
        WriteToStream(os, schema);
    }

    static void WriteToStream(std::ostream& os, const core::Schema& schema) {
        for (const auto& field : schema.GetFields()) {
            os << field.name << "," << core::DataTypeToString(field.type);
            if (field.nullable) {
                os << ",nullable";
            }
            os << '\n';
        }
    }
};
}  // namespace columnar::csv

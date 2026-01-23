#include <csv/schema_manager.h>

namespace columnar::csv {
core::Schema SchemaManager::ReadFromFile(const std::string& path) {
    std::ifstream is(path, std::ios::binary);
    if (!is.is_open() || !is.good()) {
        THROW_RUNTIME_ERROR("Cannot open file");
    }
    return ReadFromStream(is);
}

core::Schema SchemaManager::ReadFromStream(std::istream& is) {
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

void SchemaManager::WriteToFile(const std::string& path, const core::Schema& schema) {
    std::ofstream os(path, std::ios::binary);
    if (!os.is_open() || !os.good()) {
        THROW_RUNTIME_ERROR("Cannot open file");
    }
    WriteToStream(os, schema);
}

void SchemaManager::WriteToStream(std::ostream& os, const core::Schema& schema) {
    for (const auto& field : schema.GetFields()) {
        os << field.name << "," << core::DataTypeToString(field.type);
        if (field.nullable) {
            os << ",nullable";
        }
        os << '\n';
    }
}
}  // namespace columnar::csv

#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/flags/usage.h>
#include <csv/csv.h>
#include <bruh/bruh.h>

#include <fstream>
#include <iostream>

ABSL_FLAG(std::string, mode, "", "Conversion mode: csv2bruh or bruh2csv");
ABSL_FLAG(std::string, schema, "", "Path to .csv schema file");
ABSL_FLAG(std::string, input, "", "Input file path");
ABSL_FLAG(std::string, output, "", "Output file path");

using namespace columnar;  // NOLINT

std::ifstream OpenInput(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in || !in.good()) {
        throw std::runtime_error("Cannot open file " + path);
    }
    return in;
}

std::ofstream OpenOutput(const std::string& path) {
    std::ofstream out(path, std::ios::binary);
    if (!out || !out.good()) {
        throw std::runtime_error("Cannot open file " + path);
    }
    return out;
}

template <typename Reader, typename Writer>
void Convert(Reader& reader, Writer& writer) {
    while (auto batch = reader.ReadNext()) {
        writer.Write(*batch);
    }
    writer.Flush();
}

int main(int argc, char** argv) {
    absl::SetProgramUsageMessage(
        "Amazing converter between CSV and BruhDB formats.\n\n"
        "Usage:\n"
        "  converter --mode=csv2bruh --schema=schema.csv --input=data.csv --output=data.bruhdb\n"
        "  converter --mode=bruh2csv --input=data.bruhdb --output=data.csv [--schema=schema.csv]");
    absl::ParseCommandLine(argc, argv);

    auto mode = absl::GetFlag(FLAGS_mode);
    auto input = absl::GetFlag(FLAGS_input);
    auto output = absl::GetFlag(FLAGS_output);
    auto schema_path = absl::GetFlag(FLAGS_schema);
    if (mode.empty() || input.empty() || output.empty()) {
        std::cerr << absl::ProgramUsageMessage();
        return 1;
    }

    try {
        if (mode == "csv2bruh") {
            if (schema_path.empty()) {
                throw std::runtime_error("Schema required");
            }
            auto schema = csv::SchemaManager::ReadFromFile(schema_path);
            auto in = OpenInput(input);
            auto out = OpenOutput(output);
            csv::CSVBatchReader reader(in, schema, {});
            bruh::BruhBatchWriter writer(out, schema);
            Convert(reader, writer);
        } else if (mode == "bruh2csv") {
            auto in = OpenInput(input);
            bruh::BruhBatchReader reader(in);
            if (!schema_path.empty()) {
                csv::SchemaManager::WriteToFile(schema_path, reader.GetSchema());
            }
            auto out = OpenOutput(output);
            csv::CSVBatchWriter writer(out, {});
            Convert(reader, writer);
        } else {
            throw std::runtime_error("Unknown mode");
        }
        std::cout << "Done.\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    } catch (...) {
        std::cerr << "Unknown error\n";
        return 1;
    }
}

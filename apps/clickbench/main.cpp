#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/flags/usage.h>
#include <csv/csv_batch_writer.h>
#include <exec/clickbench.h>
#include <util/buffered_file.h>
#include <util/memory_mapped_file.h>

#include <iostream>
#include <string>
#include <vector>

ABSL_FLAG(std::string, input, "", "Input .bruh file");
ABSL_FLAG(int, query, 0, "ClickBench query id");
ABSL_FLAG(std::string, output, "-", "Output CSV path, or '-' for stdout");
ABSL_FLAG(bool, header, false, "Print CSV header");

using namespace columnar;  // NOLINT

namespace {
void WriteBatches(std::ostream& os, const std::vector<core::Batch>& batches, bool header) {
    csv::CSVOptions options;
    options.has_header = header;
    csv::CSVBatchWriter writer(os, options);
    for (auto& batch : batches) {
        writer.Write(batch);
    }
    writer.Flush();
}

}  // namespace

int main(int argc, char** argv) {
    absl::SetProgramUsageMessage("Runs a ClickBench query over a BruhDB file.");
    absl::ParseCommandLine(argc, argv);

    auto input = absl::GetFlag(FLAGS_input);
    auto output = absl::GetFlag(FLAGS_output);
    int query = absl::GetFlag(FLAGS_query);
    bool header = absl::GetFlag(FLAGS_header);
    if (input.empty()) {
        std::cerr << absl::ProgramUsageMessage() << "\n";
        return 1;
    }
    if (query < 0) {
        std::cerr << "Unsupported ClickBench query id: " << query << "\n";
        return 1;
    }

    try {
        util::MemoryMappedInputFile in(input);
        auto batches = exec::ExecuteClickBenchQuery(in.View(), static_cast<size_t>(query));
        if (output == "-") {
            WriteBatches(std::cout, batches, header);
        } else {
            util::BufferedOutputFile out(output);
            WriteBatches(out, batches, header);
        }
        return 0;
    } catch (const std::exception& e) {
        std::string error = e.what();
        if (error.find("Unsupported ClickBench query") != std::string::npos) {
            std::cerr << "ClickBench query " << query << " is not implemented\n";
            return 2;
        }
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    } catch (...) {
        std::cerr << "Unknown error\n";
        return 1;
    }
}

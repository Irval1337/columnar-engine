#include <benchmark/benchmark.h>
#include <csv/csv.h>
#include <core/schema.h>

#include <sstream>

using namespace columnar;  // NOLINT

std::string GenerateCSV(std::size_t rows) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < rows; ++i) {
        ss << i << ",value" << i << ",0.123\n";
    }
    return ss.str();
}

void BenchCSVRowReader(benchmark::State& state) {
    std::string data = GenerateCSV(state.range(0));
    for (auto s : state) {
        std::istringstream in(data);
        csv::CSVRowReader reader(in);
        while (reader.ReadRow()) {
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BenchCSVRowReader)->Range(100, 10000);

void BenchCSVBatchReader(benchmark::State& state) {
    core::Schema schema({core::Field("id", core::DataType::Int64),
                         core::Field("name", core::DataType::String),
                         core::Field("value", core::DataType::Double)});

    std::string data = GenerateCSV(state.range(0));
    for (auto s : state) {
        std::istringstream in(data);
        csv::CSVBatchReader reader(in, schema, {});
        while (reader.ReadNext()) {
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BenchCSVBatchReader)->Range(100, 10000);

void BenchCSVBatchWriter(benchmark::State& state) {
    core::Schema schema(
        {core::Field("id", core::DataType::Int64), core::Field("name", core::DataType::String)});

    core::Batch batch(schema);
    for (int i = 0; i < state.range(0); ++i) {
        batch.ColumnAt(0).AppendFromString(std::to_string(i));
        batch.ColumnAt(1).AppendFromString("hello mir");
    }

    for (auto s : state) {
        std::ostringstream out;
        csv::CSVBatchWriter writer(out, {});
        writer.Write(batch);
        writer.Flush();
        benchmark::DoNotOptimize(out.str());
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BenchCSVBatchWriter)->Range(100, 10000);

BENCHMARK_MAIN();

#include <benchmark/benchmark.h>
#include <bruh/bruh.h>
#include <core/schema.h>

#include <sstream>

using namespace columnar;  // NOLINT

void BenchBruhBatchWriter(benchmark::State& state) {
    core::Schema schema({core::Field("id", core::DataType::Int64),
                         core::Field("name", core::DataType::String),
                         core::Field("value", core::DataType::Double)});
    core::Batch batch(schema);
    for (int i = 0; i < state.range(0); ++i) {
        batch.ColumnAt(0).AppendFromString(std::to_string(i));
        batch.ColumnAt(1).AppendFromString("value" + std::to_string(i));
        batch.ColumnAt(2).AppendFromString("0.123");
    }

    for (auto s : state) {
        std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
        bruh::BruhBatchWriter writer(ss, schema);
        writer.Write(batch);
        writer.Flush();
        benchmark::DoNotOptimize(ss.str());
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BenchBruhBatchWriter)->Range(100, 10000);

void BenchBruhBatchReader(benchmark::State& state) {
    core::Schema schema({core::Field("id", core::DataType::Int64),
                         core::Field("name", core::DataType::String),
                         core::Field("value", core::DataType::Double)});
    core::Batch batch(schema);
    for (int i = 0; i < state.range(0); ++i) {
        batch.ColumnAt(0).AppendFromString(std::to_string(i));
        batch.ColumnAt(1).AppendFromString("value" + std::to_string(i));
        batch.ColumnAt(2).AppendFromString("0.123");
    }

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    {
        bruh::BruhBatchWriter writer(ss, schema);
        writer.Write(batch);
        writer.Flush();
    }
    std::string data = ss.str();

    for (auto s : state) {
        std::istringstream in(data, std::ios::binary);
        bruh::BruhBatchReader reader(in);
        while (reader.ReadNext()) {
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BenchBruhBatchReader)->Range(100, 10000);

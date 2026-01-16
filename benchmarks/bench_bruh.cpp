#include <benchmark/benchmark.h>
#include "columnar/add.h"

static void BenchmarkAdd(benchmark::State& state) {
    int x = 1;
    int y = 2;
    for (auto _ : state) {
        benchmark::DoNotOptimize(columnar::Add(x, y));
    }
}

BENCHMARK(BenchmarkAdd);

add_executable(columnar_bench
  bench_csv.cpp
  bench_bruh.cpp
)

target_link_libraries(columnar_bench
  PRIVATE
    columnar_lib
    benchmark::benchmark_main
)

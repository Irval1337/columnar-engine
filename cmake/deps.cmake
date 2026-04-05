include(FetchContent)

if(COLUMNAR_BUILD_TESTS)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

if(COLUMNAR_BUILD_BENCHMARKS)
  FetchContent_Declare(
    benchmark
    URL https://github.com/google/benchmark/archive/refs/tags/v1.9.1.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  )
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(benchmark)
endif()

FetchContent_Declare(
  absl
  URL https://github.com/abseil/abseil-cpp/archive/refs/tags/20240116.2.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(absl)

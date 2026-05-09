cmake_minimum_required(VERSION 3.20)
project(columnar_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(COLUMNAR_BUILD_TESTS "Build tests" ON)

include(cmake/deps.cmake)

add_library(columnar_lib
  src/core/column.cpp
  src/core/encoding/auto_select.cpp
  src/core/encoding/dictionary.cpp
  src/core/encoding/rle.cpp
  src/util/buffered_file.cpp
  src/util/compression.cpp
  src/util/date_time.cpp
  src/csv/csv_batch_reader.cpp
  src/csv/csv_batch_writer.cpp
  src/csv/csv_row_reader.cpp
  src/csv/schema_manager.cpp
  src/bruh/bruh_batch_reader.cpp
  src/bruh/bruh_batch_writer.cpp
  src/exec/aggregation.cpp
  src/exec/clickbench.cpp
  src/exec/expression.cpp
  src/exec/filter_operator.cpp
  src/exec/global_aggregate_operator.cpp
  src/exec/hash_aggregate_operator.cpp
  src/exec/kernel.cpp
  src/exec/operator.cpp
  src/exec/project_operator.cpp
  src/exec/topn_operator.cpp
)

target_include_directories(columnar_lib PUBLIC ${PROJECT_SOURCE_DIR}/include)

target_include_directories(columnar_lib PRIVATE
  ${lz4_SOURCE_DIR}/lib
  ${zstd_SOURCE_DIR}/lib
)

target_link_libraries(columnar_lib PRIVATE lz4_static libzstd_static)

target_compile_options(columnar_lib PRIVATE -Wall -Wextra -Wpedantic)

add_subdirectory(apps/converter)
add_subdirectory(apps/clickbench)

if(COLUMNAR_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

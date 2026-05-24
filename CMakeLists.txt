cmake_minimum_required(VERSION 3.20)
project(columnar_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(COLUMNAR_BUILD_TESTS "Build tests" ON)
option(COLUMNAR_ENABLE_NATIVE "Build engine code with -march=native when supported" ON)
option(COLUMNAR_ENABLE_IPO "Enable interprocedural optimization/LTO when supported" ON)
set(COLUMNAR_ALLOCATOR "auto" CACHE STRING "Allocator to link into executables: auto, none, mimalloc, jemalloc")
set_property(CACHE COLUMNAR_ALLOCATOR PROPERTY STRINGS auto none mimalloc jemalloc)

include(cmake/deps.cmake)

add_library(columnar_lib
  src/core/column.cpp
  src/core/encoding/auto_select.cpp
  src/core/encoding/dictionary.cpp
  src/core/encoding/rle.cpp
  src/util/buffered_file.cpp
  src/util/memory_mapped_file.cpp
  src/util/compression.cpp
  src/util/date_time.cpp
  src/util/string_arena.cpp
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
  src/exec/metadata_pruning.cpp
  src/exec/operator.cpp
  src/exec/project_operator.cpp
  src/exec/topn_operator.cpp
)

target_include_directories(columnar_lib PUBLIC ${PROJECT_SOURCE_DIR}/include)

target_include_directories(columnar_lib PRIVATE
  ${lz4_SOURCE_DIR}/lib
  ${zstd_SOURCE_DIR}/lib
)

target_link_libraries(columnar_lib
  PUBLIC re2::re2
  PRIVATE lz4_static libzstd_static absl::flat_hash_map)

target_compile_options(columnar_lib PRIVATE -Wall -Wextra -Wpedantic)
if(COLUMNAR_ENABLE_NATIVE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(columnar_lib PRIVATE -march=native)
endif()

if(COLUMNAR_ENABLE_IPO)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT COLUMNAR_IPO_SUPPORTED OUTPUT COLUMNAR_IPO_ERROR LANGUAGES CXX)
  if(COLUMNAR_IPO_SUPPORTED)
    set_property(TARGET columnar_lib PROPERTY INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)
  else()
    message(STATUS "IPO/LTO not supported: ${COLUMNAR_IPO_ERROR}")
  endif()
endif()

set(COLUMNAR_ALLOCATOR_LIBRARY "")
if(NOT COLUMNAR_ALLOCATOR STREQUAL "none")
  if(COLUMNAR_ALLOCATOR STREQUAL "mimalloc" OR COLUMNAR_ALLOCATOR STREQUAL "auto")
    find_library(COLUMNAR_MIMALLOC_LIBRARY NAMES mimalloc)
    if(COLUMNAR_MIMALLOC_LIBRARY)
      set(COLUMNAR_ALLOCATOR_LIBRARY "${COLUMNAR_MIMALLOC_LIBRARY}")
    elseif(COLUMNAR_ALLOCATOR STREQUAL "mimalloc")
      message(FATAL_ERROR "COLUMNAR_ALLOCATOR=mimalloc requested, but mimalloc was not found")
    endif()
  endif()
  if(NOT COLUMNAR_ALLOCATOR_LIBRARY AND
     (COLUMNAR_ALLOCATOR STREQUAL "jemalloc" OR COLUMNAR_ALLOCATOR STREQUAL "auto"))
    find_library(COLUMNAR_JEMALLOC_LIBRARY NAMES jemalloc)
    if(COLUMNAR_JEMALLOC_LIBRARY)
      set(COLUMNAR_ALLOCATOR_LIBRARY "${COLUMNAR_JEMALLOC_LIBRARY}")
    elseif(COLUMNAR_ALLOCATOR STREQUAL "jemalloc")
      message(FATAL_ERROR "COLUMNAR_ALLOCATOR=jemalloc requested, but jemalloc was not found")
    endif()
  endif()
endif()

function(columnar_tune_executable target)
  if(TARGET ${target})
    if(COLUMNAR_ENABLE_IPO AND COLUMNAR_IPO_SUPPORTED)
      set_property(TARGET ${target} PROPERTY INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)
    endif()
    if(COLUMNAR_ALLOCATOR_LIBRARY)
      target_link_libraries(${target} PRIVATE "${COLUMNAR_ALLOCATOR_LIBRARY}")
    endif()
  endif()
endfunction()

add_subdirectory(apps/converter)
add_subdirectory(apps/clickbench)

columnar_tune_executable(converter)
columnar_tune_executable(clickbench_runner)

if(COLUMNAR_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
  columnar_tune_executable(columnar_tests)
endif()

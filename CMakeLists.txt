cmake_minimum_required(VERSION 3.20)
project(columnar_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(COLUMNAR_BUILD_TESTS "Build tests" ON)
option(COLUMNAR_BUILD_BENCHMARKS "Build benchmarks" ON)

include(cmake/deps.cmake)

add_library(columnar_lib
  src/core/column.cpp
)

target_include_directories(columnar_lib PUBLIC ${PROJECT_SOURCE_DIR}/include)

target_compile_options(columnar_lib PRIVATE -Wall -Wextra -Wpedantic)

add_subdirectory(apps/converter)

if(COLUMNAR_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(COLUMNAR_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

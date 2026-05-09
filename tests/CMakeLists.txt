add_executable(columnar_tests
  test_csv.cpp
  test_bruh.cpp
  test_bruh_compression.cpp
  test_bruh_encoding.cpp
  test_exec.cpp
)

target_link_libraries(columnar_tests
  PRIVATE
    columnar_lib
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(columnar_tests)

add_executable(clickbench_runner
  main.cpp
)

target_link_libraries(clickbench_runner
  PRIVATE
    columnar_lib
    absl::flags
    absl::flags_parse
)

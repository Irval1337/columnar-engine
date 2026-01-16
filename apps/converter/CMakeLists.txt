add_executable(converter
  main.cpp
)

target_link_libraries(converter
  PRIVATE
    columnar_lib
    absl::flags
    absl::flags_parse
)

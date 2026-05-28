#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace columnar::exec {
template <typename F>
void ForSelectedRows(const std::vector<uint32_t>* selection, size_t rows, F&& f) {
    if (selection != nullptr) {
        for (uint32_t row : *selection) {
            f(static_cast<size_t>(row));
        }
    } else {
        for (size_t row = 0; row < rows; ++row) {
            f(row);
        }
    }
}
}  // namespace columnar::exec

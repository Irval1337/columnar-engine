#pragma once

#include <cstddef>

namespace columnar::bruh {
class BruhBatchReader;
}

namespace columnar::exec {
class Expression;

bool PredicateMayMatch(bruh::BruhBatchReader& reader, size_t row_group, const Expression& expr);
}  // namespace columnar::exec

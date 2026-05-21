#pragma once

#include <bruh/bruh_batch_reader.h>
#include <exec/expression.h>

#include <cstddef>

namespace columnar::exec {
bool PredicateMayMatch(bruh::BruhBatchReader& reader, size_t row_group, const Expression& expr);
}  // namespace columnar::exec

#pragma once

#include <bruh/bruh_batch_reader.h>
#include <exec/operator.h>

namespace columnar::exec {
bool TryExecuteLateTopN(bruh::BruhBatchReader& reader, const ProjectOperator& project,
                        IOperator& downstream);
}  // namespace columnar::exec

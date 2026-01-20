#pragma once

#include <core/batch.h>

namespace columnar::core {
class BatchWriter {
public:
    virtual ~BatchWriter() = default;

    virtual void Write(const Batch& batch) = 0;

    virtual void Flush() = 0;
};
}  // namespace columnar::core

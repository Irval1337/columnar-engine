#pragma once

#include <core/batch.h>

#include <optional>

namespace columnar::core {
class BatchReader {
public:
    virtual ~BatchReader() = default;

    virtual std::optional<Batch> ReadNext() = 0;

    virtual const core::Schema& GetSchema() const = 0;
};
}  // namespace columnar::core

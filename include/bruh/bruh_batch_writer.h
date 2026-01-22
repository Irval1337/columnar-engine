#pragma once

#include <core/batch_writer.h>
#include <bruh/format.h>
#include <util/macro.h>

#include <fstream>
#include <memory>
#include <ostream>
#include <string>

namespace columnar::bruh {
class BruhBatchWriter final : public core::BatchWriter {
public:
    BruhBatchWriter(std::ostream& os, const core::Schema& schema) : os_(os), schema_(schema) {
        metadata_.version = kCurrentVersion;
        metadata_.schema = schema;
        WriteMagic();
    }

    void Write(const core::Batch& batch) override;

    void Flush() override;

private:
    void WriteMagic() {
        os_.write(reinterpret_cast<const char*>(kMagicBytes), sizeof(kMagicBytes));
    }

    void WriteColumn(const core::Column& col, const core::Field& field);

    void WriteFooter();

    void WriteFields();

    void WriteRowGroups();

    std::ostream& os_;
    core::Schema schema_;
    FileMetaData metadata_;
};
}  // namespace columnar::bruh

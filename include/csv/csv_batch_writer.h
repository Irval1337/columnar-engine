#pragma once

#include <core/batch_writer.h>
#include <csv/csv_options.h>
#include <util/macro.h>

#include <fstream>
#include <string>

namespace columnar::csv {
class CSVBatchWriter final : public core::BatchWriter {
public:
    CSVBatchWriter(std::ostream& os, CSVOptions options) : os_(os), options_(options) {
    }

    void Write(const core::Batch& batch) override;

    void Flush() override {
        os_.flush();
    }

private:
    void WriteHeader(const core::Schema& schema);

    void WriteField(const std::string& value);

    std::ostream& os_;
    CSVOptions options_;
    bool header_written_ = false;
};
}  // namespace columnar::csv

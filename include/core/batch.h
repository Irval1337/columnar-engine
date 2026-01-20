#pragma once

#include <core/schema.h>
#include <core/column.h>
#include <util/macro.h>

namespace columnar::core {
class Batch {
public:
    Batch() = default;

    Batch(Schema schema) : schema_(std::move(schema)) {
        columns_.reserve(schema_.FieldsCount());
        for(auto& field : schema_.GetFields()) {
            columns_.emplace_back(MakeColumn(field.type, field.nullable));
        }
    }

    Batch(Schema schema, std::size_t reserve_rows) : Batch(std::move(schema)) {
        Reserve(reserve_rows);
    }

    const Schema& GetSchema() const {
        return schema_;
    }

    std::size_t ColumnsCount() const {
        return columns_.size();
    }

    std::size_t RowsCount() const {
        if (columns_.empty()) {
            return 0;
        }
        return columns_[0]->Size();
    }

    void Reserve(std::size_t n) {
        for (auto& c : columns_) {
            c->Reserve(n);
        }
    }

    const Column& ColumnAt(std::size_t i) const {
        return *columns_.at(i);
    }

    Column& ColumnAt(std::size_t i) {
        return *columns_.at(i);
    }

    const std::vector<std::unique_ptr<Column>>& GetColumns() const {
        return columns_;
    }

    std::vector<std::unique_ptr<Column>>& GetColumns() {
        return columns_;
    }

    void Validate() const {
        if (columns_.empty()) {
            return;
        }
        for (auto& c : columns_) {
            if (c->Size() != columns_[0]->Size()) {
                THROW_RUNTIME_ERROR("Different columns sizes");
            }
        }
    }

private:
    Schema schema_;
    std::vector<std::unique_ptr<Column>> columns_;
};
}  // namespace columnar::core

#pragma once

#include <core/field.h>
#include <util/macro.h>

#include <cstddef>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace columnar::core {
class Schema {
public:
    Schema() = default;

    Schema(std::vector<Field> fields) {
        for (auto& f : fields) {
            AddField(std::move(f));
        }
    }

    const std::vector<Field>& GetFields() const {
        return fields_;
    }

    std::size_t FieldsCount() const {
        return fields_.size();
    }

    void AddField(Field field) {
        index_.emplace(field.name, fields_.size());
        fields_.push_back(std::move(field));
    }

    const Field* FindField(std::string_view name) const {
        auto it = index_.find(std::string(name));
        if (it == index_.end()) {
            return nullptr;
        }
        return &fields_[it->second];
    }

    bool HasField(std::string_view name) const {
        return FindField(name) != nullptr;
    }

    std::size_t GetIndex(std::string_view name) const {
        auto it = index_.find(std::string(name));
        if (it == index_.end()) {
            THROW_RUNTIME_ERROR("Unknown field");
        }
        return it->second;
    }

private:
    std::vector<Field> fields_;
    std::unordered_map<std::string, std::size_t> index_;
};
}  // namespace columnar::core

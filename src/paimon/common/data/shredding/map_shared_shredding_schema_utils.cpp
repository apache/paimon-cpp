/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/data/shredding/map_shared_shredding_schema_utils.h"

#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "fmt/format.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

class MapSharedShreddingAccessBuilder::Impl {
 public:
    Impl(const std::shared_ptr<arrow::Field>& _map_field,
         const std::shared_ptr<arrow::MapType>& _map_type)
        : map_field(_map_field), map_type(_map_type) {}

    std::shared_ptr<arrow::Field> map_field;
    std::shared_ptr<arrow::MapType> map_type;
    std::vector<std::string> keys;
    std::unordered_set<std::string> unique_keys;
};

MapSharedShreddingAccessBuilder::~MapSharedShreddingAccessBuilder() = default;

MapSharedShreddingAccessBuilder::MapSharedShreddingAccessBuilder(std::unique_ptr<Impl>&& impl)
    : impl_(std::move(impl)) {}

Result<std::unique_ptr<MapSharedShreddingAccessBuilder>> MapSharedShreddingAccessBuilder::Create(
    struct ArrowSchema* map_field) {
    if (!map_field) {
        return Status::Invalid("MAP field is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Field> field,
                                      arrow::ImportField(map_field));
    if (field->type()->id() != arrow::Type::MAP) {
        return Status::Invalid(
            fmt::format("MapSharedShreddingAccessBuilder requires MAP field, got {}",
                        field->type()->ToString()));
    }
    auto map_type = arrow::internal::checked_pointer_cast<arrow::MapType>(field->type());
    if (map_type->key_type()->id() != arrow::Type::STRING) {
        return Status::Invalid(fmt::format(
            "MapSharedShreddingAccessBuilder only supports MAP with STRING keys, got {}",
            map_type->key_type()->ToString()));
    }
    auto impl = std::make_unique<Impl>(field, map_type);
    return std::unique_ptr<MapSharedShreddingAccessBuilder>(
        new MapSharedShreddingAccessBuilder(std::move(impl)));
}

Status MapSharedShreddingAccessBuilder::AddKey(const std::string& key) {
    if (key.find(',') != std::string::npos) {
        return Status::Invalid(
            fmt::format("selected MAP key {} must not contain the ',' delimiter", key));
    }
    if (!impl_->unique_keys.insert(key).second) {
        return Status::Invalid(fmt::format("selected MAP key must not be duplicated: {}", key));
    }
    impl_->keys.push_back(key);
    return Status::OK();
}

Result<std::unique_ptr<struct ArrowSchema>> MapSharedShreddingAccessBuilder::Build() const {
    if (impl_->keys.empty()) {
        return Status::Invalid(
            "shared shredding MAP selected-key projection needs at least one key");
    }
    arrow::FieldVector fields;
    fields.reserve(impl_->keys.size());
    std::string encoded_keys;
    for (size_t i = 0; i < impl_->keys.size(); ++i) {
        if (i != 0) {
            encoded_keys.push_back(',');
        }
        encoded_keys.append(impl_->keys[i]);
        fields.push_back(arrow::field(impl_->keys[i], impl_->map_type->item_type(),
                                      /*nullable=*/true));
    }
    auto metadata = arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {encoded_keys});
    auto access_field = impl_->map_field->WithType(arrow::struct_(std::move(fields)))
                            ->WithMetadata(std::move(metadata));
    auto field = std::make_unique<struct ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportField(*access_field, field.get()));
    return field;
}

Result<std::unique_ptr<::ArrowSchema>> MapSharedShreddingSchemaUtils::LogicalToPhysicalSchema(
    std::unique_ptr<::ArrowSchema> logical_schema,
    const std::map<std::string, int32_t>& field_to_num_columns) {
    if (!logical_schema) {
        return Status::Invalid("logical schema is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_logical_schema,
                                      arrow::ImportSchema(logical_schema.get()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> physical_schema,
                           MapSharedShreddingUtils::LogicalToPhysicalSchema(arrow_logical_schema,
                                                                            field_to_num_columns));
    auto c_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*physical_schema, c_schema.get()));
    return c_schema;
}

Result<std::unique_ptr<::ArrowSchema>> MapSharedShreddingSchemaUtils::AttachMetadataToSchema(
    std::unique_ptr<::ArrowSchema> physical_schema,
    const std::map<std::string, MapSharedShreddingFieldMeta>& field_name_to_meta,
    const std::string& compression) {
    if (!physical_schema) {
        return Status::Invalid("physical schema is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_physical_schema,
                                      arrow::ImportSchema(physical_schema.get()));

    arrow::FieldVector updated_fields = arrow_physical_schema->fields();
    for (const auto& [field_name, field_meta] : field_name_to_meta) {
        int32_t field_index = arrow_physical_schema->GetFieldIndex(field_name);
        if (field_index < 0) {
            return Status::Invalid(fmt::format(
                "Shared-shredding field '{}' not found in physical schema.", field_name));
        }

        const auto& field = arrow_physical_schema->field(field_index);
        auto metadata = field->metadata() ? field->metadata()->Copy()
                                          : std::make_shared<arrow::KeyValueMetadata>();
        PAIMON_RETURN_NOT_OK(
            MapSharedShreddingUtils::SerializeMetadata(field_meta, compression, metadata.get()));
        updated_fields[field_index] = field->WithMetadata(metadata);
    }

    auto updated_schema =
        arrow::schema(std::move(updated_fields), arrow_physical_schema->metadata());
    auto c_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*updated_schema, c_schema.get()));
    return c_schema;
}

Result<MapSharedShreddingFieldMeta> MapSharedShreddingSchemaUtils::ExtractMetadataFromField(
    std::unique_ptr<::ArrowSchema> physical_schema, const std::string& field_name) {
    if (!physical_schema) {
        return Status::Invalid("physical schema is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_physical_schema,
                                      arrow::ImportSchema(physical_schema.get()));
    const auto& field = arrow_physical_schema->GetFieldByName(field_name);
    if (!field) {
        return Status::Invalid(
            fmt::format("Shared-shredding field '{}' not found in physical schema.", field_name));
    }

    auto metadata =
        field->metadata() ? field->metadata()->Copy() : std::shared_ptr<arrow::KeyValueMetadata>();
    return MapSharedShreddingUtils::DeserializeMetadata(metadata);
}

}  // namespace paimon

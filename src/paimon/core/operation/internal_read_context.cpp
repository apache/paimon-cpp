/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/operation/internal_read_context.h"

#include <algorithm>
#include <functional>
#include <optional>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog_options.h"
#include "paimon/common/catalog/catalog_context.h"
#include "paimon/common/data/variant/variant_access_utils.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/predicate/full_text_match_predicate.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/common/utils/url_utils.h"
#include "paimon/core/options/map_storage_layout.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/rest/rest_api.h"
#include "paimon/status.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace paimon {
namespace {

std::string IdentifierToFlatJson(const Identifier& identifier) {
    rapidjson::Document document(rapidjson::kObjectType);
    auto& allocator = document.GetAllocator();
    document.AddMember(rapidjson::StringRef("database"),
                       rapidjson::Value(identifier.GetDatabaseName(), allocator), allocator);
    // Keep branch and system-table suffixes in the reported object name.
    document.AddMember(rapidjson::StringRef("object"),
                       rapidjson::Value(identifier.GetObjectName(), allocator), allocator);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    document.Accept(writer);
    return buffer.GetString();
}

}  // namespace

std::shared_ptr<CatalogContext> InternalReadContext::CreateDependencyCatalogContext() const {
    const auto& catalog = read_context_->GetCatalog();
    if (!catalog) {
        // Preserve path-only filesystem reads using <warehouse>/<database>.db/<table>.
        return std::make_shared<CatalogContext>(
            PathUtil::GetParentDirPath(PathUtil::GetParentDirPath(GetPath())), options_.ToMap(),
            GetFileSystemSchemeToIdentifierMap());
    }

    // Preserve the dependency catalog's effective settings: table options, then catalog
    // options, then explicit read overrides. The upstream reader loads its own table options;
    // ReadContextBuilder must not copy these catalog settings back into that read.
    auto dependency_options = options_.ToMap();
    const auto& catalog_options = catalog->GetOptions();
    for (const auto& [key, value] : catalog_options) {
        dependency_options[key] = value;
    }
    const auto& read_options = read_context_->GetOptions();
    for (const auto& [key, value] : read_options) {
        dependency_options[key] = value;
    }
    const std::string read_via_option =
        std::string(RestApi::kHeaderOptionPrefix) + RestApi::kReadViaHeader;
    auto metastore = dependency_options.find(CatalogOptions::METASTORE);
    if (metastore != dependency_options.end() &&
        StringUtils::ToLowerCase(metastore->second) == "rest" && read_context_->GetIdentifier() &&
        dependency_options.count(read_via_option) == 0) {
        // Only fill in a missing header when the merged options select a REST dependency catalog.
        dependency_options[read_via_option] =
            UrlUtils::EncodeString(IdentifierToFlatJson(*read_context_->GetIdentifier()));
    }
    // A REST warehouse identifies the catalog, independent of the table's storage location.
    return std::make_shared<CatalogContext>(catalog->GetRootPath(), dependency_options,
                                            GetFileSystemSchemeToIdentifierMap());
}

Result<std::shared_ptr<arrow::Field>> InternalReadContext::AlignReadFieldWithTableFieldIds(
    const std::shared_ptr<arrow::Field>& read_field,
    const std::shared_ptr<arrow::Field>& table_field) {
    static const std::vector<std::string> kReadMetadataWhitelist = {DataField::MAP_SELECTED_KEYS};

    if (VariantTypeUtils::IsVariantField(table_field) &&
        VariantAccessUtils::IsVariantAccessType(read_field->type())) {
        // A variant column may be read as a variant-access projection: a struct whose children
        // each carry a `__VARIANT_METADATA` description. Keep the projection type (including
        // the children's descriptions) on the aligned field.
        return table_field->WithType(read_field->type());
    }

    if (table_field->type()->id() == arrow::Type::MAP &&
        NestedProjectionUtils::IsMapSharedShreddingAccessField(read_field)) {
        auto table_map = checked_pointer_cast<arrow::MapType>(table_field->type());
        if (table_map->key_type()->id() != arrow::Type::STRING) {
            return Status::Invalid(fmt::format(
                "Selected-key MAP pushdown only supports string MAP keys for field '{}'",
                table_field->name()));
        }
        PAIMON_RETURN_NOT_OK(
            NestedProjectionUtils::ValidateMapSharedShreddingAccessField(read_field).status());
        auto read_struct = checked_pointer_cast<arrow::StructType>(read_field->type());
        const auto& selected_value_type = read_struct->field(0)->type();
        if (!selected_value_type->Equals(table_map->item_type())) {
            return Status::Invalid(fmt::format(
                "Selected-key MAP pushdown does not support pruning MAP value fields for "
                "'{}': selected type {} vs MAP value type {}",
                table_field->name(), selected_value_type->ToString(),
                table_map->item_type()->ToString()));
        }
        auto aligned_field = table_field->WithType(read_field->type());
        return DataField::MergeFieldMetadataByWhitelist(aligned_field, read_field,
                                                        kReadMetadataWhitelist);
    }

    if (read_field->type()->id() != table_field->type()->id()) {
        return Status::Invalid(fmt::format(
            "Read schema field '{}' type {} does not match table field type {}", read_field->name(),
            read_field->type()->ToString(), table_field->type()->ToString()));
    }

    auto type_id = read_field->type()->id();
    if (type_id == arrow::Type::STRUCT) {
        auto read_struct = checked_pointer_cast<arrow::StructType>(read_field->type());
        auto table_struct = checked_pointer_cast<arrow::StructType>(table_field->type());
        arrow::FieldVector rebased_children;
        rebased_children.reserve(read_struct->num_fields());
        for (const auto& read_child : read_struct->fields()) {
            auto table_child =
                NestedProjectionUtils::FindFieldByName(table_struct->fields(), read_child->name());
            if (!table_child) {
                return Status::Invalid(fmt::format(
                    "Read schema does not support schema evolution inside struct: nested field "
                    "'{}' does not exist in table field '{}'",
                    read_child->name(), read_field->name()));
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> rebased_child,
                                   AlignReadFieldWithTableFieldIds(read_child, table_child));
            rebased_children.push_back(rebased_child);
        }
        auto rebased_type = arrow::struct_(rebased_children);
        auto aligned_field = table_field->WithType(rebased_type);
        return DataField::MergeFieldMetadataByWhitelist(aligned_field, read_field,
                                                        kReadMetadataWhitelist);
    }

    if (type_id == arrow::Type::LIST) {
        auto read_list = checked_pointer_cast<arrow::ListType>(read_field->type());
        auto table_list = checked_pointer_cast<arrow::ListType>(table_field->type());
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Field> rebased_value_field,
            AlignReadFieldWithTableFieldIds(read_list->value_field(), table_list->value_field()));
        auto rebased_type = arrow::list(rebased_value_field);
        auto aligned_field = table_field->WithType(rebased_type);
        return DataField::MergeFieldMetadataByWhitelist(aligned_field, read_field,
                                                        kReadMetadataWhitelist);
    }

    if (type_id == arrow::Type::MAP) {
        auto read_map = checked_pointer_cast<arrow::MapType>(read_field->type());
        auto table_map = checked_pointer_cast<arrow::MapType>(table_field->type());
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Field> rebased_key_field,
            AlignReadFieldWithTableFieldIds(read_map->key_field(), table_map->key_field()));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Field> rebased_item_field,
            AlignReadFieldWithTableFieldIds(read_map->item_field(), table_map->item_field()));
        auto rebased_type = arrow::map(rebased_key_field->type(), rebased_item_field);
        auto aligned_field = table_field->WithType(rebased_type);
        return DataField::MergeFieldMetadataByWhitelist(aligned_field, read_field,
                                                        kReadMetadataWhitelist);
    }

    if (!read_field->type()->Equals(table_field->type())) {
        return Status::Invalid(fmt::format(
            "Read schema field '{}' type {} does not match table field type {}", read_field->name(),
            read_field->type()->ToString(), table_field->type()->ToString()));
    }

    auto aligned_field = table_field->WithType(read_field->type());
    return DataField::MergeFieldMetadataByWhitelist(aligned_field, read_field,
                                                    kReadMetadataWhitelist);
}

std::optional<DataField> InternalReadContext::TryResolveSpecialFieldById(
    int32_t field_id, const CoreOptions& core_options, bool has_vector_or_full_text_search) {
    if (field_id == SpecialFields::ValueKind().Id()) {
        return SpecialFields::ValueKind();
    }
    if (field_id == SpecialFields::RowId().Id()) {
        if (core_options.RowTrackingEnabled()) {
            return SpecialFields::RowId();
        }
        return std::nullopt;
    }
    if (field_id == SpecialFields::SequenceNumber().Id()) {
        if (core_options.RowTrackingEnabled() || core_options.KeyValueSequenceNumberEnabled()) {
            return SpecialFields::SequenceNumber();
        }
        return std::nullopt;
    }
    if (field_id == SpecialFields::IndexScore().Id()) {
        if (core_options.DataEvolutionEnabled() || has_vector_or_full_text_search) {
            return SpecialFields::IndexScore();
        }
        return std::nullopt;
    }
    return std::nullopt;
}

std::optional<DataField> InternalReadContext::TryResolveSpecialFieldByName(
    const std::string& name, const CoreOptions& core_options, bool has_vector_or_full_text_search,
    int32_t output_field_idx) {
    if (SpecialFields::ParseIndexScoreFieldName(name)) {
        // These virtual columns have read-local negative IDs, not persistent table IDs.
        return DataField(-1 - output_field_idx, arrow::field(name, arrow::float32()));
    }
    if (name == SpecialFields::ValueKind().Name()) {
        return SpecialFields::ValueKind();
    }
    if (name == SpecialFields::RowId().Name()) {
        if (core_options.RowTrackingEnabled()) {
            return SpecialFields::RowId();
        }
        return std::nullopt;
    }
    if (name == SpecialFields::SequenceNumber().Name()) {
        if (core_options.RowTrackingEnabled() || core_options.KeyValueSequenceNumberEnabled()) {
            return SpecialFields::SequenceNumber();
        }
        return std::nullopt;
    }
    if (name == SpecialFields::IndexScore().Name()) {
        if (core_options.DataEvolutionEnabled() || has_vector_or_full_text_search) {
            return SpecialFields::IndexScore();
        }
        return std::nullopt;
    }
    return std::nullopt;
}

Result<std::unique_ptr<InternalReadContext>> InternalReadContext::Create(
    const std::shared_ptr<ReadContext>& context, const std::shared_ptr<TableSchema>& table_schema,
    const std::map<std::string, std::string>& options,
    const std::shared_ptr<FileSystem>& file_system, const std::string* path) {
    // The file system a table reads through, or null for the read context's own, which callers
    // without a table hand over.
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions core_options,
        CoreOptions::FromMap(
            options, file_system != nullptr ? file_system : context->GetSpecificFileSystem(),
            context->GetFileSystemSchemeToIdentifierMap()));
    core_options.WithCache(context->GetCache());
    if (context->HasVectorOrFullTextSearch()) {
        if (!core_options.FileIndexReadEnabled()) {
            return Status::Invalid(
                "File Index read must be enabled for VectorSearch or FullTextSearch");
        }
        if (!table_schema->PrimaryKeys().empty() && !core_options.DeletionVectorsEnabled()) {
            return Status::NotImplemented(
                "VectorSearch or FullTextSearch on primary-key tables requires deletion vectors");
        }
        if (core_options.DataEvolutionEnabled()) {
            return Status::NotImplemented(
                "VectorSearch or FullTextSearch does not support data-evolution reads yet");
        }
        if (context->GetRealtimeContext()) {
            return Status::NotImplemented(
                "VectorSearch or FullTextSearch does not support real-time reads yet");
        }
        const std::string& search_field = context->GetVectorSearch()
                                              ? context->GetVectorSearch()->field_name
                                              : context->GetFullTextSearch()->field_name;
        PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] DataField field,
                               table_schema->GetField(search_field));
    }
    // prepare read schema
    // Priority: projected_arrow_schema > read_field_ids > read_field_names
    const bool has_projected_read_schema = context->HasReadSchema();
    const bool has_vector_or_full_text_search = context->HasVectorOrFullTextSearch();
    std::vector<DataField> read_data_fields;
    if (has_projected_read_schema) {
        // Nested column pruning path: user provided a read C ArrowSchema
        // where STRUCT types may contain only a subset of sub-fields.
        // ImportSchema consumes the C schema — that's fine, it's one-shot usage.
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> read_schema,
                                          arrow::ImportSchema(context->GetReadSchema()));
        read_data_fields.reserve(read_schema->num_fields());
        // Align special-field validation with read_field_ids/read_field_names branches.
        for (const auto& read_field : read_schema->fields()) {
            if (auto resolved_special_field = TryResolveSpecialFieldByName(
                    read_field->name(), core_options, has_vector_or_full_text_search,
                    static_cast<int32_t>(read_data_fields.size()))) {
                read_data_fields.push_back(*resolved_special_field);
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(DataField table_field,
                                   table_schema->GetField(read_field->name()));
            if (NestedProjectionUtils::IsMapSharedShreddingAccessField(read_field)) {
                PAIMON_ASSIGN_OR_RAISE(MapStorageLayout layout,
                                       core_options.GetMapStorageLayout(table_field.Name()));
                if (layout != MapStorageLayout::SHARED_SHREDDING) {
                    return Status::Invalid(fmt::format(
                        "Selected-key MAP pushdown only supports top-level shared-shredding MAP "
                        "field: {}",
                        table_field.Name()));
                }
            }
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::Field> aligned_field,
                AlignReadFieldWithTableFieldIds(read_field, table_field.ArrowField()));
            read_data_fields.emplace_back(table_field.Id(), aligned_field,
                                          table_field.Description());
        }
    } else if (!context->GetReadFieldIds().empty()) {
        read_data_fields.reserve(context->GetReadFieldIds().size());
        for (const auto& field_id : context->GetReadFieldIds()) {
            if (auto resolved_special_field = TryResolveSpecialFieldById(
                    field_id, core_options, has_vector_or_full_text_search)) {
                read_data_fields.push_back(*resolved_special_field);
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(field_id));
            read_data_fields.push_back(field);
        }
    } else if (!context->GetReadFieldNames().empty()) {
        read_data_fields.reserve(context->GetReadFieldNames().size());
        for (const auto& name : context->GetReadFieldNames()) {
            if (auto resolved_special_field =
                    TryResolveSpecialFieldByName(name, core_options, has_vector_or_full_text_search,
                                                 static_cast<int32_t>(read_data_fields.size()))) {
                read_data_fields.push_back(*resolved_special_field);
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(name));
            read_data_fields.push_back(field);
        }
    } else {
        // if field names not set, read all fields
        read_data_fields = table_schema->Fields();
    }
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_data_fields);
    // validate read schema to avoid redundant fields.
    // For projected read schema, nested sub-fields may be user-requested fields
    // that do not exist in table schema, so they may not have paimon field IDs.
    if (has_projected_read_schema) {
        PAIMON_RETURN_NOT_OK(ArrowSchemaValidator::ValidateSchema(*read_schema));
    } else {
        PAIMON_RETURN_NOT_OK(ArrowSchemaValidator::ValidateSchemaWithFieldId(*read_schema));
    }
    if (context->GetFullTextSearch() &&
        read_schema->GetFieldIndex(SpecialFields::IndexScore().Name()) >= 0) {
        return Status::NotImplemented("File full-text search does not support score output yet");
    }
    // validate predicate
    if (context->GetPredicate()) {
        PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
            *read_schema, context->GetPredicate(), /*validate_field_idx=*/true));
    }
    if (!context->GetMemoryPool()) {
        return Status::Invalid("memory pool is null");
    }
    std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(context->GetMemoryPool());
    auto internal_context = std::unique_ptr<InternalReadContext>(new InternalReadContext(
        context, table_schema, read_schema, core_options, arrow_pool,
        path != nullptr ? std::optional<std::string>(*path) : std::nullopt));
    PAIMON_RETURN_NOT_OK(internal_context->BindIndexScores());
    return internal_context
}

Status InternalReadContext::BindIndexScores() {
    std::map<std::string, int32_t> projected_scores;
    for (int32_t idx = 0; idx < read_schema_->num_fields(); ++idx) {
        if (auto field_name =
                SpecialFields::ParseIndexScoreFieldName(read_schema_->field(idx)->name())) {
            projected_scores.emplace(*field_name, idx);
        }
    }
    if (projected_scores.empty()) {
        return Status::OK();
    }
    auto bind = [&](const IndexScoreSource& source, bool under_or) -> Status {
        auto iter = projected_scores.find(source.field_name);
        if (iter == projected_scores.end()) {
            return Status::OK();
        }
        if (under_or) {
            return Status::Invalid("Score-producing FullTextMatch cannot appear under OR");
        }
        if (!index_score_bindings_.emplace(iter->second, source).second) {
            return Status::Invalid(fmt::format(
                "Score-producing search field '{}' occurs more than once", source.field_name));
        }
        return Status::OK();
    };
    std::function<Status(const std::shared_ptr<Predicate>&, bool)> visit =
        [&](const std::shared_ptr<Predicate>& predicate, bool under_or) -> Status {
        if (auto full_text = std::dynamic_pointer_cast<FullTextMatchPredicate>(predicate)) {
            return bind({IndexScoreSource::Type::FULL_TEXT_MATCH, full_text->FieldName()},
                        under_or);
        }
        if (auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
            const bool child_under_or =
                under_or || compound->GetFunction().GetType() == Function::Type::OR;
            for (const auto& child : compound->Children()) {
                PAIMON_RETURN_NOT_OK(visit(child, child_under_or));
            }
        }
        return Status::OK();
    };
    PAIMON_RETURN_NOT_OK(visit(GetPredicate(), /*under_or=*/false));
    if (const auto& search = GetVectorSearch()) {
        PAIMON_RETURN_NOT_OK(
            bind({IndexScoreSource::Type::VECTOR_SEARCH, search->field_name}, /*under_or=*/false));
    }
    if (const auto& search = GetFullTextSearch()) {
        PAIMON_RETURN_NOT_OK(bind({IndexScoreSource::Type::FULL_TEXT_SEARCH, search->field_name},
                                  /*under_or=*/false));
    }
    for (const auto& [field_name, output_idx] : projected_scores) {
        if (index_score_bindings_.find(output_idx) == index_score_bindings_.end()) {
            return Status::Invalid(fmt::format("Index score field '{}' has no search source",
                                               read_schema_->field(output_idx)->name()));
        }
    }
    return Status::OK();
}

bool InternalReadContext::NeedsScores(const IndexScoreSource& source) const {
    if (source.type == IndexScoreSource::Type::VECTOR_SEARCH) {
        const auto& search = GetVectorSearch();
        return search && search->field_name == source.field_name;
    }
    if (source.type == IndexScoreSource::Type::FULL_TEXT_SEARCH) {
        const auto& search = GetFullTextSearch();
        return search && search->field_name == source.field_name;
    }
    return std::any_of(index_score_bindings_.begin(), index_score_bindings_.end(),
                       [&source](const auto& binding) { return binding.second == source; });
}

InternalReadContext::InternalReadContext(const std::shared_ptr<ReadContext>& read_context,
                                         const std::shared_ptr<TableSchema>& table_schema,
                                         const std::shared_ptr<arrow::Schema>& read_schema,
                                         const CoreOptions& options,
                                         const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                                         std::optional<std::string> path)
    : read_context_(read_context),
      table_schema_(table_schema),
      read_schema_(read_schema),
      arrow_pool_(arrow_pool),
      options_(options),
      path_(std::move(path)) {}

Result<std::shared_ptr<InternalReadContext>> InternalReadContext::CreateWithSchema(
    const std::shared_ptr<InternalReadContext>& original,
    const std::shared_ptr<arrow::Schema>& new_read_schema) {
    if (original->HasVectorOrFullTextSearch() ||
        PredicateUtils::ContainsFullTextMatch(original->GetPredicate())) {
        return Status::NotImplemented(
            "CreateWithSchema does not support search requests or index score projections");
    }
    // Create a new InternalReadContext sharing all properties except read_schema.
    // The new read_schema is the minimal column set for COUNT(*).
    return std::shared_ptr<InternalReadContext>(
        new InternalReadContext(original->read_context_, original->table_schema_, new_read_schema,
                                original->options_, original->arrow_pool_, original->path_));
}

}  // namespace paimon

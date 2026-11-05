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

#include "paimon/core/table/source/query_auth_table_read.h"

#include <cstdint>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/cast.h"
#include "fmt/format.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/reader/predicate_batch_reader.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/table/source/query_auth_split.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/defs.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/status.h"

namespace paimon {
namespace {

/// The column as a mask or a predicate reads it: dictionary-decoded, and cast to a string when it
/// is a large string.
Result<std::shared_ptr<arrow::Array>> DecodeDictionaryAndLargeString(
    const std::shared_ptr<arrow::Array>& column, arrow::MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> decoded,
                           CastingUtils::DecodeDictionary(column, pool));
    if (decoded->type_id() == arrow::Type::LARGE_STRING) {
        return CastingUtils::Cast(decoded, arrow::utf8(), arrow::compute::CastOptions::Safe(),
                                  pool);
    }
    return decoded;
}

/// The type `DecodeDictionaryAndLargeString()` gives a column of `type`.
std::shared_ptr<arrow::DataType> DecodedType(const std::shared_ptr<arrow::DataType>& type) {
    std::shared_ptr<arrow::DataType> value_type = type;
    if (value_type->id() == arrow::Type::DICTIONARY) {
        value_type = checked_cast<const arrow::DictionaryType&>(*value_type).value_type();
    }
    if (value_type->id() == arrow::Type::LARGE_STRING) {
        return arrow::utf8();
    }
    return value_type;
}

/// Hands out only the rows the row filter keeps. `PredicateBatchReader::NextBatchWithBitmap()`
/// only clears the bits of the rows it drops and leaves their values in the Arrow array, so every
/// batch is compacted through `NextBatch()`.
class RowFilterBoundaryReader : public BatchReader {
 public:
    explicit RowFilterBoundaryReader(std::unique_ptr<PredicateBatchReader> reader)
        : reader_(std::move(reader)) {}

    Result<ReadBatch> NextBatch() override {
        return reader_->NextBatch();
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        return BatchReader::NextBatchWithBitmap();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        reader_->Close();
    }

 private:
    std::unique_ptr<PredicateBatchReader> reader_;
};

}  // namespace

QueryAuthBatchReader::QueryAuthBatchReader(
    std::unique_ptr<BatchReader> reader,
    std::map<std::string, std::shared_ptr<MaskTransform>> masks,
    std::set<std::string> dropped_fields, std::set<std::string> decoded_fields,
    std::shared_ptr<arrow::MemoryPool> arrow_pool, std::shared_ptr<arrow::DataType> logical_type)
    : reader_(std::move(reader)),
      masks_(std::move(masks)),
      dropped_fields_(std::move(dropped_fields)),
      decoded_fields_(std::move(decoded_fields)),
      arrow_pool_(std::move(arrow_pool)),
      logical_type_(std::move(logical_type)) {
    decode_inputs_ = decoded_fields_;
    for (const auto& [target, mask] : masks_) {
        for (const std::string& input : mask->InputFields()) {
            decode_inputs_.insert(input);
        }
    }
}

Result<std::shared_ptr<arrow::StructArray>> QueryAuthBatchReader::DecodeInputs(
    const std::shared_ptr<arrow::StructArray>& batch) const {
    arrow::FieldVector fields = batch->struct_type()->fields();
    arrow::ArrayVector children = batch->fields();
    bool decoded_any = false;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (decode_inputs_.count(fields[i]->name()) == 0) {
            continue;
        }
        Result<std::shared_ptr<arrow::Array>> result =
            DecodeDictionaryAndLargeString(children[i], arrow_pool_.get());
        if (!result.ok()) {
            return result.status().WithMessage(
                fmt::format("Query auth read cannot decode column '{}': {}", fields[i]->name(),
                            result.status().message()));
        }
        std::shared_ptr<arrow::Array> decoded = std::move(result).value();
        if (decoded != children[i]) {
            fields[i] = fields[i]->WithType(decoded->type());
            children[i] = std::move(decoded);
            decoded_any = true;
        }
    }
    if (!decoded_any) {
        return batch;
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> decoded_batch,
                                      arrow::StructArray::Make(children, fields));
    return decoded_batch;
}

Result<BatchReader::ReadBatch> QueryAuthBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader_->NextBatch());
    if (BatchReader::IsEofBatch(batch)) {
        return batch;
    }
    auto& [c_array, c_schema] = batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    if (array->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid("a query-auth read requires struct batches");
    }
    auto struct_array = checked_pointer_cast<arrow::StructArray>(array);
    if (struct_array->null_count() != 0) {
        return Status::Invalid("a query-auth read does not support null rows in a batch");
    }
    if (logical_type_ != nullptr) {
        Result<std::shared_ptr<arrow::StructArray>> result =
            ArrowUtils::FlattenUnresolvableDictionaries(
                struct_array, logical_type_, arrow_pool_.get(),
                /*preserve_layout_recoverable_dictionaries=*/false);
        if (!result.ok()) {
            return result.status().WithMessage(
                fmt::format("Query auth read cannot decode output dictionaries: {}",
                            result.status().message()));
        }
        struct_array = std::move(result).value();
    }
    const auto& struct_type = *struct_array->struct_type();
    std::shared_ptr<arrow::StructArray> decoded = struct_array;
    if (!decode_inputs_.empty()) {
        PAIMON_ASSIGN_OR_RAISE(decoded, DecodeInputs(struct_array));
    }
    arrow::FieldVector fields;
    arrow::ArrayVector children;
    for (int32_t i = 0; i < struct_type.num_fields(); ++i) {
        const std::shared_ptr<arrow::Field>& field = struct_type.field(i);
        if (dropped_fields_.count(field->name()) > 0) {
            continue;
        }
        auto mask = masks_.find(field->name());
        if (mask == masks_.end()) {
            const arrow::StructArray& source =
                decoded_fields_.count(field->name()) > 0 ? *decoded : *struct_array;
            fields.push_back(source.struct_type()->field(i));
            children.push_back(source.field(i));
            continue;
        }
        const std::shared_ptr<arrow::DataType> target_type = DecodedType(field->type());
        Result<std::shared_ptr<arrow::Array>> result =
            mask->second->Evaluate(*decoded, target_type, arrow_pool_.get());
        if (!result.ok()) {
            return result.status().WithMessage(
                fmt::format("Query auth mask {} of column '{}' failed: {}", mask->second->Name(),
                            field->name(), result.status().message()));
        }
        std::shared_ptr<arrow::Array> masked = std::move(result).value();
        if (masked->length() != struct_array->length()) {
            return Status::Invalid(fmt::format("the mask of column '{}' produced {} rows for {}",
                                               field->name(), masked->length(),
                                               struct_array->length()));
        }
        if (!masked->type()->Equals(*target_type)) {
            return Status::Invalid(fmt::format("the mask of column '{}' produced {} for {}",
                                               field->name(), masked->type()->ToString(),
                                               target_type->ToString()));
        }
        // A mask may null a column that holds no nulls.
        fields.push_back(field->WithType(target_type)->WithNullable(true));
        children.push_back(std::move(masked));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> output,
                                      arrow::StructArray::Make(children, fields));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*output, c_array.get(), c_schema.get()));
    // The masked buffers come from `arrow_pool_`, which must outlive the batch, not this reader.
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), c_schema.get(), arrow_pool_));
    return batch;
}

Result<std::unique_ptr<TableRead>> QueryAuthTableRead::Create(
    const std::shared_ptr<ReadContext>& context, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool, InnerReadFactory create_inner) {
    if (context->HasVectorOrFullTextSearch() || context->EnablePreciseFileIndexEvaluate()) {
        return Status::NotImplemented(
            "Search is not supported on a query-auth table: the index ranks raw values, which a "
            "column mask invalidates.");
    }
    if (context->HasReadSchema()) {
        return Status::NotImplemented(
            "An explicit read schema is not supported on a query-auth table: the rules apply to "
            "whole columns; use SetReadFieldNames() or SetReadFieldIds() instead.");
    }
    if (context->GetRealtimeContext() != nullptr) {
        return Status::NotImplemented("Real-time reads are not supported on a query-auth table.");
    }
    std::optional<std::vector<std::string>> output_fields;
    if (!context->GetReadFieldIds().empty()) {
        std::vector<std::string> names;
        for (int32_t id : context->GetReadFieldIds()) {
            Result<DataField> field = table_schema->GetField(id);
            if (!field.ok()) {
                return Status::NotImplemented(fmt::format(
                    "Field {} is not a column of the table schema, which is all a query-auth read "
                    "can authorize.",
                    id));
            }
            names.push_back(field.value().Name());
        }
        output_fields = std::move(names);
    } else if (!context->GetReadFieldNames().empty()) {
        output_fields = context->GetReadFieldNames();
    }
    std::set<std::string> predicate_fields;
    if (context->GetPredicate() != nullptr) {
        PAIMON_RETURN_NOT_OK(
            PredicateUtils::GetAllNames(context->GetPredicate(), &predicate_fields));
    }
    std::vector<std::string> requested = output_fields.value_or(std::vector<std::string>());
    requested.insert(requested.end(), predicate_fields.begin(), predicate_fields.end());
    for (const std::string& name : requested) {
        if (!table_schema->GetField(name).ok()) {
            return Status::NotImplemented(fmt::format(
                "Column '{}' is not a column of the table schema, which is all a query-auth read "
                "can authorize.",
                name));
        }
    }
    std::unique_ptr<QueryAuthTableRead> read(new QueryAuthTableRead(
        context, table_schema, arrow_pool, std::move(create_inner), std::move(output_fields)));
    // Built now, so a read the table cannot serve fails here rather than at the first split.
    PAIMON_RETURN_NOT_OK(read->GetOrCreateInnerRead({}).status());
    return std::unique_ptr<TableRead>(std::move(read));
}

QueryAuthTableRead::QueryAuthTableRead(const std::shared_ptr<ReadContext>& context,
                                       const std::shared_ptr<TableSchema>& table_schema,
                                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                                       InnerReadFactory create_inner,
                                       std::optional<std::vector<std::string>> output_fields)
    : context_(context),
      table_schema_(table_schema),
      arrow_pool_(arrow_pool),
      create_inner_(std::move(create_inner)),
      output_fields_(std::move(output_fields)) {}

Result<std::shared_ptr<Predicate>> QueryAuthTableRead::PostMaskFilter(
    const TableQueryAuthResult& rules) const {
    const std::shared_ptr<Predicate>& predicate = context_->GetPredicate();
    if (predicate == nullptr || context_->EnablePredicateFilter()) {
        return predicate;
    }
    // As Java `ReadTransform`: the scan did not prune by the conjuncts on masked columns, so the
    // read evaluates them even when the caller evaluates the rest of its predicate itself.
    const std::set<std::string> masked_fields = rules.MaskedFields();
    std::vector<std::shared_ptr<Predicate>> masked_conjuncts;
    for (const std::shared_ptr<Predicate>& conjunct : PredicateUtils::SplitAnd(predicate)) {
        PAIMON_ASSIGN_OR_RAISE(bool masked,
                               PredicateUtils::ContainAnyField(conjunct, masked_fields));
        if (masked) {
            masked_conjuncts.push_back(conjunct);
        }
    }
    if (masked_conjuncts.empty()) {
        return std::shared_ptr<Predicate>();
    }
    return PredicateBuilder::And(masked_conjuncts);
}

Result<TableRead*> QueryAuthTableRead::GetOrCreateInnerRead(
    const std::vector<std::string>& extra_fields) {
    auto iter = inner_reads_.find(extra_fields);
    if (iter != inner_reads_.end()) {
        return iter->second.get();
    }
    std::vector<std::string> read_field_names = context_->GetReadFieldNames();
    std::vector<int32_t> read_field_ids = context_->GetReadFieldIds();
    if (!read_field_ids.empty()) {
        for (const std::string& name : extra_fields) {
            PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema_->GetField(name));
            read_field_ids.push_back(field.Id());
        }
    } else if (!read_field_names.empty()) {
        read_field_names.insert(read_field_names.end(), extra_fields.begin(), extra_fields.end());
    }
    // Without the predicate: file statistics and indexes hold raw values.
    auto inner_context = std::make_unique<ReadContext>(
        context_->GetPath(), context_->GetBranch(), read_field_names, read_field_ids,
        /*predicate=*/nullptr, /*vector_search=*/nullptr, /*full_text_search=*/nullptr,
        /*enable_precise_file_index_evaluate=*/false, /*enable_predicate_filter=*/false,
        context_->EnablePrefetch(), context_->EnableLateMaterializing(),
        context_->GetPrefetchBatchCount(), context_->GetPrefetchMaxParallelNum(),
        context_->EnableMultiThreadRowToBatch(), context_->GetRowToBatchThreadNumber(),
        context_->GetSpecificTableSchema(), context_->GetMemoryPool(), context_->GetExecutor(),
        context_->GetSpecificFileSystem(), context_->GetFileSystemSchemeToIdentifierMap(),
        /*realtime_context=*/nullptr, context_->GetOptions(), context_->ReadAheadCacheEnabled(),
        context_->GetCacheConfig(), context_->GetCache(), context_->GetFormatTable(),
        context_->GetWarmupLevel(), context_->GetCatalog(), context_->GetIdentifier());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> read,
                           create_inner_(std::move(inner_context)));
    TableRead* raw_read = read.get();
    inner_reads_.emplace(extra_fields, std::move(read));
    return raw_read;
}

Status QueryAuthTableRead::ValidateSplit(const QueryAuthSplit& split,
                                         const std::set<std::string>& filter_fields) const {
    if (!split.GetScope()) {
        return Status::Invalid(
            "The split does not record which columns its query auth rules were authorized for, "
            "as a split Java planned does not; refusing to read it. Plan the scan with "
            "paimon-cpp.");
    }
    const QueryAuthScope& scope = split.GetScope().value();
    if (scope.table_path.empty() || scope.branch.empty()) {
        return Status::Invalid(
            "The split does not record the table path and branch its query auth rules were "
            "authorized for. Plan the scan again.");
    }
    PAIMON_ASSIGN_OR_RAISE(bool same_location,
                           CatalogUtils::SameTableLocation(scope.table_path, context_->GetPath()));
    const std::string read_branch = BranchManager::NormalizeBranch(context_->GetBranch());
    if (!same_location || scope.branch != read_branch) {
        return Status::Invalid(fmt::format(
            "The split was authorized for table {} (branch {}), but the read targets {} (branch "
            "{}). Plan the scan for the table and branch being read.",
            scope.table_path, scope.branch, context_->GetPath(), read_branch));
    }
    const TableQueryAuthResult& rules = *split.GetAuthResult();
    const std::vector<DataField>& fields = table_schema_->Fields();
    // The field ids are checked first, so a renamed column is reported as such rather than as a
    // missing one.
    PAIMON_RETURN_NOT_OK(rules.ValidateFieldIds(scope.rule_field_ids, fields));
    PAIMON_RETURN_NOT_OK(rules.ValidateAgainstSchema(fields));
    // The columns the rules read themselves are the catalog's own and need no authorization.
    for (const DataField& field : fields) {
        const bool read = filter_fields.count(field.Name()) > 0 || !output_fields_ ||
                          ObjectUtils::Contains(output_fields_.value(), field.Name());
        if (!read) {
            continue;
        }
        auto authorized = scope.authorized_field_ids.find(field.Name());
        if (authorized == scope.authorized_field_ids.end()) {
            return Status::Invalid(
                fmt::format("Column '{}' is read, but the scan did not ask the catalog to "
                            "authorize it: name it in ScanContextBuilder::SetReadFieldNames().",
                            field.Name()));
        }
        if (authorized->second != field.Id()) {
            return Status::Invalid(fmt::format(
                "Column '{}' is read as field {}, but the scan authorized field {} under that "
                "name: the schema changed since the plan. Plan the scan again.",
                field.Name(), field.Id(), authorized->second));
        }
    }
    return Status::OK();
}

Result<std::unique_ptr<BatchReader>> QueryAuthTableRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    auto auth_split = std::dynamic_pointer_cast<QueryAuthSplit>(split);
    if (auth_split == nullptr || auth_split->GetAuthResult() == nullptr) {
        return Status::Invalid(fmt::format(
            "A split of a table with '{}=true' must be planned by a scan of the table through its "
            "catalog, which attaches the row filters and column masks it is read with.",
            Options::QUERY_AUTH_ENABLED));
    }
    const TableQueryAuthResult& rules = *auth_split->GetAuthResult();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> filter, PostMaskFilter(rules));
    std::set<std::string> filter_fields;
    if (filter != nullptr) {
        PAIMON_RETURN_NOT_OK(PredicateUtils::GetAllNames(filter, &filter_fields));
    }
    Status valid = ValidateSplit(*auth_split, filter_fields);
    if (!valid.ok()) {
        return valid.WithMessage(fmt::format("Query auth read of {} (schema {}): {}",
                                             context_->GetPath(), table_schema_->Id(),
                                             valid.message()));
    }

    if (rules.RejectsAllRows()) {
        return std::unique_ptr<BatchReader>(std::make_unique<ConcatBatchReader>(
            std::vector<std::unique_ptr<BatchReader>>(), arrow_pool_));
    }

    // The columns read only for the rules or for the predicate, dropped from the output.
    std::vector<std::string> extra_fields;
    if (output_fields_) {
        std::set<std::string> output(output_fields_->begin(), output_fields_->end());
        std::vector<std::string> visible = output_fields_.value();
        for (const std::string& field : filter_fields) {
            if (output.count(field) == 0) {
                visible.push_back(field);
            }
        }
        std::set<std::string> required = rules.RequiredFields(visible);
        required.insert(filter_fields.begin(), filter_fields.end());
        for (const DataField& field : table_schema_->Fields()) {
            if (required.count(field.Name()) > 0 && output.count(field.Name()) == 0) {
                extra_fields.push_back(field.Name());
            }
        }
    }
    PAIMON_ASSIGN_OR_RAISE(TableRead * inner, GetOrCreateInnerRead(extra_fields));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                           inner->CreateReader(auth_split->GetSplit()));
    // Wrapping it can fail, and `PredicateBatchReader::Create()` keeps the reader it refuses.
    ScopeGuard close_guard([&reader]() {
        if (reader) {
            reader->Close();
        }
    });

    // The row filter sees the raw values, the predicate of the read the masked ones. Each reads
    // its columns decoded, which `PredicateBatchReader` requires. Behind the row filter no column
    // stays dictionary-encoded, including nested children: a dictionary would still hold the
    // values of the rows it drops.
    const std::map<std::string, std::shared_ptr<MaskTransform>> no_masks;
    if (rules.RowFilter() != nullptr) {
        reader = std::make_unique<QueryAuthBatchReader>(
            std::move(reader), no_masks,
            /*dropped_fields=*/std::set<std::string>(), rules.RowFilterFields(), arrow_pool_,
            DataField::ConvertDataFieldsToArrowStructType(table_schema_->Fields()));
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<PredicateBatchReader> filtered,
            PredicateBatchReader::Create(std::move(reader), rules.RowFilter(), arrow_pool_));
        reader = std::make_unique<RowFilterBoundaryReader>(std::move(filtered));
    }
    std::set<std::string> dropped_fields(extra_fields.begin(), extra_fields.end());
    if (!rules.Masks().empty() || !filter_fields.empty() ||
        (filter == nullptr && !dropped_fields.empty())) {
        reader = std::make_unique<QueryAuthBatchReader>(
            std::move(reader), rules.Masks(),
            filter == nullptr ? dropped_fields : std::set<std::string>(), filter_fields,
            arrow_pool_);
    }
    if (filter != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<PredicateBatchReader> filtered,
            PredicateBatchReader::Create(std::move(reader), filter, arrow_pool_));
        reader = std::move(filtered);
        if (!dropped_fields.empty()) {
            reader = std::make_unique<QueryAuthBatchReader>(
                std::move(reader), no_masks, dropped_fields,
                /*decoded_fields=*/std::set<std::string>(), arrow_pool_);
        }
    }
    close_guard.Release();
    return reader;
}

Result<std::unique_ptr<BatchReader>> QueryAuthTableRead::CreateReader(
    const std::vector<std::shared_ptr<Split>>& splits) {
    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.reserve(splits.size());
    ScopeGuard cleanup_guard([&readers]() {
        for (const std::unique_ptr<BatchReader>& reader : readers) {
            if (reader) {
                reader->Close();
            }
        }
    });
    for (const std::shared_ptr<Split>& split : splits) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, CreateReader(split));
        readers.push_back(std::move(reader));
    }
    cleanup_guard.Release();
    return std::unique_ptr<BatchReader>(
        std::make_unique<ConcatBatchReader>(std::move(readers), arrow_pool_));
}

}  // namespace paimon

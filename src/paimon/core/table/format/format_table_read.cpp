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

#include "paimon/core/table/format/format_table_read.h"

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/reader/complete_row_kind_batch_reader.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/reader/data_file_reader_factory.h"
#include "paimon/common/reader/predicate_batch_reader.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/field_mapping_reader.h"
#include "paimon/core/table/format/format_data_split.h"
#include "paimon/core/table/format/format_path_validation.h"
#include "paimon/core/table/format/lazy_concat_batch_reader.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/predicate.h"

namespace paimon {

/// Readers, outermost first: CompleteRowKindBatchReader -> (PredicateBatchReader)
/// -> LazyConcatBatchReader across the split's files -> FieldMappingReader
/// -> (DelegatingPrefetchReader) -> (PrefetchFileBatchReader) -> FormatReader
///
/// The same shape the managed table path builds, minus what a format table has none of: no
/// deletion vectors, no bitmap index, no row-tracking fields and no shredding. The last three
/// readers are built by `DataFileReaderFactory`, which is where the two paths meet.
class FormatTableRead::Impl {
 public:
    std::shared_ptr<FormatTable> table;
    /// Columns the reader returns, in the order it returns them.
    std::shared_ptr<arrow::Schema> read_schema;
    /// The whole table schema: the mapping below splits the partition columns out of it.
    std::shared_ptr<arrow::Schema> data_schema;
    /// Splits the read schema into file columns and partition columns and rewrites the predicate
    /// against the file's own fields. The same builder the managed table path uses.
    std::shared_ptr<FieldMappingBuilder> field_mapping_builder;
    /// Turns a split's partition values into the `BinaryRow` a `FieldMappingReader` fills its
    /// partition columns from. Null when the table is not partitioned.
    std::shared_ptr<BinaryRowPartitionComputer> partition_computer;
    /// The predicate the returned reader applies exactly, or null when the caller filters itself.
    std::shared_ptr<Predicate> filter_predicate;
    std::shared_ptr<MemoryPool> pool;
    /// Runs the reads a prefetching reader issues ahead of the batches being asked for. Null when
    /// nothing asked for prefetch.
    std::shared_ptr<Executor> executor;
    std::string format_identifier;
    /// What a file is opened with. The same struct the managed table path fills in, read by the
    /// same component.
    DataFileReadOptions read_options;
};

FormatTableRead::FormatTableRead(std::unique_ptr<Impl> impl,
                                 const std::shared_ptr<MemoryPool>& pool)
    : TableRead(pool), impl_(std::move(impl)) {}

FormatTableRead::~FormatTableRead() = default;

Result<std::unique_ptr<FormatTableRead>> FormatTableRead::Create(
    const std::shared_ptr<FormatTable>& table,
    const std::optional<std::vector<std::string>>& projection,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Predicate>& predicate,
    bool enable_predicate_filter) {
    return CreateInternal(table, projection, pool, predicate, enable_predicate_filter,
                          /*read_context=*/nullptr);
}

Result<std::unique_ptr<FormatTableRead>> FormatTableRead::Create(
    const std::shared_ptr<FormatTable>& table, const std::shared_ptr<ReadContext>& read_context) {
    if (table == nullptr) {
        return Status::Invalid("format table read requires a table");
    }
    if (read_context == nullptr) {
        return Status::Invalid("format table read requires a read context");
    }
    if (read_context->GetRealtimeContext() != nullptr) {
        return Status::NotImplemented(
            "a format table has no real-time store to union with what is on disk");
    }
    // A projected read schema can rename a column, prune a nested one and give it metadata of its
    // own, while a format table's projection is a list of top-level names, so it is refused rather
    // than read as if it had never been given.
    if (read_context->GetReadSchema() != nullptr) {
        return Status::NotImplemented(
            "a format table read does not take a projected read schema; name the columns to read "
            "instead");
    }

    std::optional<std::vector<std::string>> projection;
    if (!read_context->GetReadFieldNames().empty()) {
        projection = read_context->GetReadFieldNames();
    } else if (!read_context->GetReadFieldIds().empty()) {
        // Resolved against the table's own schema, which is the only thing that knows the ids: a
        // file another engine wrote carries none.
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> c_schema, table->GetArrowSchema());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> table_schema,
                                          arrow::ImportSchema(c_schema.get()));
        PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> fields,
                               DataField::ConvertArrowSchemaToDataFields(table_schema));
        std::map<int32_t, std::string> name_by_id;
        for (const DataField& field : fields) {
            name_by_id.emplace(field.Id(), field.Name());
        }
        std::vector<std::string> names;
        names.reserve(read_context->GetReadFieldIds().size());
        for (int32_t field_id : read_context->GetReadFieldIds()) {
            auto iter = name_by_id.find(field_id);
            if (iter == name_by_id.end()) {
                return Status::Invalid(fmt::format("field id {} is not a column of table {}",
                                                   field_id, table->FullName()));
            }
            names.push_back(iter->second);
        }
        projection = std::move(names);
    }

    return CreateInternal(table, projection, read_context->GetMemoryPool(),
                          read_context->GetPredicate(), read_context->EnablePredicateFilter(),
                          read_context);
}

Result<std::unique_ptr<FormatTableRead>> FormatTableRead::CreateInternal(
    const std::shared_ptr<FormatTable>& table,
    const std::optional<std::vector<std::string>>& projection,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Predicate>& predicate,
    bool enable_predicate_filter, const std::shared_ptr<ReadContext>& read_context) {
    if (table == nullptr) {
        return Status::Invalid("format table read requires a table");
    }
    std::shared_ptr<MemoryPool> memory_pool = pool != nullptr ? pool : GetDefaultPool();

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> c_schema, table->GetArrowSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> table_schema,
                                      arrow::ImportSchema(c_schema.get()));

    const std::vector<std::string>& partition_keys = table->PartitionKeys();
    auto is_partition_key = [&partition_keys](const std::string& name) {
        return std::find(partition_keys.begin(), partition_keys.end(), name) !=
               partition_keys.end();
    };

    arrow::FieldVector read_fields;
    if (projection) {
        read_fields.reserve(projection->size());
        std::set<std::string> projected;
        for (const std::string& name : *projection) {
            // A read column is looked up by name, so twice has no meaning to act on.
            if (!projected.insert(name).second) {
                return Status::Invalid(fmt::format(
                    "column '{}' appears more than once in the projection, which paimon-cpp does "
                    "not allow",
                    name));
            }
            std::shared_ptr<arrow::Field> field = table_schema->GetFieldByName(name);
            if (field == nullptr) {
                return Status::Invalid(
                    fmt::format("field '{}' is not a column of table {}", name, table->FullName()));
            }
            read_fields.push_back(std::move(field));
        }
    } else {
        read_fields = table_schema->fields();
    }
    if (read_fields.empty()) {
        return Status::Invalid("format table read requires at least one column to read");
    }

    auto impl = std::make_unique<Impl>();
    impl->table = table;
    impl->read_schema = arrow::schema(read_fields);
    impl->pool = memory_pool;
    impl->format_identifier = FormatTable::FormatToString(table->GetFormat());
    // The whole schema, as the managed table path hands it over: the mapping splits the
    // partition columns out itself and asks the file only for what is left.
    impl->data_schema = table_schema;

    const bool has_non_partition_column =
        std::any_of(table_schema->fields().begin(), table_schema->fields().end(),
                    [&is_partition_key](const std::shared_ptr<arrow::Field>& field) {
                        return !is_partition_key(field->name());
                    });
    if (!has_non_partition_column) {
        return Status::Invalid(
            fmt::format("format table {} has no non-partition column, so its files hold nothing to "
                        "read",
                        table->FullName()));
    }

    if (predicate != nullptr) {
        // The same rules `InternalReadContext` applies to a managed table's predicate. The field
        // index is not among them: everything downstream resolves a field by name.
        PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
            *impl->read_schema, predicate, /*validate_field_idx=*/false));
        PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithLiterals(predicate));
        if (enable_predicate_filter) {
            impl->filter_predicate = predicate;
        }
    }

    // The builder also hands the file reader only the conjuncts naming columns the file holds.
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FieldMappingBuilder> field_mapping_builder,
        FieldMappingBuilder::Create(impl->read_schema, partition_keys, predicate));
    impl->field_mapping_builder = std::move(field_mapping_builder);

    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(table->Options(), table->GetFileSystem()));
    impl->read_options.read_batch_size = core_options.GetReadBatchSize();
    impl->read_options.adaptive_prefetch_strategy = core_options.EnableAdaptivePrefetchStrategy();
    if (read_context != nullptr) {
        // Straight from the context, as the managed table path takes them. Without a context
        // nobody asked for any of this, so a file is opened plainly.
        impl->read_options.cache = read_context->GetCache();
        impl->read_options.prefetch_enabled = read_context->EnablePrefetch();
        impl->read_options.prefetch_max_parallel_num = read_context->GetPrefetchMaxParallelNum();
        impl->read_options.prefetch_batch_count = read_context->GetPrefetchBatchCount();
        impl->read_options.read_ahead_cache_enabled = read_context->ReadAheadCacheEnabled();
        impl->read_options.cache_config = read_context->GetCacheConfig();
        impl->executor = read_context->GetExecutor();
    }
    if (!partition_keys.empty()) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
                               BinaryRowPartitionComputer::Create(
                                   partition_keys, table_schema, table->PartitionDefaultName(),
                                   core_options.LegacyPartitionNameEnabled(), memory_pool));
        impl->partition_computer = std::move(partition_computer);
    }

    return std::unique_ptr<FormatTableRead>(new FormatTableRead(std::move(impl), memory_pool));
}

Result<std::unique_ptr<BatchReader>> FormatTableRead::CreateSplitReader(
    const std::shared_ptr<Split>& split) {
    auto format_split = std::dynamic_pointer_cast<FormatDataSplit>(split);
    if (format_split == nullptr) {
        return Status::Invalid("format table read only accepts a FormatDataSplit");
    }

    // `CreateReader()` takes a `Split` the caller held on to, which may have been planned from
    // another table or before these files moved, so whether a file belongs to this table is asked
    // here rather than taken on trust.
    PAIMON_RETURN_NOT_OK(FormatPathValidation::ValidatePartitionKeys(
        impl_->table, format_split->partition, "split"));
    for (const FormatDataSplit::FileMeta& file : format_split->files) {
        PAIMON_RETURN_NOT_OK(FormatPathValidation::ValidatePathUnderLocation(
            file.file_path, impl_->table->Location(), "split"));
        // A split mixing partitions would read rows back under values they never had.
        PAIMON_RETURN_NOT_OK(FormatPathValidation::ValidateFileInPartition(
            impl_->table, file.file_path, format_split->partition, "split"));
        PAIMON_RETURN_NOT_OK(
            FormatPathValidation::ValidateFileIsVisible(impl_->table, file.file_path, "split"));
        if (file.file_size < 0) {
            return Status::Invalid(fmt::format("split gives {} a negative size", file.file_path));
        }
    }

    // The partition values in the shape a `FieldMappingReader` reads them from; a directory named
    // after the default partition name reads back as null.
    BinaryRow partition = BinaryRow::EmptyRow();
    if (impl_->partition_computer != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(partition,
                               impl_->partition_computer->ToBinaryRow(format_split->partition));
    }

    // One reader builder serves the whole split, built by the same component the managed table
    // path uses, so a format table's file is read with the cache and the read hints any other
    // data file is.
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> builder,
                           DataFileReaderFactory::CreateReaderBuilder(
                               impl_->format_identifier, impl_->table->Options(),
                               /*extra_format_options=*/{}, impl_->read_options, impl_->pool));
    std::shared_ptr<ReaderBuilder> reader_builder(std::move(builder));

    // Captured by value, so a file's reader outlives this `FormatTableRead`.
    std::shared_ptr<FormatTable> table = impl_->table;
    std::shared_ptr<arrow::Schema> data_schema = impl_->data_schema;
    std::shared_ptr<FieldMappingBuilder> field_mapping_builder = impl_->field_mapping_builder;
    std::shared_ptr<MemoryPool> pool = impl_->pool;
    std::shared_ptr<Executor> executor = impl_->executor;
    std::string format_identifier = impl_->format_identifier;
    DataFileReadOptions read_options = impl_->read_options;

    // Each file is named alongside its factory, so every failure says which file it was.
    std::vector<LazyConcatBatchReader::Source> sources;
    sources.reserve(format_split->files.size());
    for (const FormatDataSplit::FileMeta& file : format_split->files) {
        LazyConcatBatchReader::Source source;
        source.name = file.file_path;
        source.open = [table, reader_builder, data_schema, field_mapping_builder, partition, pool,
                       executor, format_identifier, read_options,
                       file]() -> Result<std::unique_ptr<BatchReader>> {
            // The same mapping for every file; built per file only because `FieldMappingReader`
            // takes ownership of it.
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldMapping> field_mapping,
                                   field_mapping_builder->CreateFieldMapping(data_schema));
            std::shared_ptr<arrow::Schema> file_read_schema =
                DataField::ConvertDataFieldsToArrowSchema(
                    field_mapping->non_partition_info.non_partition_data_schema);
            std::shared_ptr<Predicate> pushdown_predicate =
                field_mapping->non_partition_info.non_partition_filter;

            // The split's size is whatever the caller gave it and `Open` trusts what it is handed:
            // a stale length would truncate an object-store read or send it past the end, so the
            // file system is asked for the real one.
            PAIMON_ASSIGN_OR_RAISE(FileStatus status,
                                   table->GetFileSystem()->GetFileStatus(file.file_path));
            if (status.IsDir()) {
                return Status::Invalid("the split names a directory, not a data file");
            }
            if (file.file_size != status.GetLen()) {
                return Status::Invalid(fmt::format(
                    "the split says it is {} bytes but it is {}; the plan was made against a "
                    "different version of the file",
                    file.file_size, status.GetLen()));
            }
            // Opened through the same component the managed table path opens a data file with,
            // so prefetch and the read-ahead cache apply here too. The size is the one the file
            // system just reported, not the one the split claimed.
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<FileBatchReader> file_reader,
                DataFileReaderFactory::Open(format_identifier, file.file_path, status.GetLen(),
                                            reader_builder.get(), read_options,
                                            table->GetFileSystem(), executor, pool));

            ::ArrowSchema c_read_schema;
            ArrowSchemaMarkReleased(&c_read_schema);
            ScopeGuard read_schema_guard(
                [&c_read_schema]() { ArrowSchemaRelease(&c_read_schema); });
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*file_read_schema, &c_read_schema));
            PAIMON_RETURN_NOT_OK(file_reader->SetReadSchema(&c_read_schema, pushdown_predicate,
                                                            /*selection_bitmap=*/std::nullopt));

            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldMappingReader> reader,
                                   FieldMappingReader::Create(
                                       field_mapping_builder->GetReadFieldCount(),
                                       std::move(file_reader), partition, std::move(field_mapping),
                                       /*skip_map_selected_keys_filter_field_ids=*/{}, pool));
            return std::unique_ptr<BatchReader>(std::move(reader));
        };
        sources.push_back(std::move(source));
    }

    return std::make_unique<LazyConcatBatchReader>(std::move(sources), impl_->pool);
}

Result<std::unique_ptr<BatchReader>> FormatTableRead::ApplyFilterAndRowKind(
    std::unique_ptr<BatchReader>&& reader) {
    std::unique_ptr<BatchReader> result = std::move(reader);
    if (impl_->filter_predicate != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<PredicateBatchReader> filtered,
            PredicateBatchReader::Create(std::move(result), impl_->filter_predicate, impl_->pool));
        result = std::move(filtered);
    }
    // Every row is an insert, but `BatchReader::NextBatch()` still promises the leading
    // `_VALUE_KIND` field: an engine reading by field index would find its columns shifted.
    return std::make_unique<CompleteRowKindBatchReader>(std::move(result), impl_->pool);
}

Result<std::unique_ptr<BatchReader>> FormatTableRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, CreateSplitReader(split));
    return ApplyFilterAndRowKind(std::move(reader));
}

Result<std::unique_ptr<BatchReader>> FormatTableRead::CreateReader(
    const std::vector<std::shared_ptr<Split>>& splits) {
    std::vector<std::unique_ptr<BatchReader>> split_readers;
    split_readers.reserve(splits.size());
    for (const std::shared_ptr<Split>& split : splits) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, CreateSplitReader(split));
        split_readers.push_back(std::move(reader));
    }
    std::unique_ptr<BatchReader> reader =
        std::make_unique<ConcatBatchReader>(std::move(split_readers), impl_->pool);
    return ApplyFilterAndRowKind(std::move(reader));
}

}  // namespace paimon

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
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/checked_cast.h"
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

namespace {

/// Whether a `TIMESTAMP` a file of `format` stores in `file_unit` may be read as the table's
/// `table_unit`. A file's unit means something else in each format, so the rule does too:
///
/// - parquet records the unit, so the file's is the one its values are in. Only `MILLI` under a
///   `SECOND` column is rescaled, by `ParquetTimestampConverter`, which parquet's own round trip
///   needs since it has no `SECOND`. Any other pair would be relabelled rather than rescaled -
///   `us` under an `ms` type reads a thousand times too large - so it is refused;
/// - orc records none: a value is a seconds-plus-nanoseconds pair that `OrcAdapter` builds at
///   whatever precision the read schema asks for, and its type converter reports `NANO` either
///   way. Comparing that would refuse every orc `TIMESTAMP(0)`, `(3)` and `(6)`, this library's
///   own writes among them.
bool TimestampUnitIsCompatible(arrow::TimeUnit::type file_unit, arrow::TimeUnit::type table_unit,
                               FormatTable::Format format) {
    if (file_unit == table_unit) {
        return true;
    }
    switch (format) {
        case FormatTable::Format::ORC:
            return true;
        case FormatTable::Format::PARQUET:
            return file_unit == arrow::TimeUnit::MILLI && table_unit == arrow::TimeUnit::SECOND;
    }
    return false;
}

/// Whether a file storing `file_type` may be read as the table's `table_type`, for a file of
/// `format`.
///
/// The two have to describe the same values, with three exceptions, each allowed because something
/// downstream already reconciles it:
///
/// - a `TIMESTAMP` may differ in timezone, which the read relabels without touching the instant
///   the value names, and in unit, for which see `TimestampUnitIsCompatible()`;
/// - a `STRUCT` in the file may hold children the table does not name, which are read past as an
///   unnamed top-level column is. The ones it does name must come in the table's own order: a
///   nested column is read where the file stores it and nothing downstream puts it back, so a
///   struct ordered differently would return its values under the wrong names;
/// - a `LIST` or `MAP` element may be named differently, since what an engine calls one varies.
bool FileTypeMatchesTableType(const std::shared_ptr<arrow::DataType>& file_type,
                              const std::shared_ptr<arrow::DataType>& table_type,
                              FormatTable::Format format) {
    if (file_type->id() != table_type->id()) {
        return false;
    }
    switch (file_type->id()) {
        case arrow::Type::STRUCT: {
            int32_t file_index = 0;
            for (const std::shared_ptr<arrow::Field>& table_child : table_type->fields()) {
                bool found = false;
                while (file_index < file_type->num_fields()) {
                    const std::shared_ptr<arrow::Field>& file_child =
                        file_type->field(file_index++);
                    if (file_child->name() != table_child->name()) {
                        continue;
                    }
                    if (!FileTypeMatchesTableType(file_child->type(), table_child->type(),
                                                  format)) {
                        return false;
                    }
                    found = true;
                    break;
                }
                if (!found) {
                    return false;
                }
            }
            return true;
        }
        case arrow::Type::LIST:
        case arrow::Type::MAP: {
            if (file_type->num_fields() != table_type->num_fields()) {
                return false;
            }
            for (int32_t i = 0; i < file_type->num_fields(); i++) {
                if (!FileTypeMatchesTableType(file_type->field(i)->type(),
                                              table_type->field(i)->type(), format)) {
                    return false;
                }
            }
            return true;
        }
        case arrow::Type::TIMESTAMP:
            return TimestampUnitIsCompatible(
                checked_pointer_cast<arrow::TimestampType>(file_type)->unit(),
                checked_pointer_cast<arrow::TimestampType>(table_type)->unit(), format);
        default:
            return file_type->Equals(table_type);
    }
}

/// One file's columns, described as the table's own: matched by name, in the order the file holds
/// them, plus the partition columns. A column the table does not name is left out, and a column
/// the table names that this file does not hold is absent, which is how the field mapping learns
/// to read that one as null.
///
/// The file therefore decides which columns there are and in what order, and the table decides
/// what each of them is. That is what lets a file written before a column was added still be read,
/// and one whose columns sit in another order be read in the order it stores them.
///
/// Only the columns `read_file_columns` names are held to the table's schema: a partition column
/// is rebuilt from the directory whether or not the file stores one, and a column outside the
/// projection is never read, so neither has to agree with the table for this read to be sound.
///
/// Of a column that is read, two things are checked here rather than left to the format reader.
/// Its type, because the reader catches only a difference in kind - parquet reads a column at its
/// file type and only ever casts timestamps, so a `DECIMAL` of another scale would come back as
/// the file stored it. And its absence, when the table declares it `NOT NULL`, since the mapping
/// fills a missing column with nulls.
Result<std::vector<DataField>> DescribeFileColumns(
    const std::shared_ptr<arrow::Schema>& file_schema, const std::vector<DataField>& table_fields,
    const std::vector<std::string>& partition_keys, const std::set<std::string>& read_file_columns,
    FormatTable::Format format, const std::string& file_path) {
    std::map<std::string, const DataField*> table_fields_by_name;
    for (const DataField& table_field : table_fields) {
        table_fields_by_name.emplace(table_field.Name(), &table_field);
    }

    std::vector<DataField> file_fields;
    file_fields.reserve(table_fields.size());
    std::set<std::string> matched;
    for (const std::shared_ptr<arrow::Field>& file_field : file_schema->fields()) {
        auto iter = table_fields_by_name.find(file_field->name());
        if (iter == table_fields_by_name.end()) {
            // A column of the file the table does not name: another engine's, and not this
            // table's data. It is left in the file rather than read past.
            continue;
        }
        if (!matched.insert(file_field->name()).second) {
            return Status::Invalid(
                fmt::format("{} holds the column '{}' twice, so which of the two the table's "
                            "column of that name would be read from is undecided",
                            file_path, file_field->name()));
        }
        if (read_file_columns.count(file_field->name()) > 0 &&
            !FileTypeMatchesTableType(file_field->type(), iter->second->Type(), format)) {
            return Status::Invalid(fmt::format(
                "{} stores the column '{}' as {}, but the table declares it as {}: a format table "
                "reads a column as it declares it, having no schema history to convert through",
                file_path, file_field->name(), file_field->type()->ToString(),
                iter->second->Type()->ToString()));
        }
        file_fields.push_back(*iter->second);
    }
    for (const DataField& table_field : table_fields) {
        if (read_file_columns.count(table_field.Name()) == 0 ||
            matched.count(table_field.Name()) > 0) {
            continue;
        }
        if (!table_field.ArrowField()->nullable()) {
            return Status::Invalid(fmt::format(
                "{} does not hold the column '{}', which the table declares NOT NULL: a column a "
                "file leaves out is read as null, which that column cannot be",
                file_path, table_field.Name()));
        }
    }
    // A partition column is rebuilt from the directory name rather than read, so it belongs to
    // every file whether or not that file stores one of its own. Left out here, it would be read
    // back as null instead of as the value its directory spells out.
    for (const DataField& table_field : table_fields) {
        const bool is_partition_key = std::find(partition_keys.begin(), partition_keys.end(),
                                                table_field.Name()) != partition_keys.end();
        if (is_partition_key && matched.count(table_field.Name()) == 0) {
            file_fields.push_back(table_field);
        }
    }
    return file_fields;
}

}  // namespace

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
    /// The whole table schema, as the mapping describes a column: what a file's own columns are
    /// matched against, one file at a time. See `DescribeFileColumns()`.
    ///
    /// Shared rather than held by value: the callback that opens each file of a split captures
    /// it, and a copy per file would grow with files times columns. Shared rather than referenced,
    /// so that a reader still outlives the `FormatTableRead` that built it.
    std::shared_ptr<const std::vector<DataField>> table_fields;
    /// The columns this read asks a file for: what it projects, less the partition keys it
    /// rebuilds from the directory. Only these are held to the table's schema. Shared as
    /// `table_fields` is, and for the same reason.
    std::shared_ptr<const std::set<std::string>> read_file_columns;
    /// Splits the read schema into file columns and partition columns and rewrites the predicate
    /// against the file's own fields. The same builder the managed table path uses.
    std::shared_ptr<FieldMappingBuilder> field_mapping_builder;
    /// Turns a split's partition values into the `BinaryRow` a `FieldMappingReader` fills its
    /// partition columns from. Null when the table is not partitioned.
    std::shared_ptr<BinaryRowPartitionComputer> partition_computer;
    /// The predicate the returned reader applies exactly, or null when the caller filters itself.
    std::shared_ptr<Predicate> filter_predicate;
    std::shared_ptr<MemoryPool> pool;
    /// Derived from `pool` once, so a batch that outlives the reader that made it keeps it alive.
    std::shared_ptr<arrow::MemoryPool> arrow_pool;
    /// Runs the reads a prefetching reader issues ahead. Null when nothing asked for prefetch.
    std::shared_ptr<Executor> executor;
    std::string format_identifier;
    /// What a file is opened with: the same struct the managed table path fills in.
    DataFileReadOptions read_options;
};

FormatTableRead::FormatTableRead(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

FormatTableRead::~FormatTableRead() = default;

Result<std::unique_ptr<FormatTableRead>> FormatTableRead::TEST_Create(
    const std::shared_ptr<FormatTable>& table,
    const std::optional<std::vector<std::string>>& projection,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Predicate>& predicate,
    bool enable_predicate_filter) {
    // No context, so nothing asked for prefetch or a cache: a file is opened plainly.
    return CreateInternal(table, projection, pool, predicate, enable_predicate_filter,
                          DataFileReadOptions(), /*executor=*/nullptr);
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
    // own, while a format table's projection is a list of top-level names.
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

    // Read out of the context here, so `CreateInternal()` has one source for each setting.
    DataFileReadOptions read_options;
    read_options.cache = read_context->GetCache();
    read_options.prefetch_enabled = read_context->EnablePrefetch();
    read_options.prefetch_max_parallel_num = read_context->GetPrefetchMaxParallelNum();
    read_options.prefetch_batch_count = read_context->GetPrefetchBatchCount();
    read_options.read_ahead_cache_enabled = read_context->ReadAheadCacheEnabled();
    read_options.cache_config = read_context->GetCacheConfig();

    return CreateInternal(table, projection, read_context->GetMemoryPool(),
                          read_context->GetPredicate(), read_context->EnablePredicateFilter(),
                          read_options, read_context->GetExecutor());
}

Result<std::unique_ptr<FormatTableRead>> FormatTableRead::CreateInternal(
    const std::shared_ptr<FormatTable>& table,
    const std::optional<std::vector<std::string>>& projection,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Predicate>& predicate,
    bool enable_predicate_filter, const DataFileReadOptions& read_options,
    const std::shared_ptr<Executor>& executor) {
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
    impl->arrow_pool = GetArrowPool(memory_pool);
    impl->format_identifier = FormatTable::FormatToString(table->GetFormat());
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> table_fields,
                           DataField::ConvertArrowSchemaToDataFields(table_schema));
    impl->table_fields = std::make_shared<const std::vector<DataField>>(std::move(table_fields));
    std::set<std::string> read_file_columns;
    for (const std::shared_ptr<arrow::Field>& read_field : read_fields) {
        if (!is_partition_key(read_field->name())) {
            read_file_columns.insert(read_field->name());
        }
    }
    impl->read_file_columns =
        std::make_shared<const std::set<std::string>>(std::move(read_file_columns));

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
    // What the caller decided, with the table's own options filled in over the top: those three
    // fields describe the table rather than the read, so a caller cannot disagree with them.
    impl->read_options = read_options;
    impl->read_options.read_batch_size = core_options.GetReadBatchSize();
    impl->read_options.adaptive_prefetch_strategy = core_options.EnableAdaptivePrefetchStrategy();
    impl->read_options.prefetch_io_metrics_enabled = core_options.PrefetchIoMetricsEnabled();
    impl->executor = executor;
    if (!partition_keys.empty()) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
                               BinaryRowPartitionComputer::Create(
                                   partition_keys, table_schema, table->PartitionDefaultName(),
                                   core_options.LegacyPartitionNameEnabled(), memory_pool));
        impl->partition_computer = std::move(partition_computer);
    }

    return std::unique_ptr<FormatTableRead>(new FormatTableRead(std::move(impl)));
}

Result<std::unique_ptr<BatchReader>> FormatTableRead::CreateSplitReader(
    const std::shared_ptr<Split>& split) {
    auto format_split = std::dynamic_pointer_cast<FormatDataSplit>(split);
    if (format_split == nullptr) {
        return Status::Invalid("format table read only accepts a FormatDataSplit");
    }

    // A `Split` the caller held on to may have been planned from another table or before these
    // files moved, so whether a file belongs to this table is asked rather than trusted.
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

    // One reader builder serves the whole split, built by the component the managed table path
    // uses, so a format table's file is read with the cache and read hints any other data file is.
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> builder,
                           DataFileReaderFactory::CreateReaderBuilder(
                               impl_->format_identifier, impl_->table->Options(),
                               /*extra_format_options=*/{}, impl_->read_options, impl_->pool));
    std::shared_ptr<ReaderBuilder> reader_builder(std::move(builder));

    // Captured by value, so a file's reader outlives this `FormatTableRead`.
    std::shared_ptr<FormatTable> table = impl_->table;
    std::shared_ptr<const std::vector<DataField>> table_fields = impl_->table_fields;
    std::shared_ptr<const std::set<std::string>> read_file_columns = impl_->read_file_columns;
    std::shared_ptr<FieldMappingBuilder> field_mapping_builder = impl_->field_mapping_builder;
    std::shared_ptr<MemoryPool> pool = impl_->pool;
    std::shared_ptr<arrow::MemoryPool> arrow_pool = impl_->arrow_pool;
    std::shared_ptr<Executor> executor = impl_->executor;
    std::string format_identifier = impl_->format_identifier;
    DataFileReadOptions read_options = impl_->read_options;

    // Each file is named alongside its factory, so every failure says which file it was.
    std::vector<LazyConcatBatchReader::Source> sources;
    sources.reserve(format_split->files.size());
    for (const FormatDataSplit::FileMeta& file : format_split->files) {
        LazyConcatBatchReader::Source source;
        source.name = file.file_path;
        source.open = [table, reader_builder, table_fields, read_file_columns,
                       field_mapping_builder, partition, pool, arrow_pool, executor,
                       format_identifier, read_options,
                       file]() -> Result<std::unique_ptr<BatchReader>> {
            // `Open` trusts the length it is handed, and a stale one would truncate an
            // object-store read or send it past the end, so the file system is asked for it.
            //
            // TODO(SteNicholas): open straight from the size the plan carries once a split can
            // vouch for it. StarRocks and DuckDB both found this extra stat a hotspot on object
            // stores with small files, and #189 removed it from the managed table path. What is
            // missing here is a way to say a `FormatDataSplit` reached the read unedited.
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
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<FileBatchReader> file_reader,
                DataFileReaderFactory::Open(format_identifier, file.file_path, status.GetLen(),
                                            reader_builder.get(), read_options,
                                            table->GetFileSystem(), executor, pool, arrow_pool));

            // Built per file, against the columns that file holds rather than the table's. See
            // `DescribeFileColumns()`.
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> c_file_schema,
                                   file_reader->GetFileSchema());
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> file_schema,
                                              arrow::ImportSchema(c_file_schema.get()));
            PAIMON_ASSIGN_OR_RAISE(
                std::vector<DataField> file_fields,
                DescribeFileColumns(file_schema, *table_fields, table->PartitionKeys(),
                                    *read_file_columns, table->GetFormat(), file.file_path));
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldMapping> field_mapping,
                                   field_mapping_builder->CreateFieldMapping(file_fields));
            std::shared_ptr<arrow::Schema> file_read_schema =
                DataField::ConvertDataFieldsToArrowSchema(
                    field_mapping->non_partition_info.non_partition_data_schema);
            std::shared_ptr<Predicate> pushdown_predicate =
                field_mapping->non_partition_info.non_partition_filter;

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
                                       /*skip_map_selected_keys_filter_field_ids=*/{}, arrow_pool));
            return std::unique_ptr<BatchReader>(std::move(reader));
        };
        sources.push_back(std::move(source));
    }

    return std::make_unique<LazyConcatBatchReader>(std::move(sources), impl_->arrow_pool);
}

Result<std::unique_ptr<BatchReader>> FormatTableRead::ApplyFilterAndRowKind(
    std::unique_ptr<BatchReader>&& reader) {
    std::unique_ptr<BatchReader> result = std::move(reader);
    if (impl_->filter_predicate != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<PredicateBatchReader> filtered,
                               PredicateBatchReader::Create(
                                   std::move(result), impl_->filter_predicate, impl_->arrow_pool));
        result = std::move(filtered);
    }
    // Every row is an insert, but `BatchReader::NextBatch()` still promises the leading
    // `_VALUE_KIND` field: an engine reading by field index would find its columns shifted.
    return std::make_unique<CompleteRowKindBatchReader>(std::move(result), impl_->arrow_pool);
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
        std::make_unique<ConcatBatchReader>(std::move(split_readers), impl_->arrow_pool);
    return ApplyFilterAndRowKind(std::move(reader));
}

}  // namespace paimon

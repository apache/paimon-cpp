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

#include "paimon/core/table/format/format_table_write.h"

#include <algorithm>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/hadoop_compression.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/casting/cast_executor.h"
#include "paimon/core/casting/cast_executor_factory.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/table/format/format_file_naming.h"
#include "paimon/core/table/format/format_path_validation.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"

namespace paimon {

namespace {

Logger* WriteLogger() {
    static std::unique_ptr<Logger> logger = Logger::GetLogger("FormatTableWrite");
    return logger.get();
}

/// The extension a compression adds to a data file's name: a hadoop compression by its own
/// extension, anything else by the option's text.
std::string CompressionFileExtension(const std::string& compression) {
    if (compression.empty()) {
        return std::string();
    }
    std::optional<HadoopCompression::Kind> kind = HadoopCompression::FromName(compression);
    if (kind) {
        return HadoopCompression::ToFileExtension(*kind);
    }
    return compression;
}

/// Renders a partition column as the text a partition directory is named with.
Result<std::shared_ptr<arrow::StringArray>> RenderPartitionColumnAsText(
    const std::shared_ptr<arrow::Array>& column, const std::string& field_name,
    bool legacy_partition_name, arrow::MemoryPool* pool) {
    if (column->type_id() == arrow::Type::STRING) {
        return checked_pointer_cast<arrow::StringArray>(column);
    }
    std::shared_ptr<arrow::Array> source = column;
    // `partition.legacy-name` renders with the type's own `toString`, which for a DATE is the
    // day count rather than `YYYY-MM-DD`. DATE is the only partition type the two disagree on.
    // `DataConverterUtils` answers it the same way for the managed table path.
    if (legacy_partition_name && column->type_id() == arrow::Type::DATE32) {
        PAIMON_ASSIGN_OR_RAISE(
            source,
            CastingUtils::Cast(column, arrow::int32(), arrow::compute::CastOptions::Safe(), pool));
    }
    PAIMON_ASSIGN_OR_RAISE(FieldType source_type,
                           FieldTypeUtils::ConvertToFieldType(source->type()->id()));
    std::shared_ptr<CastExecutor> cast_executor =
        CastExecutorFactory::GetCastExecutorFactory()->GetCastExecutor(source_type,
                                                                       FieldType::STRING);
    if (cast_executor == nullptr) {
        return Status::NotImplemented(
            fmt::format("cannot name a partition directory after field '{}' of type {}", field_name,
                        column->type()->ToString()));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> casted,
                           cast_executor->Cast(source, arrow::utf8(), pool));
    return checked_pointer_cast<arrow::StringArray>(casted);
}

}  // namespace

/// Where one partition's files go, and the partition values that directory spells out.
struct FormatTablePartitionTarget {
    std::string directory;
    /// The partition as the table renders it, which need not be how the caller spelled it. The
    /// commit message carries these, so it cannot disagree with the directory its file sits in.
    std::map<std::string, std::string> partition;
};

/// The file currently being written for one partition.
struct FormatTableWriteFile {
    std::shared_ptr<OutputStream> out;
    std::unique_ptr<FormatWriter> writer;
    std::string temp_file_path;
    std::string file_path;
    int64_t record_count = 0;
};

class FormatTableWrite::Impl {
 public:
    /// Where a partition's files belong. Cached, since deriving it reads the values into their
    /// column types, renders them back out, escapes them and re-walks a whole path.
    Result<FormatTablePartitionTarget> GetPartitionTarget(
        const std::map<std::string, std::string>& partition);

    /// Opens a new hidden file in `directory` for the partition that directory stands for.
    Result<FormatTableWriteFile> OpenFile(const std::string& directory);

    /// Closes the file open in `directory` and records it for committing. A failure leaves no
    /// open file behind and stops the write, since the file is then neither writable nor
    /// publishable.
    Status FinishFile(const std::string& directory);

    /// Closes `file` and appends the message that publishes it. Everything that can fail lives
    /// here, so `FinishFile()` has one place to clean up after.
    Status CloseFileAndStage(FormatTableWriteFile* file,
                             const std::map<std::string, std::string>& partition);

    /// Gives up on a file that was never staged: closes what is still open and removes the temp
    /// file. Best effort, like `Abort()`, since the failure that got here is the one to report.
    void DiscardOpenFile(FormatTableWriteFile* file);

    /// Checks that every row belongs to the partition the batch declares. Partition columns are
    /// not written, so a disagreeing row would read back with the declared value and lose its
    /// own.
    Status ValidatePartitionColumns(
        const std::shared_ptr<arrow::StructArray>& batch,
        const std::vector<std::pair<std::string, std::string>>& ordered_partition);

    std::shared_ptr<FormatTable> table;
    std::shared_ptr<MemoryPool> pool;
    std::unique_ptr<arrow::MemoryPool> arrow_pool;
    /// Full table schema, used to check the incoming batch.
    std::shared_ptr<arrow::Schema> table_schema;
    std::shared_ptr<arrow::DataType> table_struct_type;
    /// Columns actually stored in the files: the table's, minus the partition ones.
    std::shared_ptr<arrow::Schema> data_schema;
    /// Index in the table schema of each data column.
    std::vector<int32_t> data_column_indexes;
    /// Index in the table schema of each partition column, in partition key order.
    std::vector<int32_t> partition_column_indexes;
    std::string format_identifier;
    std::string file_compression;
    /// From `partition.legacy-name`. It decides how a row's partition column is rendered, so the
    /// directory name and the row check both go by it.
    bool legacy_partition_name = true;
    /// Reads a partition into its column types and renders it back out, the way Java Paimon's
    /// writer does. Null when the table is not partitioned.
    std::unique_ptr<BinaryRowPartitionComputer> partition_computer;
    int64_t target_file_size = 0;
    int64_t target_file_row_num = 0;
    int32_t write_batch_size = 0;
    FormatFileNaming naming;

    /// Keyed by the partition the caller declared. See `GetPartitionTarget()`.
    std::map<std::map<std::string, std::string>, FormatTablePartitionTarget> partition_targets;
    /// Open file per partition, keyed by the partition's directory.
    std::map<std::string, FormatTableWriteFile> open_files;
    /// Partition values of each open file, by the same key.
    std::map<std::string, std::map<std::string, std::string>> open_partitions;
    /// Written and closed but not yet published. Kept after `PrepareCommit()` hands out a copy,
    /// so that an `Abort()` still knows what to remove.
    std::vector<FormatCommitMessage> staged_messages;
    bool prepared = false;
    bool aborted = false;
    /// The failure that closing a file stopped at, or OK. It ends the write: the rows of that
    /// file cannot be published, so publishing the others would quietly lose them.
    Status finish_failure;

    /// Why this write will take no more rows, or null while it still will. Prepared and aborted
    /// call for different work from the caller, so the refusal names which one it is.
    const char* FinishedReason() const {
        if (aborted) {
            return "format table write has been aborted";
        }
        if (prepared) {
            return "format table write has already prepared its commit";
        }
        return nullptr;
    }
};

FormatTableWrite::FormatTableWrite(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

FormatTableWrite::~FormatTableWrite() {
    if (impl_ != nullptr && impl_->FinishedReason() == nullptr) {
        // `Abort()` logs its cleanup failures and returns OK today; the status is still checked.
        Status status = Abort();
        if (!status.ok()) {
            PAIMON_LOG_WARN(WriteLogger(), "Failed to abort an abandoned write of table %s: %s",
                            impl_->table->FullName().c_str(), status.ToString().c_str());
        }
    }
}

Result<std::unique_ptr<FormatTableWrite>> FormatTableWrite::Create(
    const std::shared_ptr<FormatTable>& table, const std::shared_ptr<MemoryPool>& pool) {
    if (table == nullptr) {
        return Status::Invalid("format table write requires a table");
    }
    auto impl = std::make_unique<Impl>();
    impl->table = table;
    impl->pool = pool != nullptr ? pool : GetDefaultPool();

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> c_schema, table->GetArrowSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(impl->table_schema, arrow::ImportSchema(c_schema.get()));
    impl->table_struct_type = arrow::struct_(impl->table_schema->fields());

    const std::vector<std::string>& partition_keys = table->PartitionKeys();
    arrow::FieldVector data_fields;
    for (int32_t i = 0; i < impl->table_schema->num_fields(); i++) {
        const std::shared_ptr<arrow::Field>& field = impl->table_schema->field(i);
        if (std::find(partition_keys.begin(), partition_keys.end(), field->name()) ==
            partition_keys.end()) {
            data_fields.push_back(field);
            impl->data_column_indexes.push_back(i);
        }
    }
    if (data_fields.empty()) {
        return Status::Invalid(fmt::format(
            "format table {} has no non-partition column, so its files would hold nothing",
            table->FullName()));
    }
    impl->data_schema = arrow::schema(data_fields);

    // In partition key order, which is the order the directories nest in.
    for (const std::string& partition_key : partition_keys) {
        int32_t index = impl->table_schema->GetFieldIndex(partition_key);
        if (index < 0) {
            return Status::Invalid(fmt::format("partition field '{}' is not a column of table {}",
                                               partition_key, table->FullName()));
        }
        impl->partition_column_indexes.push_back(index);
    }

    impl->format_identifier = FormatTable::FormatToString(table->GetFormat());
    impl->file_compression = table->FileCompression();

    // Through `CoreOptions`, so `"256 mb"` means what it does elsewhere and a default lives in
    // one place.
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(table->Options(), table->GetFileSystem()));

    // parquet and orc record their compression inside the file, so the name keeps the plain
    // `.parquet` unless `file.suffix.include.compression` asks for it, and then the compression
    // goes in front: `data-<uuid>-0.snappy.parquet`.
    const std::string compression_extension = CompressionFileExtension(impl->file_compression);
    std::string extension = impl->format_identifier;
    if (!compression_extension.empty() && core_options.FileSuffixIncludeCompression()) {
        extension = compression_extension + "." + extension;
    }

    impl->legacy_partition_name = core_options.LegacyPartitionNameEnabled();
    if (!partition_keys.empty()) {
        PAIMON_ASSIGN_OR_RAISE(
            impl->partition_computer,
            BinaryRowPartitionComputer::Create(partition_keys, impl->table_schema,
                                               table->PartitionDefaultName(),
                                               impl->legacy_partition_name, impl->pool));
    }
    // A format table has no primary keys, so its target file size is the append-table default.
    impl->target_file_size = core_options.GetTargetFileSize(/*has_primary_key=*/false);
    impl->target_file_row_num = core_options.GetTargetFileRowNum();
    impl->write_batch_size = core_options.GetWriteBatchSize();
    impl->arrow_pool = GetArrowPool(impl->pool);
    PAIMON_ASSIGN_OR_RAISE(impl->naming,
                           FormatFileNaming::Create(extension, core_options.DataFilePrefix()));

    return std::unique_ptr<FormatTableWrite>(new FormatTableWrite(std::move(impl)));
}

Result<FormatTablePartitionTarget> FormatTableWrite::Impl::GetPartitionTarget(
    const std::map<std::string, std::string>& partition) {
    auto iter = partition_targets.find(partition);
    if (iter != partition_targets.end()) {
        return iter->second;
    }
    FormatTablePartitionTarget target;
    target.partition = partition;
    if (partition_computer != nullptr) {
        // The round trip Java Paimon's writer makes when it renders a partition out of a row: the
        // values are read into their column types and rendered back, so the directory is named the
        // way the table's options say rather than the way the caller spelled the value.
        PAIMON_ASSIGN_OR_RAISE(BinaryRow row, partition_computer->ToBinaryRow(partition));
        // Aliased, or the comma inside the type would read as a second macro argument.
        using RenderedPartition = std::vector<std::pair<std::string, std::string>>;
        PAIMON_ASSIGN_OR_RAISE(RenderedPartition rendered,
                               partition_computer->GeneratePartitionVector(row));
        target.partition.clear();
        for (auto& [key, value] : rendered) {
            target.partition.emplace(std::move(key), std::move(value));
        }
    }
    PAIMON_ASSIGN_OR_RAISE(target.directory,
                           FormatPathValidation::BuildPartitionDirectory(table, target.partition));
    partition_targets.emplace(partition, target);
    return target;
}

Status FormatTableWrite::Impl::ValidatePartitionColumns(
    const std::shared_ptr<arrow::StructArray>& batch,
    const std::vector<std::pair<std::string, std::string>>& ordered_partition) {
    // A null, an empty string and a whitespace-only string alike stand for the default partition
    // name, so all three land in the same directory.
    const std::string& default_partition_name = table->PartitionDefaultName();
    for (size_t i = 0; i < ordered_partition.size(); i++) {
        const std::string& partition_key = ordered_partition[i].first;
        const std::string& declared_value = ordered_partition[i].second;
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::StringArray> text_column,
            RenderPartitionColumnAsText(batch->field(partition_column_indexes[i]), partition_key,
                                        legacy_partition_name, arrow_pool.get()));
        // False for every partition the table rendered, since `GeneratePartitionVector()` has
        // already replaced a null or blank value with the default partition name. Kept so that a
        // value reaching here without that round trip cannot take the fast path below, where a
        // blank has to compare equal to the default partition name rather than to itself.
        const bool declared_is_blank = StringUtils::IsNullOrWhitespaceOnly(declared_value);
        for (int64_t row = 0; row < text_column->length(); row++) {
            const bool is_null = text_column->IsNull(row);
            const std::string_view rendered =
                is_null ? std::string_view() : text_column->GetView(row);
            // The ordinary case: a row that renders exactly as the batch declared.
            if (!is_null && !declared_is_blank && rendered == declared_value) {
                continue;
            }
            const std::string_view row_value = StringUtils::IsNullOrWhitespaceOnly(rendered)
                                                   ? std::string_view(default_partition_name)
                                                   : rendered;
            if (row_value == declared_value) {
                continue;
            }
            return Status::Invalid(fmt::format(
                "row {} of the batch has '{}' in partition column '{}', but the batch declares "
                "partition '{}={}'. The partition columns are not written to the file, so this "
                "row would be stored under a partition it does not belong to and read back "
                "with the declared value.",
                row, row_value, partition_key, partition_key, declared_value));
        }
    }
    return Status::OK();
}

Result<FormatTableWriteFile> FormatTableWrite::Impl::OpenFile(const std::string& directory) {
    FormatTableWriteFile file;
    file.file_path = PathUtil::JoinPath(directory, naming.NextFileName());
    PAIMON_ASSIGN_OR_RAISE(std::string temp_relative_path, naming.NextTempFilePath());
    file.temp_file_path = PathUtil::JoinPath(directory, temp_relative_path);

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileFormat> file_format,
                           FileFormatFactory::Get(format_identifier, table->Options()));
    ::ArrowSchema c_schema;
    ArrowSchemaMarkReleased(&c_schema);
    ScopeGuard schema_guard([&c_schema]() { ArrowSchemaRelease(&c_schema); });
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*data_schema, &c_schema));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriterBuilder> writer_builder,
                           file_format->CreateWriterBuilder(&c_schema, write_batch_size));
    writer_builder->WithMemoryPool(pool);

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<OutputStream> out,
        table->GetFileSystem()->Create(file.temp_file_path, /*overwrite=*/false));
    file.out = std::move(out);
    // From here the temp file exists on disk and only this guard knows about it, so a failure
    // below has to remove it or it is left behind for good.
    std::shared_ptr<FileSystem> file_system = table->GetFileSystem();
    ScopeGuard temp_file_guard([&file, &file_system]() {
        // The stream is closed before the file goes: a stream dropped without `Close()` may still
        // flush, and on an object store that write would land after the delete.
        if (file.out != nullptr) {
            Status closed = file.out->Close();
            if (!closed.ok()) {
                PAIMON_LOG_WARN(WriteLogger(),
                                "Failed to close %s after its writer could not be opened: %s",
                                file.temp_file_path.c_str(), closed.ToString().c_str());
            }
            file.out.reset();
        }
        Status status = file_system->Delete(file.temp_file_path, /*recursive=*/false);
        if (!status.ok()) {
            PAIMON_LOG_WARN(WriteLogger(),
                            "Failed to remove the temp file %s after its writer "
                            "could not be opened: %s",
                            file.temp_file_path.c_str(), status.ToString().c_str());
        }
    });
    PAIMON_ASSIGN_OR_RAISE(file.writer, writer_builder->Build(file.out, file_compression));
    temp_file_guard.Release();
    return file;
}

Status FormatTableWrite::Impl::FinishFile(const std::string& directory) {
    auto file_iter = open_files.find(directory);
    if (file_iter == open_files.end()) {
        return Status::OK();
    }
    // The file stays in `open_files` until it is recorded for committing: dropped earlier, a
    // failure below would leave a temp path no `Abort()` knows about.
    FormatTableWriteFile& file = file_iter->second;
    // The partition is written and erased together with the file, so this cannot miss; should
    // that ever break, it must not quietly commit a file under no partition.
    auto partition_iter = open_partitions.find(directory);
    if (partition_iter == open_partitions.end()) {
        return Status::Invalid(
            fmt::format("no partition was recorded for the file open in {}", directory));
    }

    Status status = CloseFileAndStage(&file, partition_iter->second);
    if (!status.ok()) {
        // Closing gets this far only once the writer is finished and gone, so there is nothing
        // left to write into and nothing whole to publish. What is on disk is discarded and the
        // write stops here: leaving the entry in `open_files` would have the next `Write()` or
        // `PrepareCommit()` reach through a writer that is no longer there.
        DiscardOpenFile(&file);
        open_files.erase(file_iter);
        open_partitions.erase(partition_iter);
        finish_failure = status;
        return status;
    }
    open_files.erase(file_iter);
    open_partitions.erase(partition_iter);
    return Status::OK();
}

Status FormatTableWrite::Impl::CloseFileAndStage(
    FormatTableWriteFile* file, const std::map<std::string, std::string>& partition) {
    PAIMON_RETURN_NOT_OK(file->writer->Flush());
    PAIMON_RETURN_NOT_OK(file->writer->Finish());
    file->writer.reset();
    // The size is read before closing, while the stream still knows how far it wrote.
    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, file->out->GetPos());
    PAIMON_RETURN_NOT_OK(file->out->Flush());
    PAIMON_RETURN_NOT_OK(file->out->Close());
    file->out.reset();

    PAIMON_LOG_DEBUG(WriteLogger(), "Staged %s for %s, %ld rows, %ld bytes",
                     file->temp_file_path.c_str(), file->file_path.c_str(), file->record_count,
                     file_size);
    staged_messages.emplace_back(file->temp_file_path, file->file_path, partition,
                                 file->record_count, file_size);
    return Status::OK();
}

void FormatTableWrite::Impl::DiscardOpenFile(FormatTableWriteFile* file) {
    std::shared_ptr<FileSystem> file_system = table->GetFileSystem();
    if (file->writer != nullptr) {
        Status status = file->writer->Finish();
        if (!status.ok()) {
            PAIMON_LOG_WARN(WriteLogger(), "Failed to finish the writer of %s while discarding: %s",
                            file->temp_file_path.c_str(), status.ToString().c_str());
        }
        file->writer.reset();
    }
    // The stream is closed before the file goes: a stream dropped without `Close()` may still
    // flush, and on an object store that write would land after the delete.
    if (file->out != nullptr) {
        Status status = file->out->Close();
        if (!status.ok()) {
            PAIMON_LOG_WARN(WriteLogger(), "Failed to close %s while discarding: %s",
                            file->temp_file_path.c_str(), status.ToString().c_str());
        }
        file->out.reset();
    }
    Status status = file_system->Delete(file->temp_file_path, /*recursive=*/false);
    if (!status.ok() && !status.IsNotExist()) {
        PAIMON_LOG_WARN(WriteLogger(), "Failed to remove the temp file %s while discarding: %s",
                        file->temp_file_path.c_str(), status.ToString().c_str());
    }
}

Status FormatTableWrite::Write(std::unique_ptr<RecordBatch>&& batch) {
    if (const char* finished = impl_->FinishedReason(); finished != nullptr) {
        return Status::Invalid(finished);
    }
    // A file that could not be closed ends the write: see `Impl::finish_failure`.
    PAIMON_RETURN_NOT_OK(impl_->finish_failure);
    if (batch == nullptr || batch->GetData() == nullptr) {
        return Status::Invalid("format table write requires a batch");
    }
    for (RecordBatch::RowKind row_kind : batch->GetRowKind()) {
        if (row_kind != RecordBatch::RowKind::INSERT) {
            return Status::Invalid(
                "format table only supports INSERT rows: a directory of data files records no "
                "row identity for an update or a delete to apply to");
        }
    }

    // A partial partition would not name a single directory.
    const std::map<std::string, std::string>& partition = batch->GetPartition();
    const std::vector<std::string>& partition_keys = impl_->table->PartitionKeys();
    if (partition.size() != partition_keys.size()) {
        return Status::Invalid(fmt::format(
            "batch carries {} partition values but table {} is partitioned by {} fields",
            partition.size(), impl_->table->FullName(), partition_keys.size()));
    }
    for (const std::string& partition_key : partition_keys) {
        if (partition.find(partition_key) == partition.end()) {
            return Status::Invalid(fmt::format(
                "batch does not carry a value for partition field '{}'", partition_key));
        }
    }

    // `year=2025/month=01/`, or `2025/01/` under `format-table.partition-path-only-value`. The
    // scan reads back whichever is written here, so both go through one place.
    PAIMON_ASSIGN_OR_RAISE(FormatTablePartitionTarget target, impl_->GetPartitionTarget(partition));
    const std::string& directory = target.directory;
    // In the partition the table rendered rather than the one the caller spelled, since a value
    // can be written more than one way.
    std::vector<std::pair<std::string, std::string>> ordered_partition;
    ordered_partition.reserve(partition_keys.size());
    for (const std::string& partition_key : partition_keys) {
        auto iter = target.partition.find(partition_key);
        if (iter == target.partition.end()) {
            return Status::Invalid(
                fmt::format("partition field '{}' is missing from the partition the table rendered",
                            partition_key));
        }
        ordered_partition.emplace_back(partition_key, iter->second);
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> array,
        arrow::ImportArray(batch->GetData(), impl_->table_struct_type));
    PAIMON_RETURN_NOT_OK(ArrowUtils::CheckNullabilityMatch(impl_->table_schema, array));
    auto table_struct = checked_pointer_cast<arrow::StructArray>(array);
    if (table_struct->null_count() != 0) {
        return Status::Invalid(
            "format table write does not support a null row: a row of the table must have a value "
            "for every column, even if that value is null");
    }

    PAIMON_RETURN_NOT_OK(impl_->ValidatePartitionColumns(table_struct, ordered_partition));

    // Partition columns are not written: the directory holds those values.
    arrow::ArrayVector data_columns;
    data_columns.reserve(impl_->data_column_indexes.size());
    for (int32_t index : impl_->data_column_indexes) {
        data_columns.push_back(table_struct->field(index));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> data_struct,
        arrow::StructArray::Make(data_columns, impl_->data_schema->fields()));

    // A write that never receives a row leaves the directory as it found it.
    if (data_struct->length() == 0) {
        return Status::OK();
    }

    auto file_iter = impl_->open_files.find(directory);
    if (file_iter == impl_->open_files.end()) {
        PAIMON_ASSIGN_OR_RAISE(FormatTableWriteFile file, impl_->OpenFile(directory));
        file_iter = impl_->open_files.emplace(directory, std::move(file)).first;
        impl_->open_partitions[directory] = target.partition;
    }

    ArrowArray c_data_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*data_struct, &c_data_array));
    PAIMON_RETURN_NOT_OK(file_iter->second.writer->AddBatch(&c_data_array));
    file_iter->second.record_count += data_struct->length();

    // Checked between batches, so a file may pass either target by up to one batch. The row count
    // comes first, since it costs the writer nothing.
    bool reached = file_iter->second.record_count >= impl_->target_file_row_num;
    if (!reached) {
        PAIMON_ASSIGN_OR_RAISE(reached, file_iter->second.writer->ReachTargetSize(
                                            /*suggested_check=*/true, impl_->target_file_size));
    }
    if (reached) {
        PAIMON_RETURN_NOT_OK(impl_->FinishFile(directory));
    }
    return Status::OK();
}

Result<std::vector<FormatCommitMessage>> FormatTableWrite::PrepareCommit() {
    if (const char* finished = impl_->FinishedReason(); finished != nullptr) {
        return Status::Invalid(finished);
    }
    // A file that could not be closed ends the write: see `Impl::finish_failure`.
    PAIMON_RETURN_NOT_OK(impl_->finish_failure);
    std::vector<std::string> directories;
    directories.reserve(impl_->open_files.size());
    for (const auto& open_file : impl_->open_files) {
        directories.push_back(open_file.first);
    }
    for (const std::string& directory : directories) {
        Status status = impl_->FinishFile(directory);
        if (!status.ok()) {
            // Whatever was already closed is unpublished too, so nothing of this write survives.
            Status abort_status = Abort();
            if (!abort_status.ok()) {
                PAIMON_LOG_WARN(WriteLogger(),
                                "Failed to abort table %s after preparing its commit failed: %s",
                                impl_->table->FullName().c_str(), abort_status.ToString().c_str());
            }
            return status;
        }
    }
    impl_->prepared = true;
    // A copy, not a move: `Abort()` must still know what to remove afterwards.
    return impl_->staged_messages;
}

Status FormatTableWrite::Abort() {
    std::shared_ptr<FileSystem> file_system = impl_->table->GetFileSystem();
    // Best effort: a file that cannot be removed stays behind, hidden, rather than failing the
    // abort and hiding whatever caused it.
    for (auto& open_file : impl_->open_files) {
        impl_->DiscardOpenFile(&open_file.second);
    }
    impl_->open_files.clear();
    impl_->open_partitions.clear();
    for (const FormatCommitMessage& message : impl_->staged_messages) {
        Status status = file_system->Delete(message.temp_file_path, /*recursive=*/false);
        // Already gone is the ordinary case once the files have been committed, or aborted once.
        if (!status.ok() && !status.IsNotExist()) {
            PAIMON_LOG_WARN(WriteLogger(), "Failed to remove the staged file %s while aborting: %s",
                            message.temp_file_path.c_str(), status.ToString().c_str());
        }
    }
    impl_->staged_messages.clear();
    // Kept apart from `prepared`, so a later call reports aborted rather than committed.
    impl_->aborted = true;
    return Status::OK();
}

}  // namespace paimon

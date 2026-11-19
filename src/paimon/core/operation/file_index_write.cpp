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

#include "paimon/file_index/file_index_write.h"

#include <algorithm>
#include <cstddef>
#include <set>
#include <unordered_map>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/file_index/file_index_format_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"

namespace paimon {
namespace {

// Field name -> index type -> payload writer; an empty writer denotes an empty index.
using IndexPayloads =
    std::map<std::string, std::map<std::string, FileIndexFormat::Writer::PayloadWriter>>;

struct BucketUpdates {
    BinaryRow partition;
    int32_t bucket;
    int32_t total_buckets;
    std::vector<std::shared_ptr<DataFileMeta>> before;
    std::vector<std::shared_ptr<DataFileMeta>> after;
};

FileIndexFormat::Writer::PayloadWriter CopyPayload(const std::shared_ptr<InputStream>& input,
                                                   int64_t offset, int64_t length,
                                                   const std::shared_ptr<MemoryPool>& pool) {
    return [input, offset, length, pool](OutputStream* output) -> Result<bool> {
        if (length == 0) {
            return true;
        }
        constexpr int64_t kCopyBufferSize = 1024 * 1024;
        auto buffer = Bytes::AllocateBytes(std::min(length, kCopyBufferSize), pool.get());
        PAIMON_RETURN_NOT_OK(input->Seek(offset, FS_SEEK_SET));
        int64_t remaining = length;
        while (remaining > 0) {
            int64_t size = std::min(remaining, kCopyBufferSize);
            PAIMON_ASSIGN_OR_RAISE(int64_t read, input->Read(buffer->data(), size));
            if (read <= 0 || read > size) {
                return Status::IOError("Could not read the complete file index payload");
            }
            PAIMON_ASSIGN_OR_RAISE(int64_t written, output->Write(buffer->data(), read));
            if (written != read) {
                return Status::IOError("Could not write the complete file index payload");
            }
            remaining -= read;
        }
        return true;
    };
}

Result<IndexPayloads> ReadExistingIndexes(const std::shared_ptr<DataFileMeta>& file,
                                          const std::optional<std::string>& source_path,
                                          const std::shared_ptr<FileSystem>& fs,
                                          const std::shared_ptr<MemoryPool>& pool,
                                          std::shared_ptr<InputStream>* source) {
    if (file->embedded_index) {
        *source = std::make_shared<ByteArrayInputStream>(file->embedded_index->data(),
                                                         file->embedded_index->size());
    } else if (source_path) {
        PAIMON_ASSIGN_OR_RAISE(*source, fs->Open(*source_path));
    } else {
        return IndexPayloads();
    }
    const auto& input = *source;
    PAIMON_ASSIGN_OR_RAISE(FileIndexFormatUtils::FileIndexEntries entries,
                           FileIndexFormatUtils::ReadIndexEntries(input, pool));
    PAIMON_ASSIGN_OR_RAISE(int64_t file_length, input->Length());
    IndexPayloads payloads;
    for (const auto& [column, indexes] : entries) {
        for (const auto& [type, range] : indexes) {
            const auto& [offset, length] = range;
            if (offset == FileIndexFormat::EMPTY_INDEX_FLAG && length == 0) {
                payloads[column][type] = {};
            } else {
                if (offset < 0 || length < 0 || offset > file_length ||
                    length > file_length - offset) {
                    return Status::Invalid("Invalid existing file index payload range");
                }
                payloads[column][type] = CopyPayload(input, offset, length, pool);
            }
        }
    }
    return payloads;
}

Result<std::shared_ptr<DataFileMeta>> WriteFileIndexes(
    const DataFileIndexInput& request, const std::shared_ptr<DataFileMeta>& file,
    const std::shared_ptr<DataFilePathFactory>& paths, int32_t format_version,
    const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<MemoryPool>& pool,
    std::vector<std::string>* generated) {
    std::optional<std::string> source_path;
    std::vector<std::optional<std::string>> extras;
    for (const auto& extra : file->extra_files) {
        if (extra && StringUtils::EndsWith(*extra, DataFilePathFactory::INDEX_PATH_SUFFIX)) {
            if (source_path) {
                return Status::Invalid("Found more than one index file for one data file");
            }
            source_path = paths->ToAlignedPath(*extra, file);
        } else {
            extras.push_back(extra);
        }
    }
    std::shared_ptr<InputStream> source;
    ScopeGuard close_source([&source]() {
        if (source) {
            [[maybe_unused]] Status ignored = source->Close();
        }
    });
    PAIMON_ASSIGN_OR_RAISE(IndexPayloads payloads,
                           ReadExistingIndexes(file, source_path, fs, pool, &source));
    for (const auto& index : request.indexes) {
        if (!index.input) {
            payloads[index.field_name][index.index_type] = {};
        } else {
            PAIMON_ASSIGN_OR_RAISE(int64_t length, index.input->Length());
            if (length <= 0) {
                return Status::Invalid("A non-null file index input must have positive length");
            }
            payloads[index.field_name][index.index_type] =
                CopyPayload(index.input, 0, length, pool);
        }
    }
    std::string index_path = source_path
                                 ? DataFilePathFactory::CreateNewFileIndexFilePath(*source_path)
                                 : paths->ToFileIndexPath(paths->ToPath(file));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> output,
                           fs->Create(index_path, /*overwrite=*/false));
    generated->push_back(index_path);
    ScopeGuard close_output([&output]() { [[maybe_unused]] Status ignored = output->Close(); });
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexFormat::Writer> writer,
                           FileIndexFormat::CreateWriter(output, format_version, pool));
    for (const auto& [column, indexes] : payloads) {
        for (const auto& [type, payload] : indexes) {
            PAIMON_RETURN_NOT_OK(writer->WriteIndex(column, type, payload));
        }
    }
    PAIMON_RETURN_NOT_OK(writer->Finish());
    PAIMON_RETURN_NOT_OK(writer->Close());
    close_output.Release();
    extras.emplace_back(PathUtil::GetName(index_path));
    auto indexed_file = file->CopyWithExtraFiles(extras);
    indexed_file->embedded_index.reset();
    return indexed_file;
}

Result<std::map<std::string, ManifestEntry>> FindFiles(
    const FileStore& store, const std::shared_ptr<TableSchema>& schema,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<SnapshotManager>& snapshot_manager, const Snapshot& snapshot,
    const std::shared_ptr<FileStorePathFactory>& paths,
    const std::map<std::string, const DataFileIndexInput*>& requests,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> partition_schema,
                           FieldMapping::GetPartitionSchema(arrow_schema, schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ManifestList> manifest_list,
                           store.NewManifestList(paths, pool));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ManifestFile> manifest_file,
                           store.NewManifestFile(paths, partition_schema, pool));
    auto filter = std::make_shared<ScanFilter>(
        nullptr, std::vector<std::map<std::string, std::string>>{}, std::nullopt);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<AppendOnlyFileStoreScan> scan,
                           AppendOnlyFileStoreScan::Create(
                               snapshot_manager, schema_manager, manifest_list, manifest_file,
                               schema, arrow_schema, filter, store.GetOptions(), executor, pool));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> plan,
                           scan->WithSnapshot(snapshot)->CreatePlan());
    std::map<std::string, ManifestEntry> files;
    for (const auto& entry : plan->Files()) {
        if (requests.count(entry.FileName()) == 0) {
            continue;
        }
        if (!files.emplace(entry.FileName(), entry).second) {
            return Status::Invalid(
                fmt::format("Ambiguous data file name in the base snapshot: {}", entry.FileName()));
        }
        if (entry.Bucket() < 0 || entry.TotalBuckets() <= 0 ||
            entry.Bucket() >= entry.TotalBuckets()) {
            return Status::Invalid("FileIndexWrite requires data files in fixed buckets");
        }
    }
    for (const auto& request : requests) {
        if (files.count(request.first) == 0) {
            return Status::Invalid(fmt::format(
                "File index target is not live in the base snapshot: {}", request.first));
        }
    }
    return files;
}

}  // namespace

Result<std::vector<std::shared_ptr<CommitMessage>>> FileIndexWrite::PrepareCommit(
    const std::string& table_path, const std::string& branch,
    const std::map<std::string, std::string>& options,
    const std::optional<int64_t>& base_snapshot_id, const std::vector<DataFileIndexInput>& files,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<Executor>& executor) {
    if (table_path.empty()) {
        return Status::Invalid("FileIndexWrite requires a table path");
    }
    if (base_snapshot_id && *base_snapshot_id < Snapshot::FIRST_SNAPSHOT_ID) {
        return Status::Invalid("Invalid base snapshot id");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::string resolved_branch,
        BranchManager::ResolveBranch(std::nullopt, options, branch, "FileIndexWrite"));
    std::shared_ptr<FileSystem> fs = file_system;
    if (!fs) {
        PAIMON_ASSIGN_OR_RAISE(CoreOptions initial_options, CoreOptions::FromMap(options));
        fs = initial_options.GetFileSystem();
    }
    auto snapshot_manager = std::make_shared<SnapshotManager>(fs, table_path, resolved_branch);
    std::optional<Snapshot> snapshot;
    if (base_snapshot_id) {
        PAIMON_ASSIGN_OR_RAISE(snapshot, snapshot_manager->LoadSnapshot(*base_snapshot_id));
    } else {
        PAIMON_ASSIGN_OR_RAISE(snapshot, snapshot_manager->LatestSnapshot());
    }
    if (!snapshot) {
        return Status::Invalid("FileIndexWrite requires an existing snapshot");
    }
    auto schema_manager = std::make_shared<SchemaManager>(fs, table_path, resolved_branch);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> schema,
                           schema_manager->ReadSchema(snapshot->SchemaId()));
    if (!schema->PrimaryKeys().empty() || schema->NumBuckets() <= 0 ||
        ResolveBucketMode(schema->NumBuckets(), schema) != BucketMode::HASH_FIXED) {
        return Status::Invalid("FileIndexWrite only supports append tables with fixed buckets");
    }
    std::map<std::string, std::string> overrides = options;
    overrides[Options::BRANCH] = resolved_branch;
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> configured_schema,
                           schema->MergeOptions(overrides));
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(configured_schema->Options(), file_system));
    fs = core_options.GetFileSystem();
    if (core_options.GetBucket() != schema->NumBuckets()) {
        return Status::Invalid("FileIndexWrite cannot override the table's bucket count");
    }
    int32_t format_version = core_options.FileIndexFormatVersion();
    if (format_version != FileIndexFormat::V_1 && format_version != FileIndexFormat::V_2) {
        return Status::Invalid("Unsupported file index format version");
    }
    std::set<std::string> file_names;
    std::map<std::string, const DataFileIndexInput*> requests;
    for (const auto& file : files) {
        PAIMON_RETURN_NOT_OK(
            PathUtil::CheckSinglePathComponent("data file name", file.data_file_name));
        if (!file_names.insert(file.data_file_name).second) {
            return Status::Invalid("Duplicate data file name in file index inputs");
        }
        std::set<std::pair<std::string, std::string>> keys;
        for (const auto& index : file.indexes) {
            if (index.field_name.empty() || index.index_type.empty()) {
                return Status::Invalid("File index field name and index type must not be empty");
            }
            if (!keys.emplace(index.field_name, index.index_type).second) {
                return Status::Invalid("Duplicate field and index type in file index inputs");
            }
        }
        if (!file.indexes.empty()) {
            requests.emplace(file.data_file_name, &file);
        }
    }
    std::vector<std::shared_ptr<CommitMessage>> messages;
    if (requests.empty()) {
        return messages;
    }
    std::shared_ptr<MemoryPool> memory_pool = pool ? pool : GetDefaultPool();
    std::shared_ptr<Executor> scan_executor = executor;
    if (!scan_executor) {
        scan_executor = CreateDefaultExecutor();
    }
    FileStore store(table_path, core_options);
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> paths,
        store.NewPathFactory(arrow_schema, schema->PartitionKeys(),
                             core_options.GetFileFormat()->Identifier(), memory_pool));
    std::map<std::string, ManifestEntry> targets;
    PAIMON_ASSIGN_OR_RAISE(
        targets, FindFiles(store, schema, schema_manager, snapshot_manager, *snapshot, paths,
                           requests, scan_executor, memory_pool));
    std::vector<std::string> generated;
    ScopeGuard cleanup([&]() {
        for (const auto& path : generated) {
            [[maybe_unused]] Status ignored = fs->Delete(path, /*recursive=*/false);
        }
    });
    std::vector<BucketUpdates> buckets;
    std::unordered_map<BinaryRow, std::map<int32_t, size_t>> bucket_positions;
    for (const auto& [name, entry] : targets) {
        auto& partition_buckets = bucket_positions[entry.Partition()];
        auto [position, inserted] = partition_buckets.emplace(entry.Bucket(), buckets.size());
        if (inserted) {
            buckets.push_back({entry.Partition(), entry.Bucket(), entry.TotalBuckets(), {}, {}});
        }
        auto& bucket = buckets[position->second];
        if (bucket.total_buckets != entry.TotalBuckets()) {
            return Status::Invalid("Inconsistent total bucket counts in file index targets");
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> file_paths,
                               paths->CreateDataFilePathFactory(entry.Partition(), entry.Bucket()));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> indexed_file,
                               WriteFileIndexes(*requests.at(name), entry.File(), file_paths,
                                                format_version, fs, memory_pool, &generated));
        bucket.before.push_back(entry.File());
        bucket.after.push_back(std::move(indexed_file));
    }
    for (auto& bucket : buckets) {
        messages.push_back(std::make_shared<CommitMessageImpl>(
            bucket.partition, bucket.bucket, bucket.total_buckets, DataIncrement({}, {}, {}),
            CompactIncrement(std::move(bucket.before), std::move(bucket.after), {})));
    }
    cleanup.Release();
    return messages;
}

}  // namespace paimon

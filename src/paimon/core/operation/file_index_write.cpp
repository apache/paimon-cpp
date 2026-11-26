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
#include <optional>
#include <set>
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
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/defs.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {
namespace {

// Field name -> index type -> payload writer; an empty writer denotes an empty index.
using IndexPayloads =
    std::map<std::string, std::map<std::string, FileIndexFormat::Writer::PayloadWriter>>;

using FileIndexRequests = std::map<std::string, const DataFileIndexInput*>;

struct FileIndexWriteContext {
    std::shared_ptr<TableSchema> schema;
    CoreOptions options;
};

struct IndexTarget {
    std::shared_ptr<CommitMessageImpl> message;
    std::shared_ptr<DataFileMeta> file;
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
    std::string index_path = source_path
                                 ? DataFilePathFactory::CreateNewFileIndexFilePath(*source_path)
                                 : paths->ToFileIndexPath(paths->ToPath(file));
    PAIMON_ASSIGN_OR_RAISE(bool use_existing, fs->Exists(index_path));
    extras.emplace_back(PathUtil::GetName(index_path));
    auto indexed_file = file->CopyWithExtraFiles(extras);
    indexed_file->embedded_index.reset();
    if (use_existing) {
        return indexed_file;
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
    PAIMON_ASSIGN_OR_RAISE(std::string temporary_path, PathUtil::CreateTempPath(index_path));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> output,
                           fs->Create(temporary_path, /*overwrite=*/false));
    generated->push_back(temporary_path);
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
    PAIMON_ASSIGN_OR_RAISE(use_existing, fs->Exists(index_path));
    if (!use_existing) {
        Status renamed = fs->Rename(temporary_path, index_path);
        if (!renamed.ok()) {
            auto destination_exists = fs->Exists(index_path);
            if (!renamed.IsExist() && !(destination_exists.ok() && destination_exists.value())) {
                return renamed;
            }
            use_existing = true;
        } else {
            generated->back() = index_path;
        }
    }
    if (use_existing) {
        // Reuse the existing target without claiming it for failure cleanup.
        [[maybe_unused]] Status ignored = fs->Delete(temporary_path, /*recursive=*/false);
        generated->pop_back();
    }
    return indexed_file;
}

Result<std::map<std::string, IndexTarget>> FindFiles(
    const std::vector<std::shared_ptr<CommitMessage>>& messages,
    const FileIndexRequests& requests) {
    std::map<std::string, IndexTarget> targets;
    for (const auto& raw : messages) {
        auto message = std::dynamic_pointer_cast<CommitMessageImpl>(raw);
        if (!message) {
            return Status::Invalid("FileIndexWrite requires non-null CommitMessageImpl messages");
        }
        for (const auto* files : {&message->GetNewFilesIncrement().NewFiles(),
                                  &message->GetCompactIncrement().CompactAfter()}) {
            for (const auto& file : *files) {
                if (!file) {
                    return Status::Invalid("Null data file in commit message");
                }
                if (requests.count(file->file_name) == 0) {
                    continue;
                }
                if (!message->TotalBuckets() || *message->TotalBuckets() <= 0 ||
                    message->Bucket() < 0 || message->Bucket() >= *message->TotalBuckets()) {
                    return Status::Invalid("FileIndexWrite requires data files in fixed buckets");
                }
                if (!targets.emplace(file->file_name, IndexTarget{message, file}).second) {
                    return Status::Invalid(fmt::format(
                        "Ambiguous data file name in commit messages: {}", file->file_name));
                }
            }
        }
    }
    for (const auto& request : requests) {
        if (targets.count(request.first) == 0) {
            return Status::Invalid(
                fmt::format("File index target is not an added data file in commit messages: {}",
                            request.first));
        }
    }
    return targets;
}

Result<FileIndexWriteContext> LoadWriteContext(const std::string& table_path,
                                               const std::string& branch,
                                               const std::map<std::string, std::string>& options,
                                               const std::shared_ptr<FileSystem>& file_system) {
    if (table_path.empty()) {
        return Status::Invalid("FileIndexWrite requires a table path");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::string resolved_branch,
        BranchManager::ResolveBranch(std::nullopt, options, branch, "FileIndexWrite"));
    std::shared_ptr<FileSystem> fs = file_system;
    if (!fs) {
        PAIMON_ASSIGN_OR_RAISE(CoreOptions initial_options, CoreOptions::FromMap(options));
        fs = initial_options.GetFileSystem();
    }
    SchemaManager schema_manager(fs, table_path, resolved_branch);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest,
                           schema_manager.Latest());
    if (!latest) {
        return Status::Invalid("FileIndexWrite requires an existing table schema");
    }
    std::shared_ptr<TableSchema> schema = *latest;
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
    if (core_options.GetBucket() != schema->NumBuckets()) {
        return Status::Invalid("FileIndexWrite cannot override the table's bucket count");
    }
    int32_t format_version = core_options.FileIndexFormatVersion();
    if (format_version != FileIndexFormat::V_1 && format_version != FileIndexFormat::V_2) {
        return Status::Invalid("Unsupported file index format version");
    }
    return FileIndexWriteContext{std::move(schema), std::move(core_options)};
}

Result<FileIndexRequests> CollectIndexRequests(const std::vector<DataFileIndexInput>& files) {
    std::set<std::string> file_names;
    FileIndexRequests requests;
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
    return requests;
}

std::vector<std::shared_ptr<DataFileMeta>> ReplaceIndexedFiles(
    const std::vector<std::shared_ptr<DataFileMeta>>& files,
    const std::map<std::string, std::shared_ptr<DataFileMeta>>& replacements) {
    std::vector<std::shared_ptr<DataFileMeta>> result;
    result.reserve(files.size());
    for (const auto& file : files) {
        auto iter = replacements.find(file->file_name);
        result.push_back(iter == replacements.end() ? file : iter->second);
    }
    return result;
}

Result<std::vector<std::shared_ptr<CommitMessage>>> WriteIndexesAndBuildMessages(
    const std::vector<std::shared_ptr<CommitMessage>>& messages, const FileIndexRequests& requests,
    const std::map<std::string, IndexTarget>& targets,
    const std::shared_ptr<FileStorePathFactory>& paths, int32_t format_version,
    const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<MemoryPool>& memory_pool) {
    std::vector<std::string> generated;
    ScopeGuard cleanup([&]() {
        for (const auto& path : generated) {
            [[maybe_unused]] Status ignored = fs->Delete(path, /*recursive=*/false);
        }
    });
    std::map<std::string, std::shared_ptr<DataFileMeta>> replacements;
    for (const auto& [name, target] : targets) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> file_paths,
                               paths->CreateDataFilePathFactory(target.message->Partition(),
                                                                target.message->Bucket()));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> indexed_file,
                               WriteFileIndexes(*requests.at(name), target.file, file_paths,
                                                format_version, fs, memory_pool, &generated));
        replacements.emplace(name, std::move(indexed_file));
    }
    std::vector<std::shared_ptr<CommitMessage>> updated;
    updated.reserve(messages.size());
    for (const auto& raw : messages) {
        const auto* message = dynamic_cast<const CommitMessageImpl*>(raw.get());
        const auto& data = message->GetNewFilesIncrement();
        const auto& compact = message->GetCompactIncrement();
        auto new_files = ReplaceIndexedFiles(data.NewFiles(), replacements);
        auto compact_after = ReplaceIndexedFiles(compact.CompactAfter(), replacements);
        if (new_files == data.NewFiles() && compact_after == compact.CompactAfter()) {
            updated.push_back(raw);
            continue;
        }
        updated.push_back(std::make_shared<CommitMessageImpl>(
            message->Partition(), message->Bucket(), message->TotalBuckets(),
            DataIncrement(std::move(new_files),
                          std::vector<std::shared_ptr<DataFileMeta>>(data.DeletedFiles()),
                          std::vector<std::shared_ptr<DataFileMeta>>(data.ChangelogFiles()),
                          std::vector<std::shared_ptr<IndexFileMeta>>(data.NewIndexFiles()),
                          std::vector<std::shared_ptr<IndexFileMeta>>(data.DeletedIndexFiles())),
            CompactIncrement(
                std::vector<std::shared_ptr<DataFileMeta>>(compact.CompactBefore()),
                std::move(compact_after),
                std::vector<std::shared_ptr<DataFileMeta>>(compact.ChangelogFiles()),
                std::vector<std::shared_ptr<IndexFileMeta>>(compact.NewIndexFiles()),
                std::vector<std::shared_ptr<IndexFileMeta>>(compact.DeletedIndexFiles()))));
    }
    cleanup.Release();
    return updated;
}

}  // namespace

Result<std::vector<std::shared_ptr<CommitMessage>>> FileIndexWrite::PrepareCommit(
    const std::string& table_path, const std::string& branch,
    const std::map<std::string, std::string>& options,
    const std::vector<std::shared_ptr<CommitMessage>>& messages,
    const std::vector<DataFileIndexInput>& files, const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(FileIndexWriteContext context,
                           LoadWriteContext(table_path, branch, options, file_system));
    PAIMON_ASSIGN_OR_RAISE(FileIndexRequests requests, CollectIndexRequests(files));
    std::map<std::string, IndexTarget> targets;
    PAIMON_ASSIGN_OR_RAISE(targets, FindFiles(messages, requests));
    if (requests.empty()) {
        return messages;
    }
    std::shared_ptr<MemoryPool> memory_pool = pool ? pool : GetDefaultPool();
    FileStore store(table_path, context.options);
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(context.schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> paths,
        store.NewPathFactory(arrow_schema, context.schema->PartitionKeys(),
                             context.options.GetFileFormat()->Identifier(), memory_pool));
    return WriteIndexesAndBuildMessages(messages, requests, targets, paths,
                                        context.options.FileIndexFormatVersion(),
                                        context.options.GetFileSystem(), memory_pool);
}

}  // namespace paimon

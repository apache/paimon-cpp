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

#include "paimon/file_index/file_index_read.h"

#include <map>
#include <optional>
#include <utility>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/append_dv_context.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/precise_file_index_read.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats_evolutions.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/read_context.h"

namespace paimon {
namespace {

class FileIndexReadImpl final : public FileIndexRead {
 public:
    static Result<std::unique_ptr<FileIndexReadImpl>> Create(
        AppendDvContext context, const std::shared_ptr<Split>& split,
        const std::shared_ptr<MemoryPool>& pool);

    Result<std::vector<FileRowSelection>> Query(
        const std::shared_ptr<Predicate>& predicate) override;

 private:
    struct File {
        std::shared_ptr<DataFileMeta> meta;
        DataSplit::SimpleDataFileMeta reference;
        std::optional<SimpleStatsEvolution::EvolutionStats> stats;
        std::unique_ptr<PreciseFileIndexRead> read;
        RoaringBitmap32 deleted_positions;
    };

    FileIndexReadImpl(AppendDvContext context, std::shared_ptr<DataFilePathFactory> file_paths,
                      DeletionVector::Factory dv_factory, const BinaryRow& partition_row,
                      std::map<std::string, std::string> partition, int32_t bucket,
                      std::vector<File> files, std::shared_ptr<MemoryPool> pool)
        : context_(std::move(context)),
          pool_(std::move(pool)),
          evolutions_(context_.schema, pool_),
          file_paths_(std::move(file_paths)),
          dv_factory_(std::move(dv_factory)),
          partition_row_(partition_row),
          partition_(std::move(partition)),
          bucket_(bucket),
          files_(std::move(files)) {}

    Status LoadStats(File* file);
    Status LoadReader(File* file);
    Result<RoaringBitmap32> QueryFile(const std::shared_ptr<PredicateFilter>& predicate,
                                      File* file);

    AppendDvContext context_;
    std::shared_ptr<MemoryPool> pool_;
    SimpleStatsEvolutions evolutions_;
    std::shared_ptr<DataFilePathFactory> file_paths_;
    std::unique_ptr<RawFileSplitRead> split_read_;
    DeletionVector::Factory dv_factory_;
    BinaryRow partition_row_;
    std::map<std::string, std::string> partition_;
    int32_t bucket_;
    std::vector<File> files_;
};

Result<std::unique_ptr<FileIndexReadImpl>> FileIndexReadImpl::Create(
    AppendDvContext context, const std::shared_ptr<Split>& split,
    const std::shared_ptr<MemoryPool>& pool) {
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
    if (!data_split || data_split->IsStreaming() || !data_split->RawConvertible() ||
        !data_split->BeforeFiles().empty()) {
        return Status::NotImplemented("FileIndexRead requires a raw-convertible batch DataSplit");
    }
    const auto& files = data_split->DataFiles();
    const auto& deletions = data_split->DeletionFiles();
    if (!deletions.empty() && deletions.size() != files.size()) {
        return Status::Invalid("FileIndexRead split has mismatched deletion files");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> file_paths,
                           context.path_factory->CreateDataFilePathFactory(data_split->Partition(),
                                                                           data_split->Bucket()));
    std::vector<std::pair<std::string, std::string>> partition_values;
    PAIMON_ASSIGN_OR_RAISE(partition_values,
                           context.path_factory->GeneratePartitionVector(data_split->Partition()));
    std::map<std::string, std::string> partition(partition_values.begin(), partition_values.end());
    auto dv_factory = DeletionVector::CreateFactory(
        context.options.GetFileSystem(), DeletionVector::CreateDeletionFileMap(files, deletions),
        pool);
    auto references = data_split->GetFileList();
    std::vector<File> query_files;
    query_files.reserve(files.size());
    for (size_t i = 0; i < files.size(); ++i) {
        query_files.push_back(File{files[i], std::move(references[i]), std::nullopt, nullptr, {}});
    }
    return std::unique_ptr<FileIndexReadImpl>(new FileIndexReadImpl(
        std::move(context), std::move(file_paths), std::move(dv_factory), data_split->Partition(),
        std::move(partition), data_split->Bucket(), std::move(query_files), pool));
}

Status FileIndexReadImpl::LoadStats(File* file) {
    if (file->stats) {
        return Status::OK();
    }
    std::shared_ptr<TableSchema> schema = context_.schema;
    if (file->meta->schema_id != schema->Id()) {
        PAIMON_ASSIGN_OR_RAISE(schema, context_.schema_manager->ReadSchema(file->meta->schema_id));
    }
    auto evolution = evolutions_.GetOrCreate(schema);
    auto dense_fields = file->meta->value_stats_cols;
    if (file->meta->value_stats.MinValues().GetFieldCount() == 0) {
        dense_fields = std::vector<std::string>{};
    }
    PAIMON_ASSIGN_OR_RAISE(
        SimpleStatsEvolution::EvolutionStats stats,
        evolution->Evolution(file->meta->value_stats, file->meta->row_count, dense_fields));
    file->stats.emplace(std::move(stats));
    return Status::OK();
}

Status FileIndexReadImpl::LoadReader(File* file) {
    if (file->read) {
        return Status::OK();
    }
    if (!split_read_) {
        ReadContextBuilder builder(context_.path_factory->RootPath());
        builder.SetOptions(context_.options.ToMap())
            .WithBranch(context_.options.GetBranch())
            .WithFileSystem(context_.options.GetFileSystem())
            .WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context, builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<InternalReadContext> internal_context,
            InternalReadContext::Create(read_context, context_.schema, context_.options.ToMap()));
        split_read_ = std::make_unique<RawFileSplitRead>(context_.path_factory, internal_context,
                                                         pool_, read_context->GetExecutor());
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<PreciseFileIndexRead> read,
                           PreciseFileIndexRead::Create(*split_read_, partition_row_, file->meta,
                                                        nullptr, file_paths_, {}));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DeletionVector> dv, dv_factory_(file->meta->file_name));
    if (dv) {
        auto bitmap = dynamic_cast<BitmapDeletionVector*>(dv.get());
        if (!bitmap) {
            return Status::NotImplemented(
                "FileIndexRead does not support bitmap64 deletion vectors");
        }
        file->deleted_positions = *bitmap->GetBitmap();
    }
    file->read = std::move(read);
    return Status::OK();
}

Result<RoaringBitmap32> FileIndexReadImpl::QueryFile(
    const std::shared_ptr<PredicateFilter>& predicate, File* file) {
    if (file->meta->row_count < 0) {
        return Status::Invalid("Data file row count is negative");
    }
    if (file->meta->row_count == 0) {
        return RoaringBitmap32();
    }
    PAIMON_RETURN_NOT_OK(LoadStats(file));
    const auto& stats = *file->stats;
    PAIMON_ASSIGN_OR_RAISE(
        bool may_match, predicate->Test(context_.arrow_schema, file->meta->row_count,
                                        *stats.min_values, *stats.max_values, *stats.null_counts));
    if (!may_match) {
        return RoaringBitmap32();
    }
    if (file->meta->row_count > RoaringBitmap32::MAX_VALUE) {
        return Status::NotImplemented("FileIndexRead requires bitmap32 physical row positions");
    }
    PAIMON_RETURN_NOT_OK(LoadReader(file));
    RoaringBitmap32 candidates;
    candidates.AddRange(0, static_cast<int32_t>(file->meta->row_count));
    candidates -= file->deleted_positions;
    if (candidates.IsEmpty()) {
        return candidates;
    }
    return file->read->ReadRowPositions(predicate, candidates);
}

Result<std::vector<FileRowSelection>> FileIndexReadImpl::Query(
    const std::shared_ptr<Predicate>& predicate) {
    if (!predicate) {
        return Status::Invalid("FileIndexRead requires a non-null query predicate");
    }
    PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
        *context_.arrow_schema, predicate, /*validate_field_idx=*/true));
    auto filter = std::dynamic_pointer_cast<PredicateFilter>(predicate);
    if (!filter) {
        return Status::NotImplemented("FileIndexRead requires a scalar predicate");
    }
    std::vector<FileRowSelection> result;
    for (auto& file : files_) {
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 matches, QueryFile(filter, &file));
        if (!matches.IsEmpty()) {
            result.push_back(
                FileRowSelection{file.reference, partition_, bucket_, std::move(matches)});
        }
    }
    return result;
}

}  // namespace

Result<std::unique_ptr<FileIndexRead>> FileIndexRead::Create(
    const std::string& table_path, const std::string& branch,
    const std::map<std::string, std::string>& options, const std::shared_ptr<Split>& split,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool) {
    if (!split) {
        return Status::Invalid("FileIndexRead requires a split");
    }
    PAIMON_ASSIGN_OR_RAISE(AppendDvContext context,
                           AppendDvContext::Create(table_path, branch, options, file_system, pool));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexReadImpl> read,
                           FileIndexReadImpl::Create(std::move(context), split, pool));
    return std::unique_ptr<FileIndexRead>(std::move(read));
}

}  // namespace paimon

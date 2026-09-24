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

#include "paimon/core/operation/raw_file_split_read.h"

#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/reader/complete_row_kind_batch_reader.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/status.h"
#include "paimon/table/source/data_split.h"

namespace paimon {
class DataFilePathFactory;
class Executor;
class Predicate;

namespace {

Status ValidateFileLocalRowRanges(const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
                                  const std::optional<std::vector<Range>>& local_row_ranges) {
    if (local_row_ranges == std::nullopt) {
        return Status::OK();
    }
    if (data_files.size() != 1) {
        return Status::Invalid("file-local row ranges require exactly one data file");
    }
    const auto& file = data_files.front();
    for (const Range& range : local_row_ranges.value()) {
        if (range.from < 0 || range.to < range.from || range.to >= file->row_count ||
            range.to >= std::numeric_limits<int32_t>::max()) {
            return Status::Invalid(
                fmt::format("Invalid file-local row range [{}, {}] for file {} with {} rows.",
                            range.from, range.to, file->file_name, file->row_count));
        }
    }
    return Status::OK();
}

}  // namespace

RawFileSplitRead::RawFileSplitRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                                   const std::shared_ptr<InternalReadContext>& context,
                                   const std::shared_ptr<MemoryPool>& memory_pool,
                                   const std::shared_ptr<Executor>& executor)
    : AbstractSplitRead(path_factory, context,
                        std::make_unique<SchemaManager>(context->GetCoreOptions().GetFileSystem(),
                                                        context->GetPath(),
                                                        context->GetCoreOptions().GetBranch()),
                        memory_pool, executor) {}

Result<std::unique_ptr<BatchReader>> RawFileSplitRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    if (auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
        PAIMON_RETURN_NOT_OK(indexed_split->Validate());
        if (!indexed_split->Scores().empty()) {
            // TODO(wangyong9999): Propagate indexed scores through the primary-key
            // physical-position read path.
            return Status::NotImplemented(
                "Primary-key reads do not support scored indexed splits yet.");
        }
        const std::shared_ptr<DataSplit>& inner_split = indexed_split->GetDataSplit();
        auto inner_split_impl = std::dynamic_pointer_cast<DataSplitImpl>(inner_split);
        if (!inner_split_impl) {
            return Status::Invalid("cannot cast indexed inner split to data_split");
        }
        if (inner_split_impl->DataFiles().size() != 1) {
            return Status::Invalid(
                "indexed splits with file-local row ranges must contain exactly one file");
        }
        return CreateReader(inner_split_impl->Partition(), inner_split_impl->Bucket(),
                            inner_split_impl->DataFiles(), inner_split_impl->DeletionFiles(),
                            indexed_split->RowRanges());
    }
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
    if (!data_split) {
        return Status::Invalid("cannot cast split to data_split in RawFileSplitRead");
    }
    return CreateReader(data_split->Partition(), data_split->Bucket(), data_split->DataFiles(),
                        data_split->DeletionFiles(), /*local_row_ranges=*/std::nullopt);
}

Result<std::unique_ptr<BatchReader>> RawFileSplitRead::CreateReader(
    const BinaryRow& partition, int32_t bucket,
    const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
    DeletionVector::Factory dv_factory, const std::optional<std::vector<Range>>& local_row_ranges) {
    PAIMON_RETURN_NOT_OK(ValidateFileLocalRowRanges(data_files, local_row_ranges));
    const auto& predicate = context_->GetPredicate();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                           path_factory_->CreateDataFilePathFactory(partition, bucket));

    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::unique_ptr<FileBatchReader>> raw_file_readers,
        CreateRawFileReaders(partition, data_files, raw_read_schema_, predicate, dv_factory,
                             local_row_ranges, data_file_path_factory,
                             /*extra_format_options=*/{}));

    auto raw_readers =
        ObjectUtils::MoveVector<std::unique_ptr<BatchReader>>(std::move(raw_file_readers));
    auto concat_batch_reader =
        std::make_unique<ConcatBatchReader>(std::move(raw_readers), arrow_pool_);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> batch_reader,
                           ApplyPredicateFilterIfNeeded(std::move(concat_batch_reader), predicate));
    return std::make_unique<CompleteRowKindBatchReader>(std::move(batch_reader), arrow_pool_);
}

Result<std::unique_ptr<BatchReader>> RawFileSplitRead::CreateReader(
    const BinaryRow& partition, int32_t bucket,
    const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
    const std::vector<std::optional<DeletionFile>>& deletion_files,
    const std::optional<std::vector<Range>>& local_row_ranges) {
    auto dv_factory = DeletionVector::CreateFactory(
        options_.GetFileSystem(), DeletionVector::CreateDeletionFileMap(data_files, deletion_files),
        pool_);
    return CreateReader(partition, bucket, data_files, dv_factory, local_row_ranges);
}

Result<bool> RawFileSplitRead::Match(const std::shared_ptr<Split>& split,
                                     bool force_keep_delete) const {
    bool is_indexed = false;
    std::shared_ptr<Split> data_split = split;
    if (auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
        PAIMON_RETURN_NOT_OK(indexed_split->Validate());
        data_split = indexed_split->GetDataSplit();
        is_indexed = true;
    }
    auto split_impl = dynamic_cast<DataSplitImpl*>(data_split.get());
    if (split_impl == nullptr) {
        return Status::Invalid("unexpected error, split cast to impl failed");
    }
    if (context_->GetTableSchema()->PrimaryKeys().empty()) {
        // for append table, always return true
        return true;
    }
    bool matched = !force_keep_delete && !split_impl->IsStreaming() &&
                   (is_indexed || split_impl->RawConvertible());
    if (matched) {
        // for legacy version, we are not sure if there are delete rows, but in order to be
        // compatible with the query acceleration of the OLAP engine, we have generated raw
        // files.
        // Here, for the sake of correctness, we still need to perform drop delete filtering.
        for (const auto& file : split_impl->DataFiles()) {
            if (file == nullptr || file->delete_row_count == std::nullopt ||
                file->delete_row_count.value() != 0) {
                return false;
            }
        }
    }
    return matched;
}

}  // namespace paimon

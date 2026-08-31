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

#include "paimon/core/mergetree/compact/full_changelog_merge_tree_compact_rewriter.h"

#include <utility>

#include "paimon/common/data/serializer/row_compacted_serializer.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/core/mergetree/compact/full_changelog_merge_function_wrapper.h"
#include "paimon/core/mergetree/compact/internal_row_equalizer.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/utils/primary_key_table_utils.h"
#include "paimon/read_context.h"

namespace paimon {

FullChangelogMergeTreeCompactRewriter::FullChangelogMergeTreeCompactRewriter(
    int32_t max_level, const BinaryRow& partition, int32_t bucket, int64_t schema_id,
    const std::vector<std::string>& trimmed_primary_keys, const CoreOptions& options,
    const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<arrow::Schema>& write_schema, DeletionVector::Factory dv_factory,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    std::unique_ptr<MergeFileSplitRead>&& merge_file_split_read,
    MergeFunctionWrapperFactory merge_function_wrapper_factory,
    ChangelogMergeFunctionWrapperFactory changelog_merge_function_wrapper_factory,
    const std::shared_ptr<CancellationController>& cancellation_controller,
    const std::shared_ptr<MemoryPool>& pool)
    : ChangelogMergeTreeRewriter(max_level, /*force_drop_delete=*/false, partition, bucket,
                                 schema_id, trimmed_primary_keys, options, data_schema,
                                 write_schema, std::move(dv_factory), path_factory_cache,
                                 std::move(merge_file_split_read),
                                 std::move(merge_function_wrapper_factory),
                                 std::move(changelog_merge_function_wrapper_factory),
                                 /*produce_changelog=*/true, cancellation_controller, pool) {}

Result<std::unique_ptr<FullChangelogMergeTreeCompactRewriter>>
FullChangelogMergeTreeCompactRewriter::Create(
    int32_t max_level, int32_t bucket, const BinaryRow& partition,
    const std::shared_ptr<TableSchema>& table_schema, DeletionVector::Factory dv_factory,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    const CoreOptions& options,
    const std::shared_ptr<CancellationController>& cancellation_controller,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema->TrimmedPrimaryKeys());
    std::shared_ptr<arrow::Schema> data_schema =
        DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    std::shared_ptr<arrow::Schema> write_schema =
        SpecialFields::CompleteSequenceAndValueKindField(data_schema);

    ReadContextBuilder read_context_builder(path_factory_cache->RootPath());
    read_context_builder.SetOptions(options.ToMap())
        .WithFileSystem(options.GetFileSystem())
        .EnablePrefetch(true)
        .SetPrefetchMaxParallelNum(1)
        .SetPrefetchBatchCount(3)
        .WithMemoryPool(pool);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context,
                           read_context_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> internal_context,
        InternalReadContext::Create(read_context, table_schema, options.ToMap()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        path_factory_cache->GetOrCreatePathFactory(options.GetFileFormat()->Identifier()));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<MergeFileSplitRead> merge_file_split_read,
        MergeFileSplitRead::Create(path_factory, internal_context, pool, CreateDefaultExecutor()));

    MergeFunctionWrapperFactory merge_function_wrapper_factory =
        []() -> Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> {
        return std::shared_ptr<MergeFunctionWrapper<KeyValue>>();
    };

    FieldsComparator::FieldComparatorFunc value_equalizer;
    if (options.ChangelogRowDeduplicate()) {
        PAIMON_ASSIGN_OR_RAISE(value_equalizer,
                               InternalRowEqualizer::Create(
                                   data_schema, options.GetChangelogRowDeduplicateIgnoreFields()));
    }
    ChangelogMergeFunctionWrapperFactory changelog_merge_function_wrapper_factory =
        [data_schema, trimmed_primary_keys, options, max_level, value_equalizer,
         pool](int32_t /*output_level*/)
        -> Result<std::shared_ptr<MergeFunctionWrapper<ChangelogResult>>> {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MergeFunction> merge_function,
                               PrimaryKeyTableUtils::CreateMergeFunction(
                                   data_schema, trimmed_primary_keys, options, pool));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RowCompactedSerializer> value_serializer,
                               RowCompactedSerializer::Create(data_schema, pool));
        std::shared_ptr<MergeFunctionWrapper<ChangelogResult>> wrapper =
            std::make_shared<FullChangelogMergeFunctionWrapper>(
                std::move(merge_function), max_level, std::move(value_serializer), value_equalizer);
        return wrapper;
    };

    return std::unique_ptr<FullChangelogMergeTreeCompactRewriter>(
        new FullChangelogMergeTreeCompactRewriter(
            max_level, partition, bucket, table_schema->Id(), trimmed_primary_keys, options,
            data_schema, write_schema, std::move(dv_factory), path_factory_cache,
            std::move(merge_file_split_read), std::move(merge_function_wrapper_factory),
            std::move(changelog_merge_function_wrapper_factory), cancellation_controller, pool));
}

Result<CompactResult> FullChangelogMergeTreeCompactRewriter::Rewrite(
    int32_t output_level, bool drop_delete, const std::vector<std::vector<SortedRun>>& sections) {
    if (output_level == max_level_ && !drop_delete) {
        return Status::Invalid(
            "Delete records should be dropped from result of full compaction. This is "
            "unexpected.");
    }
    return ChangelogMergeTreeRewriter::Rewrite(output_level, drop_delete, sections);
}

}  // namespace paimon

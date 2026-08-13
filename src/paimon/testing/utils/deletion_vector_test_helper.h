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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/commit_message.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/range_helper.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/data_evolution_utils.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::test {

/// Helpers to express row-level deletes on an append or data-evolution table the way an
/// external engine does: write a deletion vector index file and commit it as an index-only
/// commit message.
class DeletionVectorTestHelper {
 public:
    DeletionVectorTestHelper() = delete;
    ~DeletionVectorTestHelper() = delete;

    /// Builds, but does not commit, an index-only commit message holding one deletion vector per
    /// entry of `deleted_positions_by_file`, all in a single index file. A data file is covered
    /// by at most one deletion vector, so covering it again later is rejected unless that commit
    /// also drops the index file holding the earlier one, which `replaced_commit_msg` does.
    ///
    /// @param file_format_identifier The table's data file format, for the path factory.
    /// @param base_commit_msg A data commit message of the same partition and bucket, which
    ///     supplies the partition, bucket and total buckets of the index commit message.
    /// @param deleted_positions_by_file Deleted positions keyed by data file name. Positions
    ///     are relative to that file; for a data evolution table the key must be the row range
    ///     group's anchor file (see RetrieveAnchorFileNames).
    /// @param replaced_commit_msg An earlier deletion vector commit message whose index files
    ///     this one drops. Null when nothing is replaced.
    static Result<std::shared_ptr<CommitMessage>> CreateDeletionVectorCommitMessage(
        const std::shared_ptr<FileSystem>& file_system, const std::string& table_path,
        const std::string& file_format_identifier,
        const std::shared_ptr<CommitMessage>& base_commit_msg,
        const std::map<std::string, std::vector<int64_t>>& deleted_positions_by_file,
        const std::shared_ptr<MemoryPool>& pool,
        const std::shared_ptr<CommitMessage>& replaced_commit_msg = nullptr) {
        auto base_msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(base_commit_msg);
        if (!base_msg_impl) {
            return Status::Invalid("cannot cast commit message to CommitMessageImpl");
        }
        SchemaManager schema_manager(file_system, table_path);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema_opt,
                               schema_manager.Latest());
        if (!latest_schema_opt) {
            return Status::Invalid("table schema does not exist");
        }
        const std::shared_ptr<TableSchema>& table_schema = latest_schema_opt.value();
        PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                               CoreOptions::FromMap(table_schema->Options(), file_system));
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                               core_options.CreateExternalPaths());
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                               core_options.CreateGlobalIndexExternalPath());
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(table_path, arrow_schema, table_schema->PartitionKeys(),
                                         core_options.GetPartitionDefaultName(),
                                         file_format_identifier, core_options.DataFilePrefix(),
                                         core_options.LegacyPartitionNameEnabled(), external_paths,
                                         global_index_external_path,
                                         core_options.IndexFileInDataFileDir(), pool));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<IndexPathFactory> index_path_factory,
                               path_factory->CreateIndexFileFactory(base_msg_impl->Partition(),
                                                                    base_msg_impl->Bucket()));
        DeletionVectorsIndexFile index_file(
            file_system, std::shared_ptr<IndexPathFactory>(std::move(index_path_factory)),
            core_options.DeletionVectorsBitmap64(), pool);

        std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors;
        for (const auto& [file_name, deleted_positions] : deleted_positions_by_file) {
            // deleting through the vector itself applies the same position bound a writer is
            // held to, instead of narrowing to the bitmap's index type and wrapping silently
            auto deletion_vector = std::make_shared<BitmapDeletionVector>(RoaringBitmap32());
            for (int64_t position : deleted_positions) {
                if (position < 0) {
                    return Status::Invalid(fmt::format(
                        "Deleted position {} of data file {} is negative.", position, file_name));
                }
                PAIMON_RETURN_NOT_OK(deletion_vector->Delete(position));
            }
            deletion_vectors[file_name] = std::move(deletion_vector);
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexFileMeta> index_file_meta,
                               index_file.WriteSingleFile(deletion_vectors));

        std::vector<std::shared_ptr<IndexFileMeta>> deleted_index_files;
        if (replaced_commit_msg) {
            auto replaced_msg_impl =
                std::dynamic_pointer_cast<CommitMessageImpl>(replaced_commit_msg);
            if (!replaced_msg_impl) {
                return Status::Invalid("cannot cast replaced commit message to CommitMessageImpl");
            }
            deleted_index_files = replaced_msg_impl->GetNewFilesIncrement().NewIndexFiles();
        }
        return std::make_shared<CommitMessageImpl>(
            base_msg_impl->Partition(), base_msg_impl->Bucket(), base_msg_impl->TotalBuckets(),
            DataIncrement({}, {}, {}, {index_file_meta}, std::move(deleted_index_files)),
            CompactIncrement({}, {}, {}));
    }

    /// Groups the new data files of `commit_msgs` into row range groups and returns the anchor
    /// file name of each group, ordered by ascending first row id.
    ///
    /// @param commit_msgs The data commit messages whose files form the groups. Every file must
    ///     already carry a first row id: the commit assigns them onto its own copies, so the
    ///     caller has to stamp them here first.
    /// @return One anchor file name per row range group.
    static Result<std::vector<std::string>> RetrieveAnchorFileNames(
        const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) {
        std::vector<std::shared_ptr<DataFileMeta>> files;
        for (const auto& commit_msg : commit_msgs) {
            auto commit_msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(commit_msg);
            if (!commit_msg_impl) {
                return Status::Invalid("cannot cast commit message to CommitMessageImpl");
            }
            for (const auto& file : commit_msg_impl->GetNewFilesIncrement().NewFiles()) {
                files.push_back(file);
            }
        }
        return RetrieveAnchorFileNames(files);
    }

    /// The same over data file metas, so a test can derive the anchors from a planned split,
    /// which is what the read looks a group's deletion vector up by. Deriving them only from
    /// the metas the caller wrote hides a mismatch between the two.
    static Result<std::vector<std::string>> RetrieveAnchorFileNames(
        const std::vector<std::shared_ptr<DataFileMeta>>& data_files) {
        std::vector<std::shared_ptr<DataFileMeta>> files = data_files;
        RangeHelper<std::shared_ptr<DataFileMeta>> range_helper(
            [](const std::shared_ptr<DataFileMeta>& meta) -> Result<int64_t> {
                return meta->NonNullFirstRowId();
            },
            [](const std::shared_ptr<DataFileMeta>& meta) -> Result<int64_t> {
                PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, meta->NonNullFirstRowId());
                return first_row_id + meta->row_count - 1;
            });
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> groups,
                               range_helper.MergeOverlappingRanges(std::move(files)));

        std::vector<std::string> anchor_file_names;
        anchor_file_names.reserve(groups.size());
        for (const auto& group : groups) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> anchor,
                                   DataEvolutionUtils::RetrieveAnchorFile(group));
            anchor_file_names.push_back(anchor->file_name);
        }
        return anchor_file_names;
    }

    /// Convenience wrapper of RetrieveAnchorFileNames for a single row range group.
    static Result<std::string> RetrieveAnchorFileName(
        const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> anchor_file_names,
                               RetrieveAnchorFileNames(commit_msgs));
        if (anchor_file_names.size() != 1) {
            return Status::Invalid("expected exactly one row range group in commit messages");
        }
        return anchor_file_names[0];
    }
};

}  // namespace paimon::test

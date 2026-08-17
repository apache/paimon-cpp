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

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "paimon/core/index/pk/primary_key_index_definition.h"
#include "paimon/core/index/pksorted/pk_sorted_index_group.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/global_index/global_index_result.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"

namespace paimon {
/// Plans and evaluates source-backed primary-key scalar index groups in file-local
/// row-position space.
///
/// The scan works on one captured snapshot: data splits and index manifest entries must
/// come from the same snapshot. Every active data file is associated with the validated
/// payload group of its (bucket, field, data level); files without a valid group keep an
/// empty group map and later fall back to a normal scan. Evaluation runs the predicate
/// against the group payloads once per group and query, then localizes group ordinals to
/// per-file physical row positions using the ordered source row-count prefix.
class PrimaryKeySortedIndexScan {
 public:
    PrimaryKeySortedIndexScan() = delete;
    ~PrimaryKeySortedIndexScan() = delete;

    /// One active data file and its complete field-local payload groups.
    class FilePlan {
     public:
        FilePlan(std::shared_ptr<DataSplitImpl> source_split, int32_t file_index,
                 std::map<int32_t, std::shared_ptr<PkSortedIndexGroup>> groups)
            : source_split_(std::move(source_split)),
              file_index_(file_index),
              groups_(std::move(groups)) {}

        const std::shared_ptr<DataSplitImpl>& SourceSplit() const {
            return source_split_;
        }

        int32_t FileIndex() const {
            return file_index_;
        }

        const std::shared_ptr<DataFileMeta>& DataFile() const {
            return source_split_->DataFiles()[file_index_];
        }

        std::shared_ptr<PkSortedIndexGroup> Group(int32_t field_id) const {
            auto iter = groups_.find(field_id);
            return iter == groups_.end() ? nullptr : iter->second;
        }

        const std::map<int32_t, std::shared_ptr<PkSortedIndexGroup>>& Groups() const {
            return groups_;
        }

     private:
        std::shared_ptr<DataSplitImpl> source_split_;
        int32_t file_index_;
        std::map<int32_t, std::shared_ptr<PkSortedIndexGroup>> groups_;
    };

    /// Immutable groups for all source files in one captured snapshot.
    class Plan {
     public:
        Plan(int64_t snapshot_id, std::vector<FilePlan> files)
            : snapshot_id_(snapshot_id), files_(std::move(files)) {}

        int64_t SnapshotId() const {
            return snapshot_id_;
        }

        const std::vector<FilePlan>& Files() const {
            return files_;
        }

     private:
        int64_t snapshot_id_;
        std::vector<FilePlan> files_;
    };

    /// Optional file-local index result; a null result means that the file requires a
    /// normal scan.
    class EvaluatedFile {
     public:
        EvaluatedFile(FilePlan file, std::shared_ptr<GlobalIndexResult> result)
            : file_(std::move(file)), result_(std::move(result)) {}

        const FilePlan& File() const {
            return file_;
        }

        const std::shared_ptr<GlobalIndexResult>& IndexResult() const {
            return result_;
        }

     private:
        FilePlan file_;
        std::shared_ptr<GlobalIndexResult> result_;
    };

    /// Predicate results for all source files in one captured snapshot.
    class EvaluatedPlan {
     public:
        EvaluatedPlan(int64_t snapshot_id, std::vector<EvaluatedFile> files)
            : snapshot_id_(snapshot_id), files_(std::move(files)) {}

        int64_t SnapshotId() const {
            return snapshot_id_;
        }

        const std::vector<EvaluatedFile>& Files() const {
            return files_;
        }

     private:
        int64_t snapshot_id_;
        std::vector<EvaluatedFile> files_;
    };

    /// Creates a payload reader for one validated group. Returning a null reader marks the
    /// index type as unusable so affected predicates keep their normal scan semantics.
    using ReaderFactory = std::function<Result<std::shared_ptr<GlobalIndexReader>>(
        const FilePlan& file, const PrimaryKeyIndexDefinition& definition,
        const PkSortedIndexGroup& group)>;

    /// Associates every active data file with the validated payload groups of its bucket.
    ///
    /// `index_entries` must be the ADD entries of the same snapshot carrying global index
    /// metadata with source metadata. Splits must be non-streaming and their deletion
    /// files, when present, must align with their data files.
    static Result<Plan> CreatePlan(int64_t snapshot_id,
                                   const std::vector<std::shared_ptr<DataSplitImpl>>& data_splits,
                                   const std::vector<PrimaryKeyIndexDefinition>& definitions,
                                   const std::vector<IndexManifestEntry>& index_entries);

    /// Evaluates the predicate for every planned file. Evaluation failures degrade to a
    /// null per-file result instead of failing the scan.
    static Result<EvaluatedPlan> Evaluate(const Plan& plan,
                                          const std::shared_ptr<TableSchema>& table_schema,
                                          const std::shared_ptr<Predicate>& predicate,
                                          const std::vector<PrimaryKeyIndexDefinition>& definitions,
                                          const ReaderFactory& reader_factory);

    /// Creates the default reader factory which opens the single BTree payload of each validated
    /// group through the table's index directory layout. Non-BTree families resolve to a null
    /// reader and therefore keep normal scan semantics.
    static ReaderFactory MakeReaderFactory(
        const std::shared_ptr<FileSystem>& file_system,
        const std::shared_ptr<IndexFilePathFactories>& path_factories,
        const std::shared_ptr<TableSchema>& table_schema, const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon

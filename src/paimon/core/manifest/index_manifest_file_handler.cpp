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

#include "paimon/core/manifest/index_manifest_file_handler.h"

#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/index_file_meta.h"
namespace paimon {

using BucketIdentifier = std::tuple<BinaryRow, int32_t, std::string>;

Result<std::vector<IndexManifestEntry>> IndexManifestFileHandler::BucketedCombiner::Combine(
    const std::vector<IndexManifestEntry>& prev_index_files,
    const std::vector<IndexManifestEntry>& new_index_files) const {
    std::unordered_map<BucketIdentifier, IndexManifestEntry> index_entries;
    for (const auto& entry : prev_index_files) {
        index_entries.insert_or_assign(
            BucketIdentifier(entry.partition, entry.bucket, entry.index_file->IndexType()), entry);
    }

    std::vector<IndexManifestEntry> removed;
    removed.reserve(new_index_files.size());
    std::vector<IndexManifestEntry> added;
    added.reserve(new_index_files.size());

    for (const auto& entry : new_index_files) {
        if (entry.kind == FileKind::Delete()) {
            removed.push_back(entry);
        } else if (entry.kind == FileKind::Add()) {
            added.push_back(entry);
        }
    }

    // The deleted entry is processed first to avoid overwriting a new entry.
    for (const auto& entry : removed) {
        index_entries.erase(
            BucketIdentifier(entry.partition, entry.bucket, entry.index_file->IndexType()));
    }
    for (const auto& entry : added) {
        index_entries.insert_or_assign(
            BucketIdentifier(entry.partition, entry.bucket, entry.index_file->IndexType()), entry);
    }

    std::vector<IndexManifestEntry> result_entries;
    result_entries.reserve(index_entries.size());
    for (const auto& [_, entry] : index_entries) {
        result_entries.push_back(entry);
    }
    return result_entries;
}

Result<std::vector<IndexManifestEntry>> IndexManifestFileHandler::GlobalFileNameCombiner::Combine(
    const std::vector<IndexManifestEntry>& prev_index_files,
    const std::vector<IndexManifestEntry>& new_index_files) const {
    std::map<std::string, IndexManifestEntry> index_entries;
    for (const auto& entry : prev_index_files) {
        index_entries.insert_or_assign(entry.index_file->FileName(), entry);
    }

    std::vector<IndexManifestEntry> removed;
    removed.reserve(new_index_files.size());
    std::vector<IndexManifestEntry> added;
    added.reserve(new_index_files.size());

    for (const auto& entry : new_index_files) {
        if (entry.kind == FileKind::Delete()) {
            removed.push_back(entry);
        } else if (entry.kind == FileKind::Add()) {
            added.push_back(entry);
        }
    }

    // The deleted entry is processed first to avoid overwriting a new entry.
    for (const auto& entry : removed) {
        index_entries.erase(entry.index_file->FileName());
    }
    for (const auto& entry : added) {
        index_entries.insert_or_assign(entry.index_file->FileName(), entry);
    }

    std::vector<IndexManifestEntry> result_entries;
    result_entries.reserve(index_entries.size());
    for (const auto& [_, entry] : index_entries) {
        result_entries.push_back(entry);
    }
    return result_entries;
}

namespace {
using DeletionVectorRanges = LinkedHashMap<std::string, DeletionVectorMeta>;

/// The deletion vectors an index file holds, keyed by the data file each covers. Null when the
/// entry holds none.
const DeletionVectorRanges* GetDeletionVectorRanges(const IndexManifestEntry& entry) {
    const std::optional<DeletionVectorRanges>& dv_ranges = entry.index_file->DvRanges();
    return dv_ranges == std::nullopt ? nullptr : &dv_ranges.value();
}
}  // namespace

Result<std::vector<IndexManifestEntry>>
IndexManifestFileHandler::GlobalDeletionVectorCombiner::Combine(
    const std::vector<IndexManifestEntry>& prev_index_files,
    const std::vector<IndexManifestEntry>& new_index_files) const {
    std::map<std::string, IndexManifestEntry> index_entries;
    std::set<std::string> covered_data_files;
    for (const auto& entry : prev_index_files) {
        index_entries.insert_or_assign(entry.index_file->FileName(), entry);
        const DeletionVectorRanges* dv_ranges = GetDeletionVectorRanges(entry);
        if (dv_ranges == nullptr) {
            continue;
        }
        for (const auto& [data_file, _] : *dv_ranges) {
            covered_data_files.insert(data_file);
        }
    }

    std::vector<const IndexManifestEntry*> removed;
    std::vector<const IndexManifestEntry*> added;
    for (const auto& entry : new_index_files) {
        if (entry.kind == FileKind::Delete()) {
            removed.push_back(&entry);
        } else if (entry.kind == FileKind::Add()) {
            added.push_back(&entry);
        }
    }

    // The deleted entry is processed first, so that an index file taking over the data files of
    // the one it replaces is not rejected as a second vector for them. Paimon Java's
    // GlobalCombiner is order sensitive here; its two sibling combiners are not.
    for (const IndexManifestEntry* entry : removed) {
        const std::string& file_name = entry->index_file->FileName();
        if (index_entries.erase(file_name) == 0) {
            return Status::Invalid(fmt::format(
                "Trying to delete deletion vector index file {} which does not exist.", file_name));
        }
        const DeletionVectorRanges* dv_ranges = GetDeletionVectorRanges(*entry);
        if (dv_ranges == nullptr) {
            continue;
        }
        for (const auto& [data_file, _] : *dv_ranges) {
            if (covered_data_files.erase(data_file) == 0) {
                return Status::Invalid(
                    fmt::format("Trying to delete the deletion vector of data file {}, which does "
                                "not exist.",
                                data_file));
            }
        }
    }
    for (const IndexManifestEntry* entry : added) {
        const std::string& file_name = entry->index_file->FileName();
        if (index_entries.find(file_name) != index_entries.end()) {
            return Status::Invalid(fmt::format(
                "Trying to add deletion vector index file {} which is already added.", file_name));
        }
        const DeletionVectorRanges* dv_ranges = GetDeletionVectorRanges(*entry);
        if (dv_ranges != nullptr) {
            for (const auto& [data_file, _] : *dv_ranges) {
                if (!covered_data_files.insert(data_file).second) {
                    return Status::Invalid(fmt::format(
                        "Trying to add a second deletion vector for data file {}.", data_file));
                }
            }
        }
        index_entries.insert_or_assign(file_name, *entry);
    }

    std::vector<IndexManifestEntry> result_entries;
    result_entries.reserve(index_entries.size());
    for (const auto& [_, entry] : index_entries) {
        result_entries.push_back(entry);
    }
    return result_entries;
}

Result<std::string> IndexManifestFileHandler::Write(
    const std::optional<std::string>& previous_index_manifest,
    const std::vector<IndexManifestEntry>& new_index_entries, int32_t bucket_mode,
    IndexManifestFile* index_manifest_file) {
    std::vector<IndexManifestEntry> entries;
    if (previous_index_manifest != std::nullopt) {
        PAIMON_RETURN_NOT_OK(index_manifest_file->Read(previous_index_manifest.value(),
                                                       /*filter=*/nullptr, &entries));
    }
    for (const auto& entry : entries) {
        if (!(entry.kind == FileKind::Add())) {
            return Status::Invalid("Invalid entry, file kind is not add.");
        }
    }
    std::map<std::string, std::vector<IndexManifestEntry>> previous = SeparateIndexEntries(entries);
    std::map<std::string, std::vector<IndexManifestEntry>> current =
        SeparateIndexEntries(new_index_entries);

    std::set<std::string> index_types;
    for (const auto& [index_type, _] : previous) {
        index_types.insert(index_type);
    }
    for (const auto& [index_type, _] : current) {
        index_types.insert(index_type);
    }

    std::vector<IndexManifestEntry> index_entries;
    index_entries.reserve(previous.size() + current.size());
    for (const auto& index_type : index_types) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<IndexManifestFileHandler::IndexManifestFileCombiner> combiner,
            GetIndexManifestFileCombine(index_type, bucket_mode));
        std::vector<IndexManifestEntry> typed_previous_entries = previous[index_type];
        std::vector<IndexManifestEntry> typed_current_entries = current[index_type];
        PAIMON_ASSIGN_OR_RAISE(std::vector<IndexManifestEntry> combined_entries,
                               combiner->Combine(typed_previous_entries, typed_current_entries));

        index_entries.insert(index_entries.end(), combined_entries.begin(), combined_entries.end());
    }

    std::pair<std::string, int64_t> file_path_and_length;
    PAIMON_ASSIGN_OR_RAISE(file_path_and_length,
                           index_manifest_file->WriteWithoutRolling(index_entries));
    return file_path_and_length.first;
}

std::map<std::string, std::vector<IndexManifestEntry>>
IndexManifestFileHandler::SeparateIndexEntries(
    const std::vector<IndexManifestEntry>& index_entries) {
    std::map<std::string, std::vector<IndexManifestEntry>> result;
    for (const auto& index_entry : index_entries) {
        std::string index_type = index_entry.index_file->IndexType();
        result[index_type].push_back(index_entry);
    }
    return result;
}

Result<std::unique_ptr<IndexManifestFileHandler::IndexManifestFileCombiner>>
IndexManifestFileHandler::GetIndexManifestFileCombine(const std::string& index_type,
                                                      int32_t bucket_mode) {
    if (index_type != DeletionVectorsIndexFile::DELETION_VECTORS_INDEX && index_type != "HASH") {
        return std::make_unique<GlobalFileNameCombiner>();
    }
    // `bucket_mode` is the configured bucket, not a resolved BucketMode, standing in for Paimon
    // Java's BucketMode.BUCKET_UNAWARE check. The two agree on every table that can exist:
    // SchemaValidation rejects the other unaware bucket, 0, and BucketIdCalculator refuses to
    // write the primary key table on which -1 means HASH_DYNAMIC instead. Lifting either
    // restriction means passing the resolved BucketMode here, or a dynamic bucket table would
    // combine its per-bucket deletion vectors by index file name.
    if (index_type == DeletionVectorsIndexFile::DELETION_VECTORS_INDEX && bucket_mode == -1) {
        return std::make_unique<GlobalDeletionVectorCombiner>();
    }
    return std::make_unique<BucketedCombiner>();
}

}  // namespace paimon

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

#include "paimon/core/table/source/data_evolution_full_text_scan.h"

#include <algorithm>
#include <cassert>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/source/index_full_text_search_split_impl.h"
#include "paimon/core/tag/tag.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/core/utils/tag_manager.h"
#include "paimon/defs.h"
#include "paimon/global_index/full_text/full_text_defs.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/global_index/lucene/lucene_defs.h"

namespace paimon {
namespace {
/// The files of one index shard that may rank rows of the searched field.
struct IndexRangeCandidate {
    Range file_range;
    std::string identity;
    /// Whether the searched field is the indexed field of the index, not an extra field.
    bool primary;
    std::vector<std::shared_ptr<IndexFileMeta>> files;
    /// The rows assigned to this shard, merged as they are assigned in ascending order.
    std::vector<Range> assigned;
};

/// Resolves the snapshot to search as Java `TimeTravelUtil#tryTravelOrLatest` does: the one that
/// the time-travel option selects, whatever `scan.mode` is, or else the latest one.
Result<std::optional<Snapshot>> ResolveSnapshot(const CoreOptions& options,
                                                const SnapshotManager& snapshot_manager,
                                                const TagManager& tag_manager) {
    // Java `TimeTravelUtil#SCAN_KEYS`, without `scan.watermark`, which Paimon C++ does not define.
    const char* const time_travel_keys[] = {Options::SCAN_SNAPSHOT_ID, Options::SCAN_TAG_NAME,
                                            Options::SCAN_TIMESTAMP,
                                            Options::SCAN_TIMESTAMP_MILLIS};
    const std::map<std::string, std::string>& raw_options = options.ToMap();
    std::vector<std::string> keys;
    for (const char* key : time_travel_keys) {
        if (raw_options.count(key) > 0) {
            keys.emplace_back(key);
        }
    }
    if (keys.empty()) {
        return snapshot_manager.LatestSnapshot();
    }
    if (keys.size() > 1) {
        return Status::Invalid(
            fmt::format("Only one of the following parameters may be set: [{}], but got [{}]",
                        fmt::join(std::begin(time_travel_keys), std::end(time_travel_keys), ", "),
                        fmt::join(keys, ", ")));
    }
    const std::string& key = keys[0];
    if (key == Options::SCAN_SNAPSHOT_ID) {
        const std::optional<int64_t>& snapshot_id = options.GetScanSnapshotId();
        if (!snapshot_id) {
            return Status::Invalid(fmt::format("invalid {}", key));
        }
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot,
                               snapshot_manager.LoadSnapshot(snapshot_id.value()));
        return std::optional<Snapshot>(std::move(snapshot));
    }
    if (key == Options::SCAN_TAG_NAME) {
        std::optional<std::string> tag_name = options.GetScanTagName();
        if (!tag_name) {
            return Status::Invalid(fmt::format("invalid {}", key));
        }
        PAIMON_ASSIGN_OR_RAISE(Tag tag, tag_manager.GetOrThrow(tag_name.value()));
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, tag.TrimToSnapshot());
        return std::optional<Snapshot>(std::move(snapshot));
    }
    // `CoreOptions` parses both `scan.timestamp` and `scan.timestamp-millis` into milliseconds.
    std::optional<int64_t> timestamp_millis = options.GetScanTimestampMillis();
    if (!timestamp_millis) {
        return Status::Invalid(fmt::format("invalid {}", key));
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot,
                           snapshot_manager.EarlierOrEqualTimeMillis(timestamp_millis.value()));
    if (!snapshot) {
        return Status::Invalid(
            fmt::format("There is currently no snapshot earlier than or equal to timestamp [{}]",
                        timestamp_millis.value()));
    }
    return snapshot;
}

/// Returns whether `left` is chosen before `right` for a row that both cover, as Java
/// `DataEvolutionFullTextScan#compareCandidates` does.
struct CandidateRanksBefore {
    bool operator()(const IndexRangeCandidate* left, const IndexRangeCandidate* right) const {
        if (left->primary != right->primary) {
            return left->primary;
        }
        return std::tie(left->identity, left->file_range.from, left->file_range.to) <
               std::tie(right->identity, right->file_range.from, right->file_range.to);
    }
};
}  // namespace

DataEvolutionFullTextScan::DataEvolutionFullTextScan(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<PredicateFilter>& partition_filter, const DataField& text_field,
    const std::shared_ptr<MemoryPool>& pool)
    : root_path_(root_path),
      table_schema_(table_schema),
      options_(options),
      path_factory_(path_factory),
      partition_filter_(partition_filter),
      text_field_(text_field),
      pool_(pool) {}

bool DataEvolutionFullTextScan::IndexesField(const GlobalIndexMeta& meta, int32_t field_id) {
    if (meta.index_field_id == field_id) {
        return true;
    }
    return meta.extra_field_ids &&
           std::find(meta.extra_field_ids->begin(), meta.extra_field_ids->end(), field_id) !=
               meta.extra_field_ids->end();
}

std::string DataEvolutionFullTextScan::IndexIdentity(const IndexFileMeta& file) {
    const std::optional<GlobalIndexMeta>& meta = file.GetGlobalIndexMeta();
    assert(meta);
    std::vector<int32_t> extra_field_ids = meta->extra_field_ids.value_or(std::vector<int32_t>());
    return fmt::format("{}|{}|[{}]", file.IndexType(), meta->index_field_id,
                       fmt::join(extra_field_ids, ", "));
}

std::vector<DataEvolutionFullTextScan::IndexRangeSelection>
DataEvolutionFullTextScan::ChooseIndexRanges(
    const std::vector<std::shared_ptr<IndexFileMeta>>& index_files, int32_t field_id) {
    // (physical range, identity) -> candidate
    std::map<std::pair<Range, std::string>, IndexRangeCandidate> candidates;
    for (const auto& file : index_files) {
        const std::optional<GlobalIndexMeta>& meta = file->GetGlobalIndexMeta();
        if (!meta || !IndexesField(meta.value(), field_id)) {
            continue;
        }
        Range file_range(meta->row_range_start, meta->row_range_end);
        std::string identity = IndexIdentity(*file);
        auto iter = candidates
                        .try_emplace(std::make_pair(file_range, identity),
                                     IndexRangeCandidate{file_range, identity,
                                                         meta->index_field_id == field_id,
                                                         /*files=*/{}, /*assigned=*/{}})
                        .first;
        iter->second.files.push_back(file);
    }

    // Split the coverage at every boundary: the start of a shard and the row after its end.
    std::set<int64_t> boundaries;
    std::unordered_map<int64_t, std::vector<IndexRangeCandidate*>> starting_at;
    std::unordered_map<int64_t, std::vector<IndexRangeCandidate*>> ending_before;
    for (auto& entry : candidates) {
        IndexRangeCandidate& candidate = entry.second;
        boundaries.insert(candidate.file_range.from);
        starting_at[candidate.file_range.from].push_back(&candidate);
        if (candidate.file_range.to != std::numeric_limits<int64_t>::max()) {
            int64_t after_end = candidate.file_range.to + 1;
            boundaries.insert(after_end);
            ending_before[after_end].push_back(&candidate);
        }
    }
    std::set<IndexRangeCandidate*, CandidateRanksBefore> active;
    for (auto boundary = boundaries.begin(); boundary != boundaries.end(); ++boundary) {
        int64_t from = *boundary;
        for (IndexRangeCandidate* candidate : ending_before[from]) {
            active.erase(candidate);
        }
        for (IndexRangeCandidate* candidate : starting_at[from]) {
            active.insert(candidate);
        }
        if (active.empty()) {
            continue;
        }
        auto next = std::next(boundary);
        int64_t to = next != boundaries.end() ? *next - 1 : std::numeric_limits<int64_t>::max();
        IndexRangeCandidate* best = *active.begin();
        // The pieces are visited in ascending order, so a piece can only extend the last one.
        if (!best->assigned.empty() && best->assigned.back().to == from - 1) {
            best->assigned.back().to = to;
        } else {
            best->assigned.emplace_back(from, to);
        }
    }

    std::vector<IndexRangeSelection> selections;
    for (auto& entry : candidates) {
        IndexRangeCandidate& candidate = entry.second;
        if (candidate.assigned.empty()) {
            continue;
        }
        selections.push_back(IndexRangeSelection{candidate.file_range, candidate.identity,
                                                 std::move(candidate.files),
                                                 std::move(candidate.assigned)});
    }
    std::sort(selections.begin(), selections.end(),
              [](const IndexRangeSelection& left, const IndexRangeSelection& right) {
                  return std::tie(left.file_range.from, left.file_range.to, left.identity) <
                         std::tie(right.file_range.from, right.file_range.to, right.identity);
              });
    return selections;
}

Result<bool> DataEvolutionFullTextScan::ServesFullTextSearch(const std::string& index_type,
                                                             const GlobalIndexer* indexer,
                                                             const std::string& table_path,
                                                             int64_t snapshot_id) {
    if (indexer != nullptr) {
        return indexer->SupportsFullTextSearch();
    }
    if (index_type == full_text::kIdentifier || index_type == lucene::kIdentifier) {
        return Status::NotImplemented(
            fmt::format("cannot search the {} index of table {} in snapshot {}: Paimon C++ is "
                        "built without its global indexer",
                        index_type, table_path, snapshot_id));
    }
    // Any other index type without a registered indexer cannot be read, as in GlobalIndexScan.
    return false;
}

Result<std::shared_ptr<FullTextScan::Plan>> DataEvolutionFullTextScan::CreatePlan() {
    std::string branch = BranchManager::NormalizeBranch(options_.GetBranch());
    SnapshotManager snapshot_manager(options_.GetFileSystem(), root_path_, branch);
    TagManager tag_manager(options_.GetFileSystem(), root_path_, branch);
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot,
                           ResolveSnapshot(options_, snapshot_manager, tag_manager));
    if (!snapshot) {
        return std::make_shared<Plan>(std::nullopt,
                                      std::vector<std::shared_ptr<FullTextSearchSplit>>());
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<IndexManifestFile> index_manifest_file,
        IndexManifestFile::Create(options_.GetFileSystem(), options_.GetManifestFormat(),
                                  options_.GetManifestCompression(), path_factory_,
                                  options_.GetBucket(), pool_, options_));
    IndexFileHandler index_file_handler(options_.GetFileSystem(), std::move(index_manifest_file),
                                        std::make_shared<IndexFilePathFactories>(path_factory_),
                                        options_.DeletionVectorsBitmap64(), pool_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> partition_fields,
                           table_schema_->GetFields(table_schema_->PartitionKeys()));
    auto partition_schema = DataField::ConvertDataFieldsToArrowSchema(partition_fields);
    int32_t field_id = text_field_.Id();
    std::function<Result<bool>(const IndexManifestEntry&)> filter =
        [&](const IndexManifestEntry& entry) -> Result<bool> {
        if (partition_filter_) {
            PAIMON_ASSIGN_OR_RAISE(bool accepted,
                                   partition_filter_->Test(partition_schema, entry.partition));
            if (!accepted) {
                return false;
            }
        }
        const std::optional<GlobalIndexMeta>& meta = entry.index_file->GetGlobalIndexMeta();
        return meta && IndexesField(meta.value(), field_id);
    };
    PAIMON_ASSIGN_OR_RAISE(std::vector<IndexManifestEntry> entries,
                           index_file_handler.Scan(snapshot.value(), filter));

    std::map<std::string, bool> serves_full_text_search;
    std::vector<std::shared_ptr<IndexFileMeta>> full_text_index_files;
    for (const auto& entry : entries) {
        const std::string& index_type = entry.index_file->IndexType();
        auto iter = serves_full_text_search.find(index_type);
        if (iter == serves_full_text_search.end()) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                                   GlobalIndexerFactory::Get(index_type, options_.ToMap()));
            PAIMON_ASSIGN_OR_RAISE(bool served, ServesFullTextSearch(index_type, indexer.get(),
                                                                     root_path_, snapshot->Id()));
            iter = serves_full_text_search.emplace(index_type, served).first;
        }
        if (iter->second) {
            full_text_index_files.push_back(entry.index_file);
        }
    }

    std::vector<std::shared_ptr<FullTextSearchSplit>> splits;
    for (auto& selection : ChooseIndexRanges(full_text_index_files, field_id)) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexFullTextSearchSplitImpl> split,
                               IndexFullTextSearchSplitImpl::Create(
                                   snapshot->Id(), text_field_.Name(), selection.file_range.from,
                                   selection.file_range.to, std::move(selection.files),
                                   std::move(selection.search_ranges)));
        splits.push_back(std::move(split));
    }
    return std::make_shared<Plan>(snapshot->Id(), std::move(splits));
}

}  // namespace paimon

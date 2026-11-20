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

#include "paimon/core/table/source/filtered_row_id_reader.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/global_index/global_index_evaluator_impl.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/table/source/abstract_table_scan.h"
#include "paimon/core/table/source/data_evolution_batch_scan.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/core/utils/tag_manager.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"

namespace paimon {
namespace {
/// Returns whether `predicate` has a leaf that a global index may answer with candidates only.
bool HasCandidateOnlyLeaf(const std::shared_ptr<Predicate>& predicate) {
    if (auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        const auto& children = compound->Children();
        return std::any_of(children.begin(), children.end(), HasCandidateOnlyLeaf);
    }
    if (auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        Function::Type type = leaf->GetFunction().GetType();
        return type == Function::Type::CONTAINS || type == Function::Type::ENDS_WITH ||
               type == Function::Type::LIKE;
    }
    return false;
}
}  // namespace

Result<Snapshot> FilteredRowIdReader::LoadSnapshot(const CoreOptions& options,
                                                   const std::string& root_path,
                                                   int64_t snapshot_id) {
    std::string branch = BranchManager::NormalizeBranch(options.GetBranch());
    SnapshotManager snapshot_manager(options.GetFileSystem(), root_path, branch);
    Result<Snapshot> snapshot = snapshot_manager.LoadSnapshot(snapshot_id);
    if (snapshot.ok()) {
        return snapshot;
    }
    PAIMON_ASSIGN_OR_RAISE(bool exists, snapshot_manager.SnapshotExists(snapshot_id));
    if (exists) {
        return snapshot.status();
    }
    TagManager tag_manager(options.GetFileSystem(), root_path, branch);
    if (std::optional<std::string> name = options.GetScanTagName()) {
        PAIMON_ASSIGN_OR_RAISE(Tag tag, tag_manager.GetOrThrow(*name));
        if (tag.Id() != snapshot_id) {
            return Status::Invalid(fmt::format(
                "cannot read the rows of snapshot {}, as the data scan resolved another one "
                "({} from tag {})",
                snapshot_id, tag.Id(), *name));
        }
        return tag.TrimToSnapshot();
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tag_names, tag_manager.ListTagNames());
    for (const std::string& name : tag_names) {
        // A tag may disappear after listing. Propagate read errors for tags that still exist.
        Result<std::optional<Tag>> tag = tag_manager.Get(name);
        if (!tag.ok()) {
            PAIMON_ASSIGN_OR_RAISE(bool tag_exists,
                                   options.GetFileSystem()->Exists(tag_manager.TagPath(name)));
            if (tag_exists) {
                return tag.status();
            }
            continue;
        }
        if (tag.value() && tag.value()->Id() == snapshot_id) {
            return tag.value()->TrimToSnapshot();
        }
    }
    return Status::NotExist(fmt::format(
        "snapshot {} that the full-text search splits were planned from no longer exists",
        snapshot_id));
}

FilteredRowIdReader::FilteredRowIdReader(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const CoreOptions& options, const std::shared_ptr<PredicateFilter>& partition_filter,
    const std::shared_ptr<Predicate>& filter, int64_t snapshot_id,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool)
    : root_path_(root_path),
      table_schema_(table_schema),
      options_(options),
      partition_filter_(partition_filter),
      filter_(filter),
      snapshot_id_(snapshot_id),
      executor_(executor),
      pool_(pool) {}

Result<bool> FilteredRowIdReader::IsExact(const TableSchema& table_schema,
                                          const std::shared_ptr<Predicate>& filter,
                                          const std::set<int32_t>& contributing_field_ids) {
    PAIMON_ASSIGN_OR_RAISE(std::set<int32_t> filter_field_ids,
                           GlobalIndexEvaluatorImpl::CollectFieldIds(table_schema, filter));
    if (!std::includes(contributing_field_ids.begin(), contributing_field_ids.end(),
                       filter_field_ids.begin(), filter_field_ids.end())) {
        return false;
    }
    return !HasCandidateOnlyLeaf(filter);
}

Result<RoaringBitmap64> FilteredRowIdReader::MatchingRowIds(RoaringBitmap64 rows) const {
    BitmapGlobalIndexResult result(std::move(rows));
    PAIMON_ASSIGN_OR_RAISE(std::vector<Range> ranges, result.ToRanges());
    PAIMON_ASSIGN_OR_RAISE(RowRangeIndex row_ranges, RowRangeIndex::Create(ranges));
    return MatchingRowIds(row_ranges);
}

Result<RoaringBitmap64> FilteredRowIdReader::MatchingRowIds(const RowRangeIndex& row_ranges) const {
    RoaringBitmap64 matching;
    if (row_ranges.Ranges().empty()) {
        return matching;
    }
    std::set<std::string> filter_fields;
    PAIMON_RETURN_NOT_OK(PredicateUtils::GetAllNames(filter_, &filter_fields));
    std::vector<std::string> read_fields;
    for (const DataField& field : table_schema_->Fields()) {
        if (filter_fields.count(field.Name()) > 0) {
            read_fields.push_back(field.Name());
        }
    }
    PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, LoadSnapshot(options_, root_path_, snapshot_id_));
    PAIMON_RETURN_NOT_OK(
        ReadRows(root_path_, table_schema_, options_, partition_filter_, snapshot, row_ranges,
                 read_fields, filter_, executor_, pool_,
                 [&row_ranges, &matching](const arrow::StructArray& /*batch*/,
                                          const arrow::Int64Array& row_ids) -> Status {
                     for (int64_t i = 0; i < row_ids.length(); ++i) {
                         int64_t row_id = row_ids.Value(i);
                         if (row_ranges.Intersects(row_id, row_id)) {
                             matching.Add(row_id);
                         }
                     }
                     return Status::OK();
                 }));
    return matching;
}

Status FilteredRowIdReader::ReadRows(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const CoreOptions& options, const std::shared_ptr<PredicateFilter>& partition_filter,
    const Snapshot& snapshot, const RowRangeIndex& row_ranges,
    const std::vector<std::string>& read_fields, const std::shared_ptr<Predicate>& predicate,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool,
    const BatchConsumer& consumer) {
    const std::string branch = BranchManager::NormalizeBranch(options.GetBranch());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStoreTable> table,
        FileStoreTable::Create(options.GetFileSystem(), root_path, table_schema,
                               CatalogUtils::IdentifierFromPath(root_path, branch), /*uuid=*/"",
                               branch));
    ScanContextBuilder scan_builder(root_path);
    scan_builder.SetOptions(options.ToMap())
        .SetPredicate(partition_filter)
        .WithFileSystem(options.GetFileSystem())
        .WithCache(options.GetCache())
        .WithMemoryPool(pool)
        .WithExecutor(executor);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> table_scan,
                           table->NewScan(std::move(scan_context)));
    auto* data_scan = dynamic_cast<AbstractTableScan*>(table_scan.get());
    if (!data_scan) {
        return Status::Invalid("reading row id ranges requires a native table scan");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan,
                           data_scan->PlanSnapshotRanges(snapshot, row_ranges));
    if (plan->Splits().empty()) {
        return Status::OK();
    }
    if (!table_schema->PrimaryKeys().empty()) {
        return Status::NotImplemented("cannot read full-text row id ranges on a primary-key table");
    }
    if (options.DataEvolutionEnabled()) {
        PAIMON_ASSIGN_OR_RAISE(plan, DataEvolutionBatchScan::WrapToIndexedSplits(
                                         plan, row_ranges, /*id_to_score=*/{}));
    }

    std::vector<std::string> fields = read_fields;
    fields.push_back(SpecialFields::RowId().Name());
    ReadContextBuilder read_builder(root_path);
    read_builder.SetOptions(options.ToMap())
        .SetReadFieldNames(fields)
        .EnablePrefetch(true)
        .WithFileSystem(options.GetFileSystem())
        .WithCache(options.GetCache())
        .WithMemoryPool(pool)
        .WithExecutor(executor);
    if (predicate) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> read_predicate,
                               PredicateUtils::CreatePickedFieldFilter(
                                   predicate, ObjectUtils::CreateIdentifierToIndexMap(fields)));
        read_builder.SetPredicate(read_predicate).EnablePredicateFilter(true);
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                           table->NewRead(std::move(read_context)));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                           table_read->CreateReader(plan->Splits()));
    ScopeGuard guard([&reader]() { reader->Close(); });
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        if (array->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("cannot read rows from a batch that is not a struct");
        }
        auto struct_array = checked_pointer_cast<arrow::StructArray>(array);
        std::shared_ptr<arrow::Array> row_id_array =
            struct_array->GetFieldByName(SpecialFields::RowId().Name());
        if (!row_id_array || row_id_array->type_id() != arrow::Type::INT64) {
            return Status::Invalid(
                fmt::format("cannot read rows without an int64 {}", SpecialFields::RowId().Name()));
        }
        PAIMON_RETURN_NOT_OK(
            consumer(*struct_array, *checked_pointer_cast<arrow::Int64Array>(row_id_array)));
    }
    return Status::OK();
}

}  // namespace paimon

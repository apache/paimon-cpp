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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/catalog/table_load_options.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/global_index_evaluator_impl.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/table/source/append_only_table_read.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/startup_mode.h"
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

/// Returns `options` with the scan pinned to snapshot `snapshot_id`, as Java does. A tag may
/// outlive its snapshot, so once the snapshot has expired a search of a tag reads the tag instead,
/// which the caller must check still refers to the snapshot.
Result<std::map<std::string, std::string>> PinToSnapshot(const std::string& root_path,
                                                         const CoreOptions& options,
                                                         int64_t snapshot_id) {
    std::map<std::string, std::string> pinned = options.ToMap();
    pinned[Options::SCAN_MODE] = StartupMode::FromSnapshot().ToString();
    pinned.erase(Options::SCAN_TIMESTAMP);
    pinned.erase(Options::SCAN_TIMESTAMP_MILLIS);
    if (pinned.count(Options::SCAN_TAG_NAME) > 0) {
        SnapshotManager snapshot_manager(options.GetFileSystem(), root_path,
                                         BranchManager::NormalizeBranch(options.GetBranch()));
        PAIMON_ASSIGN_OR_RAISE(bool exists, snapshot_manager.SnapshotExists(snapshot_id));
        if (!exists) {
            pinned.erase(Options::SCAN_SNAPSHOT_ID);
            return pinned;
        }
        pinned.erase(Options::SCAN_TAG_NAME);
    }
    pinned[Options::SCAN_SNAPSHOT_ID] = std::to_string(snapshot_id);
    return pinned;
}

/// Creates a read of `read_context` with `table_schema` rather than the latest schema, as Java
/// `FilteredRowIdReader` reads with the table that the search loaded, so that a column renamed or
/// replaced since then cannot point the filter at another field.
Result<std::unique_ptr<TableRead>> CreateReadWithSchema(
    const TableLoadOptions& load, std::unique_ptr<ReadContext> read_context,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<Executor>& executor) {
    // Row ids come from row tracking, which a primary-key table does not support.
    if (!table_schema->PrimaryKeys().empty()) {
        return Status::NotImplemented("cannot evaluate a row filter on a primary-key table");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> internal_context,
                           InternalReadContext::Create(std::move(read_context), table_schema,
                                                       options, load.file_system, load.path));
    const CoreOptions& core_options = internal_context->GetCoreOptions();
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> external_path_weights,
                           core_options.GetExternalPathWeights());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> global_index_external_paths,
                           core_options.CreateGlobalIndexExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            internal_context->GetPath(), arrow_schema, table_schema->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_paths, core_options.IndexFileInDataFileDir(),
            external_path_weights, pool));
    return std::unique_ptr<TableRead>(
        std::make_unique<AppendOnlyTableRead>(path_factory, internal_context, pool, executor));
}
}  // namespace

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

Result<RoaringBitmap64> FilteredRowIdReader::MatchingRowIds(const RoaringBitmap64& rows) const {
    RoaringBitmap64 matching;
    if (rows.IsEmpty()) {
        return matching;
    }
    std::map<std::string, std::string> pinned_options;
    PAIMON_ASSIGN_OR_RAISE(pinned_options, PinToSnapshot(root_path_, options_, snapshot_id_));

    // Unlike the read, the scan may load the latest schema, as it selects files by partition and
    // row id alone.
    TableLoadOptions scan_builder_load{root_path_};
    std::map<std::string, std::string> scan_builder_options;
    ScanContextBuilder scan_builder;
    scan_builder_options = pinned_options;
    scan_builder_load.file_system = options_.GetFileSystem();
    scan_builder.SetPredicate(partition_filter_)
        .SetGlobalIndexResult(std::make_shared<BitmapGlobalIndexResult>(rows))
        .WithMemoryPool(pool_)
        .WithExecutor(executor_);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<TableScan> table_scan,
        TableScan::Create(scan_builder_load, std::move(scan_context), scan_builder_options));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
    // An empty plan of another snapshot would drop every candidate, so it is checked too.
    if (plan->SnapshotId() != snapshot_id_) {
        return Status::Invalid(fmt::format(
            "cannot evaluate a row filter on snapshot {}, as the data scan resolved another one",
            snapshot_id_));
    }
    if (plan->Splits().empty()) {
        return matching;
    }

    // The read schema decides the field indexes that a predicate of the read must refer to.
    std::set<std::string> filter_fields;
    PAIMON_RETURN_NOT_OK(PredicateUtils::GetAllNames(filter_, &filter_fields));
    std::vector<std::string> read_fields;
    for (const DataField& field : table_schema_->Fields()) {
        if (filter_fields.count(field.Name()) > 0) {
            read_fields.push_back(field.Name());
        }
    }
    read_fields.push_back(SpecialFields::RowId().Name());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> read_filter,
                           PredicateUtils::CreatePickedFieldFilter(
                               filter_, ObjectUtils::CreateIdentifierToIndexMap(read_fields)));

    TableLoadOptions read_builder_load{root_path_};
    std::map<std::string, std::string> read_builder_options;
    ReadContextBuilder read_builder;
    read_builder_options = pinned_options;
    read_builder_load.file_system = options_.GetFileSystem();
    read_builder.SetReadFieldNames(read_fields)
        .SetPredicate(read_filter)
        .EnablePredicateFilter(true)
        .EnablePrefetch(true)
        .WithMemoryPool(pool_)
        .WithExecutor(executor_);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                           CreateReadWithSchema(read_builder_load, std::move(read_context),
                                                table_schema_, pinned_options, pool_, executor_));
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
            return Status::Invalid("cannot evaluate a row filter on a batch that is not a struct");
        }
        std::shared_ptr<arrow::Array> row_id_array =
            checked_pointer_cast<arrow::StructArray>(array)->GetFieldByName(
                SpecialFields::RowId().Name());
        if (!row_id_array || row_id_array->type_id() != arrow::Type::INT64) {
            return Status::Invalid(fmt::format("cannot evaluate a row filter without an int64 {}",
                                               SpecialFields::RowId().Name()));
        }
        auto row_ids = checked_pointer_cast<arrow::Int64Array>(row_id_array);
        for (int64_t i = 0; i < row_ids->length(); ++i) {
            int64_t row_id = row_ids->Value(i);
            if (rows.Contains(row_id)) {
                matching.Add(row_id);
            }
        }
    }
    return matching;
}

}  // namespace paimon

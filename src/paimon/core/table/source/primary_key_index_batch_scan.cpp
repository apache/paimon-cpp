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

#include "paimon/core/table/source/primary_key_index_batch_scan.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <thread>

#include "fmt/format.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/index/pk/primary_key_index_definitions.h"
#include "paimon/core/table/source/plan_impl.h"
#include "paimon/core/table/source/primary_key_sorted_index_result.h"
#include "paimon/core/table/source/primary_key_sorted_index_scan.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/executor.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate_builder.h"

namespace paimon {
namespace {
Result<std::shared_ptr<Executor>> CreateGlobalIndexExecutor(const CoreOptions& core_options) {
    uint32_t thread_num = std::thread::hardware_concurrency();
    std::optional<int32_t> configured_thread_num = core_options.GetGlobalIndexThreadNum();
    if (configured_thread_num) {
        if (configured_thread_num.value() <= 0) {
            return Status::Invalid(fmt::format("invalid global index thread number {}",
                                               configured_thread_num.value()));
        }
        thread_num = static_cast<uint32_t>(configured_thread_num.value());
    } else if (thread_num == 0) {
        thread_num = 1;
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Executor> executor, CreateDefaultExecutor(thread_num));
    return executor;
}

/// Restricts a predicate to leaves over the indexed fields: an AND keeps its convertible
/// children, an OR is only kept when every child is convertible, and everything else is
/// dropped. A null return means no part of the predicate can use the index.
Result<std::shared_ptr<Predicate>> ProjectToIndexedFields(
    const std::shared_ptr<Predicate>& predicate, const std::set<std::string>& indexed_fields) {
    if (predicate == nullptr) {
        return std::shared_ptr<Predicate>(nullptr);
    }
    if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        if (indexed_fields.count(leaf_predicate->FieldName()) > 0) {
            return predicate;
        }
        return std::shared_ptr<Predicate>(nullptr);
    }
    auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate);
    if (compound_predicate == nullptr) {
        return std::shared_ptr<Predicate>(nullptr);
    }
    bool is_and = compound_predicate->GetFunction().GetType() == Function::Type::AND;
    bool is_or = compound_predicate->GetFunction().GetType() == Function::Type::OR;
    if (!is_and && !is_or) {
        return std::shared_ptr<Predicate>(nullptr);
    }
    std::vector<std::shared_ptr<Predicate>> converted_children;
    for (const std::shared_ptr<Predicate>& child : compound_predicate->Children()) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> converted_child,
                               ProjectToIndexedFields(child, indexed_fields));
        if (converted_child != nullptr) {
            converted_children.push_back(std::move(converted_child));
        } else if (is_or) {
            return std::shared_ptr<Predicate>(nullptr);
        }
    }
    if (converted_children.empty()) {
        return std::shared_ptr<Predicate>(nullptr);
    }
    if (converted_children.size() == 1) {
        return converted_children[0];
    }
    if (is_and) {
        return PredicateBuilder::And(converted_children);
    }
    return PredicateBuilder::Or(converted_children);
}

void FlattenChildren(const std::shared_ptr<CompoundPredicate>& compound_predicate,
                     std::vector<std::shared_ptr<Predicate>>* flattened) {
    for (const std::shared_ptr<Predicate>& child : compound_predicate->Children()) {
        auto compound_child = std::dynamic_pointer_cast<CompoundPredicate>(child);
        if (compound_child != nullptr && compound_child->GetFunction().GetType() ==
                                             compound_predicate->GetFunction().GetType()) {
            FlattenChildren(compound_child, flattened);
        } else {
            flattened->push_back(child);
        }
    }
}

/// A predicate is null-rejecting when it cannot match a row whose tested field is null.
/// Under SQL three-valued logic every comparison and match predicate rejects null; only
/// IS NULL accepts it, and IS NOT NULL is the predicate being pruned.
bool IsNullRejecting(const std::shared_ptr<Predicate>& predicate) {
    auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    if (leaf_predicate == nullptr) {
        return false;
    }
    switch (leaf_predicate->GetFunction().GetType()) {
        case Function::Type::EQUAL:
        case Function::Type::NOT_EQUAL:
        case Function::Type::GREATER_THAN:
        case Function::Type::GREATER_OR_EQUAL:
        case Function::Type::LESS_THAN:
        case Function::Type::LESS_OR_EQUAL:
        case Function::Type::IN:
        case Function::Type::NOT_IN:
        case Function::Type::STARTS_WITH:
        case Function::Type::ENDS_WITH:
        case Function::Type::CONTAINS:
        case Function::Type::LIKE:
            return true;
        default:
            return false;
    }
}

bool IsIsNotNull(const std::shared_ptr<Predicate>& predicate) {
    auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    return leaf_predicate != nullptr &&
           leaf_predicate->GetFunction().GetType() == Function::Type::IS_NOT_NULL;
}

/// Flattens nested same-function compounds and, inside an AND, removes `f IS NOT NULL`
/// leaves made redundant by a null-rejecting sibling on the same field. Pruning must not
/// consider `f IS NULL` as constraining: dropping IS NOT NULL from
/// "f IS NULL AND f IS NOT NULL" would turn the empty result into the set of null rows.
Result<std::shared_ptr<Predicate>> NormalizePredicate(const std::shared_ptr<Predicate>& predicate) {
    auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate);
    if (compound_predicate == nullptr) {
        return predicate;
    }
    std::vector<std::shared_ptr<Predicate>> children;
    FlattenChildren(compound_predicate, &children);

    bool is_and = compound_predicate->GetFunction().GetType() == Function::Type::AND;
    if (is_and) {
        std::set<std::string> constrained_fields;
        for (const std::shared_ptr<Predicate>& child : children) {
            if (IsNullRejecting(child)) {
                constrained_fields.insert(
                    std::dynamic_pointer_cast<LeafPredicate>(child)->FieldName());
            }
        }
        if (!constrained_fields.empty()) {
            std::vector<std::shared_ptr<Predicate>> pruned;
            pruned.reserve(children.size());
            for (const std::shared_ptr<Predicate>& child : children) {
                if (IsIsNotNull(child) &&
                    constrained_fields.count(
                        std::dynamic_pointer_cast<LeafPredicate>(child)->FieldName()) > 0) {
                    continue;
                }
                pruned.push_back(child);
            }
            children = std::move(pruned);
        }
    }

    std::vector<std::shared_ptr<Predicate>> normalized_children;
    normalized_children.reserve(children.size());
    for (const std::shared_ptr<Predicate>& child : children) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> normalized_child,
                               NormalizePredicate(child));
        normalized_children.push_back(std::move(normalized_child));
    }
    if (normalized_children.size() == 1) {
        return normalized_children[0];
    }
    if (is_and) {
        return PredicateBuilder::And(normalized_children);
    }
    return PredicateBuilder::Or(normalized_children);
}
}  // namespace

Result<std::unique_ptr<PrimaryKeyIndexBatchScan>> PrimaryKeyIndexBatchScan::Create(
    const std::shared_ptr<SnapshotReader>& snapshot_reader,
    std::unique_ptr<DataTableBatchScan>&& batch_scan,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(PrimaryKeyIndexDefinitions definitions,
                           PrimaryKeyIndexDefinitions::Create(*table_schema));
    return std::unique_ptr<PrimaryKeyIndexBatchScan>(new PrimaryKeyIndexBatchScan(
        snapshot_reader, std::move(batch_scan), table_schema, path_factory, core_options, pool,
        definitions.ScalarDefinitions()));
}

Result<std::shared_ptr<Plan>> PrimaryKeyIndexBatchScan::CreatePlan() {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> data_plan, batch_scan_->CreatePlan());
    if (!core_options_.GlobalIndexEnabled() || scalar_definitions_.empty() ||
        data_plan->SnapshotId() == std::nullopt || data_plan->Splits().empty()) {
        return data_plan;
    }

    std::set<std::string> indexed_fields;
    std::set<int32_t> indexed_field_ids;
    for (const PrimaryKeyIndexDefinition& definition : scalar_definitions_) {
        indexed_fields.insert(definition.Column());
        indexed_field_ids.insert(definition.FieldId());
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<Predicate> index_predicate,
        ProjectToIndexedFields(batch_scan_->GetNonPartitionPredicate(), indexed_fields));
    if (index_predicate == nullptr) {
        return data_plan;
    }
    PAIMON_ASSIGN_OR_RAISE(index_predicate, NormalizePredicate(index_predicate));

    std::vector<std::shared_ptr<DataSplitImpl>> data_splits;
    data_splits.reserve(data_plan->Splits().size());
    for (const std::shared_ptr<Split>& split : data_plan->Splits()) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        if (data_split == nullptr || data_split->IsStreaming()) {
            return data_plan;
        }
        data_splits.push_back(std::move(data_split));
    }

    int64_t snapshot_id = data_plan->SnapshotId().value();
    const std::shared_ptr<SnapshotManager>& snapshot_manager =
        snapshot_reader_->GetSnapshotManager();
    Result<Snapshot> snapshot_result = snapshot_manager->LoadSnapshot(snapshot_id);
    if (!snapshot_result.ok()) {
        return data_plan;
    }

    const std::unique_ptr<IndexFileHandler>& index_file_handler =
        snapshot_reader_->GetIndexFileHandler();
    if (index_file_handler == nullptr) {
        return data_plan;
    }
    std::function<Result<bool>(const IndexManifestEntry&)> entry_filter =
        [&indexed_field_ids](const IndexManifestEntry& entry) -> Result<bool> {
        if (!(entry.kind == FileKind::Add()) || entry.index_file == nullptr) {
            return false;
        }
        const std::optional<GlobalIndexMeta>& meta = entry.index_file->GetGlobalIndexMeta();
        return meta != std::nullopt && meta.value().source_meta != nullptr &&
               indexed_field_ids.count(meta.value().index_field_id) > 0;
    };
    PAIMON_ASSIGN_OR_RAISE(std::vector<IndexManifestEntry> index_entries,
                           index_file_handler->Scan(snapshot_result.value(), entry_filter));

    PAIMON_ASSIGN_OR_RAISE(PrimaryKeySortedIndexScan::Plan index_plan,
                           PrimaryKeySortedIndexScan::CreatePlan(
                               snapshot_id, data_splits, scalar_definitions_, index_entries));
    bool has_index_group = std::any_of(
        index_plan.Files().begin(), index_plan.Files().end(),
        [](const PrimaryKeySortedIndexScan::FilePlan& file) { return !file.Groups().empty(); });
    if (!has_index_group) {
        return data_plan;
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Executor> executor,
                           CreateGlobalIndexExecutor(core_options_));
    PrimaryKeySortedIndexScan::ReaderFactory reader_factory =
        PrimaryKeySortedIndexScan::MakeReaderFactory(
            core_options_.GetFileSystem(), std::make_shared<IndexFilePathFactories>(path_factory_),
            table_schema_, pool_, executor);
    PAIMON_ASSIGN_OR_RAISE(
        PrimaryKeySortedIndexScan::EvaluatedPlan evaluated_plan,
        PrimaryKeySortedIndexScan::Evaluate(index_plan, table_schema_, index_predicate,
                                            scalar_definitions_, reader_factory));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Split>> splits,
                           PrimaryKeySortedIndexResult::ToSplits(evaluated_plan));
    return std::make_shared<PlanImpl>(data_plan->SnapshotId(), splits);
}

}  // namespace paimon

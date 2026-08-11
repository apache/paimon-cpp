/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "paimon/core/index/pk/primary_key_index_definition.h"
#include "paimon/core/table/source/abstract_table_scan.h"
#include "paimon/core/table/source/data_table_batch_scan.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/result.h"

namespace paimon {
/// Batch scan for primary-key tables with source-backed scalar index definitions.
///
/// Wraps the ordinary batch scan: the data plan is computed first, then the part of the
/// scan predicate that touches indexed fields is evaluated against the validated payload
/// groups of the plan's snapshot, and covered files are narrowed to indexed splits with
/// file-local row ranges. Files without trustworthy coverage keep their normal scan; the
/// reader still applies deletion vectors and the complete original predicate.
class PrimaryKeyIndexBatchScan : public AbstractTableScan {
 public:
    static Result<std::unique_ptr<PrimaryKeyIndexBatchScan>> Create(
        const std::shared_ptr<SnapshotReader>& snapshot_reader,
        std::unique_ptr<DataTableBatchScan>&& batch_scan,
        const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& core_options,
        const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<Plan>> CreatePlan() override;

 private:
    PrimaryKeyIndexBatchScan(const std::shared_ptr<SnapshotReader>& snapshot_reader,
                             std::unique_ptr<DataTableBatchScan>&& batch_scan,
                             const std::shared_ptr<TableSchema>& table_schema,
                             const std::shared_ptr<FileStorePathFactory>& path_factory,
                             const CoreOptions& core_options,
                             const std::shared_ptr<MemoryPool>& pool,
                             std::vector<PrimaryKeyIndexDefinition> scalar_definitions)
        : AbstractTableScan(core_options, snapshot_reader),
          batch_scan_(std::move(batch_scan)),
          table_schema_(table_schema),
          path_factory_(path_factory),
          pool_(pool),
          scalar_definitions_(std::move(scalar_definitions)) {}

    std::unique_ptr<DataTableBatchScan> batch_scan_;
    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::shared_ptr<MemoryPool> pool_;
    std::vector<PrimaryKeyIndexDefinition> scalar_definitions_;
};

}  // namespace paimon

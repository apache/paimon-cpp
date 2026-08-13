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

#include "paimon/core/table/source/append_only_table_read.h"

#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/reader/predicate_batch_reader.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/operation/data_evolution_split_read.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/table/source/append_count_reader.h"
#include "paimon/core/table/source/realtime_split.h"
#include "paimon/realtime/mem_indexer.h"
#include "paimon/status.h"

namespace paimon {
class DataSplit;
class Executor;
class FileStorePathFactory;
class MemoryPool;

AppendOnlyTableRead::AppendOnlyTableRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                                         const std::shared_ptr<InternalReadContext>& context,
                                         const std::shared_ptr<MemoryPool>& memory_pool,
                                         const std::shared_ptr<Executor>& executor)
    : TableRead(memory_pool), context_(context) {
    const auto& core_options = context->GetCoreOptions();
    if (core_options.DataEvolutionEnabled()) {
        // add data evolution first
        split_reads_.push_back(
            std::make_unique<DataEvolutionSplitRead>(path_factory, context, memory_pool, executor));
    } else {
        split_reads_.push_back(
            std::make_unique<RawFileSplitRead>(path_factory, context, memory_pool, executor));
    }
}

Result<std::unique_ptr<BatchReader>> AppendOnlyTableRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    std::shared_ptr<RealtimeSplit> realtime_split = std::dynamic_pointer_cast<RealtimeSplit>(split);
    if (!realtime_split) {
        return CreateDiskReader(split);
    }

    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.reserve(realtime_split->DiskSplits().size() + 1);
    for (const std::shared_ptr<Split>& disk_split : realtime_split->DiskSplits()) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> disk_reader,
                               CreateDiskReader(disk_split));
        readers.push_back(std::move(disk_reader));
    }

    auto c_read_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportSchema(*context_->GetReadSchema(), c_read_schema.get()));
    ScopeGuard schema_guard([schema = c_read_schema.get()]() { ArrowSchemaRelease(schema); });
    MemQueryContext query_context{c_read_schema.get(), context_->GetPredicate(),
                                  /*enable_predicate_pushdown=*/true};
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::unique_ptr<BatchReader>> memory_readers,
        realtime_split->Indexer()->CreateQueryReaders(
            realtime_split->ReadView(), realtime_split->CommittedOffset(), query_context));

    for (std::unique_ptr<BatchReader>& memory_reader : memory_readers) {
        if (context_->EnablePredicateFilter() && context_->GetPredicate()) {
            PAIMON_ASSIGN_OR_RAISE(memory_reader, PredicateBatchReader::Create(
                                                      std::move(memory_reader),
                                                      context_->GetPredicate(), GetMemoryPool()));
        }
        readers.push_back(std::move(memory_reader));
    }
    return std::make_unique<ConcatBatchReader>(std::move(readers), GetMemoryPool());
}

Result<std::unique_ptr<BatchReader>> AppendOnlyTableRead::CreateDiskReader(
    const std::shared_ptr<Split>& split) {
    for (const auto& read : split_reads_) {
        PAIMON_ASSIGN_OR_RAISE(bool matched, read->Match(split, /*force_keep_delete=*/false));
        if (matched) {
            return read->CreateReader(split);
        }
    }
    return Status::Invalid("create reader failed, not read match with split.");
}

Result<std::unique_ptr<CountReader>> AppendOnlyTableRead::CreateCountReader(
    const std::vector<std::shared_ptr<Split>>& splits) {
    for (const std::shared_ptr<Split>& split : splits) {
        if (std::dynamic_pointer_cast<RealtimeSplit>(split)) {
            return Status::NotImplemented(
                "CreateCountReader does not support process-local real-time splits");
        }
    }
    if (context_->GetPredicate() != nullptr) {
        return Status::NotImplemented(
            "CreateCountReader with predicate pushdown is not supported yet");
    }

    return std::make_unique<AppendCountReader>(splits, context_->GetCoreOptions().GetFileSystem(),
                                               GetMemoryPool());
}

}  // namespace paimon

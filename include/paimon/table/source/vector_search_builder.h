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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/global_index/global_index_result.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"
#include "paimon/table/source/vector_read.h"
#include "paimon/table/source/vector_scan.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class Executor;
class FileStoreTable;

/// Searches indexed, live rows of a data-evolution table. When scalar indexes cannot evaluate
/// the filter, each shard supplies candidates for checking against the planned snapshot.
class PAIMON_EXPORT VectorSearchBuilder {
 public:
    explicit VectorSearchBuilder(const std::string& table_path);
    ~VectorSearchBuilder();

    /// Supplies the primary index column, vector and query-time options passed to the backend.
    VectorSearchBuilder& SetQuery(const std::string& field, const std::vector<float>& vector,
                                  const std::map<std::string, std::string>& options = {});
    /// Sets the positive maximum number of results.
    VectorSearchBuilder& SetLimit(int32_t limit);
    /// Intersects the current partition selection with the union of the given specs on each call.
    /// An empty list adds no restriction.
    VectorSearchBuilder& WithPartitionFilter(
        const std::vector<std::map<std::string, std::string>>& partitions);
    /// Adds a conjunct to the row filter.
    VectorSearchBuilder& WithFilter(const std::shared_ptr<Predicate>& predicate);
    /// Sets table options such as scan.snapshot-id and scan.tag-name.
    /// A builder created by a Table uses that table's branch.
    VectorSearchBuilder& SetOptions(const std::map<std::string, std::string>& options);
    VectorSearchBuilder& WithFileSystem(const std::shared_ptr<FileSystem>& file_system);
    /// The pool must support concurrent allocations when searching multiple shards.
    VectorSearchBuilder& WithMemoryPool(const std::shared_ptr<MemoryPool>& pool);
    /// Sets the executor for shard searches and data reads. A reused plan keeps its executor.
    VectorSearchBuilder& WithExecutor(const std::shared_ptr<Executor>& executor);

    Result<std::unique_ptr<VectorScan>> NewVectorScan() const;
    Result<std::unique_ptr<VectorRead>> NewVectorRead() const;
    Result<std::shared_ptr<ScoredGlobalIndexResult>> ExecuteLocal() const;

 private:
    friend class FileStoreTable;
    class Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace paimon

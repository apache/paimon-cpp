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
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class Executor;
class FileStoreTable;

/// Combines vector and full-text searches on one snapshot of a data-evolution table.
/// Each route has its own candidate limit.
class PAIMON_EXPORT HybridSearchBuilder {
 public:
    explicit HybridSearchBuilder(const std::string& table_path);
    ~HybridSearchBuilder();

    /// Intersects the current partition selection with the union of the given specs on each call.
    /// An empty list adds no restriction.
    HybridSearchBuilder& WithPartitionFilter(
        const std::vector<std::map<std::string, std::string>>& partitions);
    /// Adds a conjunct to the row filter applied by every route.
    HybridSearchBuilder& WithFilter(const std::shared_ptr<Predicate>& predicate);
    /// Adds a vector route with a finite positive weight and optional backend query options.
    HybridSearchBuilder& AddVectorRoute(const std::string& column, const std::vector<float>& vector,
                                        int32_t limit, float weight = 1.0f,
                                        const std::map<std::string, std::string>& options = {});
    /// Passes the query unchanged to the full-text index. The weight must be finite and positive,
    /// and the route options must be empty.
    HybridSearchBuilder& AddFullTextRoute(const std::string& field, const std::string& query,
                                          int32_t limit, float weight = 1.0f,
                                          const std::map<std::string, std::string>& options = {});
    /// Sets the positive limit on the fused results.
    HybridSearchBuilder& SetLimit(int32_t limit);
    /// Selects rrf (default), weighted_score or mrr.
    HybridSearchBuilder& WithRanker(const std::string& ranker);
    HybridSearchBuilder& WithRrfRanker();
    HybridSearchBuilder& WithWeightedScoreRanker();

    /// Replaces table option overrides, including the snapshot, tag or timestamp to search.
    /// A builder created by a Table uses that table's branch.
    HybridSearchBuilder& SetOptions(const std::map<std::string, std::string>& options);
    HybridSearchBuilder& WithFileSystem(const std::shared_ptr<FileSystem>& file_system);
    /// The pool must support concurrent allocations when searching multiple shards.
    HybridSearchBuilder& WithMemoryPool(const std::shared_ptr<MemoryPool>& pool);
    /// Sets the executor for shard searches and data reads.
    HybridSearchBuilder& WithExecutor(const std::shared_ptr<Executor>& executor);

    /// Returns the fused scores in ascending row-id order, suitable for SetGlobalIndexResult().
    Result<std::shared_ptr<ScoredGlobalIndexResult>> ExecuteLocal() const;

 private:
    friend class FileStoreTable;
    class Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace paimon

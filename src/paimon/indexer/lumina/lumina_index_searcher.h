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

#pragma once

#include <map>
#include <memory>
#include <string>

#include "lumina/api/LuminaSearcher.h"
#include "lumina/extensions/SearchWithFilterExtension.h"
#include "lumina/extensions/experimental/SearchWithTagExtension.h"
#include "paimon/indexer/lumina/lumina_index_options.h"
#include "paimon/indexer/lumina/lumina_memory_pool.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/result.h"

namespace paimon {
class InputStream;
}

namespace paimon::lumina {

/// Owns an opened Lumina searcher and its attached search extensions.
class LuminaIndexSearcher {
 public:
    /// Create and open a Lumina searcher.
    ///
    /// @param lumina_options Normalized Lumina options without the `lumina.` prefix.
    /// @param index_info Information about the built index.
    /// @param input Input stream containing the Lumina index.
    /// @param pool Memory pool used by Lumina.
    /// @return An opened searcher, or an error Status.
    static Result<std::unique_ptr<LuminaIndexSearcher>> Open(
        const std::map<std::string, std::string>& lumina_options, const LuminaIndexInfo& index_info,
        const std::shared_ptr<InputStream>& input, const std::shared_ptr<LuminaMemoryPool>& pool);

    ~LuminaIndexSearcher();

    LuminaIndexSearcher(const LuminaIndexSearcher&) = delete;
    LuminaIndexSearcher& operator=(const LuminaIndexSearcher&) = delete;

    /// Execute a vector search against the opened index.
    ///
    /// @param vector_search Vector search request.
    /// @return Native Lumina search result, or an error Status.
    Result<::lumina::api::LuminaSearcher::SearchResult> Search(
        const std::shared_ptr<VectorSearch>& vector_search) const;

 private:
    LuminaIndexSearcher(const LuminaIndexInfo& index_info,
                        const std::shared_ptr<LuminaMemoryPool>& pool,
                        std::unique_ptr<::lumina::api::LuminaSearcher>&& searcher);

    LuminaIndexInfo index_info_;
    std::shared_ptr<LuminaMemoryPool> pool_;
    std::unique_ptr<::lumina::extensions::SearchWithFilterExtension> searcher_with_filter_;
    std::unique_ptr<::lumina::extensions::experimental::SearchWithTagExtension> searcher_with_tag_;
    std::unique_ptr<::lumina::api::LuminaSearcher> searcher_;
};

}  // namespace paimon::lumina

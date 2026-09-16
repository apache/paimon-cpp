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

struct LuminaSearchExtensions {
    std::unique_ptr<::lumina::extensions::SearchWithFilterExtension> searcher_with_filter;
    std::unique_ptr<::lumina::extensions::experimental::SearchWithTagExtension> searcher_with_tag;
};

struct LuminaSearcherWithExtensions {
    std::unique_ptr<::lumina::api::LuminaSearcher> searcher;
    LuminaSearchExtensions extensions;
};

/// Shared Lumina vector-search dispatch for Global Index and File Index readers.
class LuminaSearchUtils {
 public:
    LuminaSearchUtils() = delete;
    ~LuminaSearchUtils() = delete;

    static Result<LuminaSearcherWithExtensions> OpenSearcher(
        const std::map<std::string, std::string>& lumina_options, const LuminaIndexInfo& index_info,
        const std::shared_ptr<InputStream>& input, LuminaMemoryPool* pool);

    static Result<::lumina::api::LuminaSearcher::SearchResult> ExecuteVectorSearch(
        ::lumina::api::LuminaSearcher& searcher,
        ::lumina::extensions::SearchWithFilterExtension& searcher_with_filter,
        ::lumina::extensions::experimental::SearchWithTagExtension* searcher_with_tag,
        const std::shared_ptr<VectorSearch>& vector_search, const LuminaIndexInfo& index_info,
        LuminaMemoryPool& pool);

 private:
    /// Attach the mandatory function filter and the configured optional tag extension.
    static Result<LuminaSearchExtensions> AttachSearchExtensions(
        ::lumina::api::LuminaSearcher& searcher, const LuminaIndexInfo& index_info);
};

}  // namespace paimon::lumina

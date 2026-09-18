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

#include "paimon/indexer/lumina/lumina_search_utils.h"

#include <utility>

#include "fmt/format.h"
#include "paimon/indexer/lumina/lumina_file_reader.h"
#include "paimon/indexer/lumina/lumina_tag_utils.h"
#include "paimon/indexer/lumina/lumina_utils.h"
#include "paimon/status.h"

namespace paimon::lumina {

Result<LuminaSearchExtensions> LuminaSearchUtils::AttachSearchExtensions(
    ::lumina::api::LuminaSearcher& searcher, const LuminaIndexInfo& index_info) {
    auto searcher_with_filter = std::make_unique<::lumina::extensions::SearchWithFilterExtension>();
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(searcher.Attach(*searcher_with_filter));

    std::unique_ptr<::lumina::extensions::experimental::SearchWithTagExtension> searcher_with_tag;
    if (index_info.has_tag) {
        searcher_with_tag =
            std::make_unique<::lumina::extensions::experimental::SearchWithTagExtension>();
        PAIMON_RETURN_NOT_OK_FROM_LUMINA(searcher.Attach(*searcher_with_tag));
    }
    return LuminaSearchExtensions{std::move(searcher_with_filter), std::move(searcher_with_tag)};
}

Result<LuminaSearcherWithExtensions> LuminaSearchUtils::OpenSearcher(
    const std::map<std::string, std::string>& lumina_options, const LuminaIndexInfo& index_info,
    const std::shared_ptr<InputStream>& input, LuminaMemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(::lumina::api::SearcherOptions searcher_options,
                           LuminaIndexOptions::CreateSearcherOptions(lumina_options, index_info));
    ::lumina::core::MemoryResourceConfig memory_resource(pool);
    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::LuminaSearcher lumina_searcher,
        ::lumina::api::LuminaSearcher::Create(searcher_options, memory_resource));
    auto searcher = std::make_unique<::lumina::api::LuminaSearcher>(std::move(lumina_searcher));
    auto file_reader = std::make_unique<LuminaFileReader>(input);
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(
        searcher->Open(std::move(file_reader), ::lumina::api::IOOptions()));
    if (searcher->GetMeta().dim != index_info.dimension) {
        return Status::Invalid(
            fmt::format("Lumina index dimension {} mismatch expected dimension {}",
                        searcher->GetMeta().dim, index_info.dimension));
    }
    PAIMON_ASSIGN_OR_RAISE(LuminaSearchExtensions extensions,
                           AttachSearchExtensions(*searcher, index_info));
    return LuminaSearcherWithExtensions{std::move(searcher), std::move(extensions)};
}

Result<::lumina::api::LuminaSearcher::SearchResult> LuminaSearchUtils::ExecuteVectorSearch(
    ::lumina::api::LuminaSearcher& searcher,
    ::lumina::extensions::SearchWithFilterExtension& searcher_with_filter,
    ::lumina::extensions::experimental::SearchWithTagExtension* searcher_with_tag,
    const std::shared_ptr<VectorSearch>& vector_search, const LuminaIndexInfo& index_info,
    LuminaMemoryPool& pool) {
    if (!vector_search) {
        return Status::Invalid("Lumina vector search must not be null");
    }
    PAIMON_ASSIGN_OR_RAISE(::lumina::api::SearchOptions search_options,
                           LuminaIndexOptions::CreateSearchOptions(*vector_search, index_info));
    ::lumina::api::Query query(vector_search->query.data(), vector_search->query.size());

    if (vector_search->predicate) {
        if (!searcher_with_tag) {
            return Status::Invalid("lumina index was not built with tag");
        }
        PAIMON_ASSIGN_OR_RAISE(::lumina::extensions::experimental::TagFilter tag_filter,
                               LuminaTagUtils::PredicateToTagFilter(vector_search->predicate));
        if (!vector_search->pre_filter) {
            PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
                ::lumina::api::LuminaSearcher::SearchResult search_result,
                searcher_with_tag->SearchWithTag(query, tag_filter, search_options, pool));
            return std::move(search_result);
        }
        auto filter = [pre_filter = vector_search->pre_filter](
                          ::lumina::core::vector_id_t id) -> bool { return pre_filter(id); };
        PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
            ::lumina::api::LuminaSearcher::SearchResult search_result,
            searcher_with_tag->SearchWithTagAndFilter(query, tag_filter, filter, search_options,
                                                      pool));
        return std::move(search_result);
    }

    if (!vector_search->pre_filter) {
        PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
            ::lumina::api::LuminaSearcher::SearchResult search_result,
            searcher.Search(query, search_options, pool));
        return std::move(search_result);
    }
    auto filter = [pre_filter = vector_search->pre_filter](::lumina::core::vector_id_t id) -> bool {
        return pre_filter(id);
    };
    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::LuminaSearcher::SearchResult search_result,
        searcher_with_filter.SearchWithFilter(query, filter, search_options, pool));
    return std::move(search_result);
}

}  // namespace paimon::lumina

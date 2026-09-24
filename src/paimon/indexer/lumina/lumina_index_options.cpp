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

#include "paimon/indexer/lumina/lumina_index_options.h"

#include <unordered_map>

#include "fmt/format.h"
#include "lumina/api/OptionsNormalize.h"
#include "lumina/core/Constants.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/indexer/lumina/lumina_utils.h"

namespace paimon::lumina {

Result<uint32_t> LuminaIndexOptions::GetDimension(
    const std::map<std::string, std::string>& lumina_options) {
    return OptionsUtils::GetValueFromMap<uint32_t>(lumina_options,
                                                   std::string(::lumina::core::kDimension));
}

Result<LuminaIndexInfo> LuminaIndexOptions::GetIndexInfo(
    const std::map<std::string, std::string>& lumina_options) {
    PAIMON_ASSIGN_OR_RAISE(uint32_t dimension, GetDimension(lumina_options));
    PAIMON_ASSIGN_OR_RAISE(std::string index_type,
                           OptionsUtils::GetValueFromMap<std::string>(
                               lumina_options, std::string(::lumina::core::kIndexType)));
    PAIMON_ASSIGN_OR_RAISE(std::string distance_type_str,
                           OptionsUtils::GetValueFromMap<std::string>(
                               lumina_options, std::string(::lumina::core::kDistanceMetric)));

    VectorSearch::DistanceType distance_type = VectorSearch::DistanceType::UNKNOWN;
    if (distance_type_str == ::lumina::core::kDistanceL2) {
        distance_type = VectorSearch::DistanceType::EUCLIDEAN;
    } else if (distance_type_str == ::lumina::core::kDistanceCosine) {
        distance_type = VectorSearch::DistanceType::COSINE;
    } else if (distance_type_str == ::lumina::core::kDistanceInnerProduct) {
        distance_type = VectorSearch::DistanceType::INNER_PRODUCT;
    }
    if (distance_type == VectorSearch::DistanceType::UNKNOWN) {
        return Status::Invalid(
            fmt::format("invalid distance type {} for lumina", distance_type_str));
    }

    bool has_tag = lumina_options.find(std::string(::lumina::core::kExtensionTagSchema)) !=
                   lumina_options.end();
    return LuminaIndexInfo{dimension, std::move(index_type), distance_type, has_tag};
}

Result<::lumina::api::BuilderOptions> LuminaIndexOptions::CreateBuilderOptions(
    const std::map<std::string, std::string>& lumina_options) {
    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::BuilderOptions builder_options,
        ::lumina::api::NormalizeBuilderOptions(std::unordered_map<std::string, std::string>(
            lumina_options.begin(), lumina_options.end())));
    return builder_options;
}

bool LuminaIndexOptions::IsCheckpointEnabled(
    const std::map<std::string, std::string>& lumina_options) {
    return lumina_options.count(std::string(::lumina::core::kExtensionCkptThreshold)) != 0 ||
           lumina_options.count(std::string(::lumina::core::kExtensionCkptCount)) != 0;
}

Result<::lumina::api::SearcherOptions> LuminaIndexOptions::CreateSearcherOptions(
    const std::map<std::string, std::string>& lumina_options, const LuminaIndexInfo& index_info) {
    std::map<std::string, std::string> options = lumina_options;
    options[std::string(::lumina::core::kDimension)] = std::to_string(index_info.dimension);
    options[std::string(::lumina::core::kIndexType)] = index_info.index_type;
    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::SearcherOptions searcher_options,
        ::lumina::api::NormalizeSearcherOptions(
            std::unordered_map<std::string, std::string>(options.begin(), options.end())));
    return searcher_options;
}

Result<::lumina::api::SearchOptions> LuminaIndexOptions::CreateSearchOptions(
    const VectorSearch& vector_search, const LuminaIndexInfo& index_info) {
    if (vector_search.distance_type &&
        vector_search.distance_type.value() != index_info.distance_type) {
        return Status::Invalid("distance type for index and search not match");
    }
    if (vector_search.query.size() != index_info.dimension) {
        return Status::Invalid("dimension for index and search not match");
    }

    std::map<std::string, std::string> lumina_options = OptionsUtils::FetchOptionsWithPrefix(
        LuminaDefines::kOptionKeyPrefix, vector_search.options);
    auto index_type_iter = lumina_options.find(std::string(::lumina::core::kIndexType));
    if (index_type_iter != lumina_options.end() &&
        index_type_iter->second != index_info.index_type) {
        return Status::Invalid("index type for index and search not match");
    }

    lumina_options[std::string(::lumina::core::kTopK)] = std::to_string(vector_search.limit);
    lumina_options[std::string(::lumina::core::kSearchThreadSafeFilter)] = "true";
    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::SearchOptions search_options,
        ::lumina::api::NormalizeSearchOptions(index_info.index_type,
                                              std::unordered_map<std::string, std::string>(
                                                  lumina_options.begin(), lumina_options.end())));
    return search_options;
}

}  // namespace paimon::lumina

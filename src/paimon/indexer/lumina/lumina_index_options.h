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

#include <cstdint>
#include <map>
#include <string>

#include "lumina/api/Options.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/result.h"

namespace paimon::lumina {

struct LuminaIndexInfo {
    uint32_t dimension;
    std::string index_type;
    VectorSearch::DistanceType distance_type;
    bool has_tag;
};

/// Shared parsing and validation for normalized Lumina options.
///
/// Input maps use native Lumina keys such as `index.dimension`. Callers remain responsible for
/// stripping their own configuration namespace (for example, the `lumina.` Global Index prefix).
class LuminaIndexOptions {
 public:
    LuminaIndexOptions() = delete;
    ~LuminaIndexOptions() = delete;

    static Result<uint32_t> GetDimension(const std::map<std::string, std::string>& lumina_options);

    static Result<LuminaIndexInfo> GetIndexInfo(
        const std::map<std::string, std::string>& lumina_options);

    static Result<::lumina::api::BuilderOptions> CreateBuilderOptions(
        const std::map<std::string, std::string>& lumina_options);

    static Result<::lumina::api::SearcherOptions> CreateSearcherOptions(
        const std::map<std::string, std::string>& lumina_options,
        const LuminaIndexInfo& index_info);

    static Result<::lumina::api::SearchOptions> CreateSearchOptions(
        const VectorSearch& vector_search, const LuminaIndexInfo& index_info);
};

}  // namespace paimon::lumina

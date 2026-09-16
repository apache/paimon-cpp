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
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "lumina/extensions/experimental/DatasetWithTag.h"
#include "lumina/extensions/experimental/TagFilter.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon::lumina {

struct LuminaTagField {
    enum class Type {
        ENUM,
        RANGE,
    };

    enum class ValueType {
        INT32,
        INT64,
        FLOAT,
        DOUBLE,
        STRING,
    };

    std::string name;
    Type type;
    ValueType value_type;
};

/// Shared tag option, Arrow conversion, and predicate conversion helpers for Lumina indexes.
class LuminaTagUtils {
 public:
    LuminaTagUtils() = delete;
    ~LuminaTagUtils() = delete;

    static Result<std::vector<LuminaTagField>> ParseTagSchema(
        const std::map<std::string, std::string>& lumina_options);

    static Result<std::optional<std::vector<std::string>>> GetExtraFieldNames(
        const std::map<std::string, std::string>& lumina_options);

    static Status ValidateTagFields(const arrow::StructType& struct_type,
                                    const std::vector<LuminaTagField>& tag_fields);

    static Result<std::vector<::lumina::extensions::experimental::TagDimensionData>>
    ExtractTagDataForSegment(const std::shared_ptr<arrow::StructArray>& struct_array,
                             const std::vector<LuminaTagField>& tag_fields, int64_t segment_start,
                             int64_t segment_len);

    static Result<::lumina::extensions::experimental::TagFilter> PredicateToTagFilter(
        const std::shared_ptr<Predicate>& predicate);
};

}  // namespace paimon::lumina

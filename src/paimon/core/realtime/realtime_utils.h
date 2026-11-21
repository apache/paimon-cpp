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
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/core/core_options.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

class RealtimeUtils {
 public:
    RealtimeUtils() = delete;
    ~RealtimeUtils() = delete;

    static Status ValidateBranch(const std::string& branch);

    static Result<int32_t> GetDeduplicateKeyPosition(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<std::string>& deduplicate_key_fields);

    PAIMON_EXPORT static Status ValidateOffsetField(const std::shared_ptr<arrow::Schema>& schema);

    static Status ValidateDeduplicateSchema(const std::shared_ptr<arrow::Schema>& schema,
                                            const std::vector<std::string>& deduplicate_key_fields);

    static Status ValidateDeduplicateFileIndex(const CoreOptions& options);
};

}  // namespace paimon

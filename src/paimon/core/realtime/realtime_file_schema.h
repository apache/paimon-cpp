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
#include <vector>

#include "arrow/type_fwd.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace paimon {
class SchemaManager;
class RoaringBitmap64;
struct DataFileMeta;

/// Resolves immutable file schemas and offset statistics without interpreting old columns using
/// the current table layout. SchemaManager provides the shared, thread-safe schema cache.
class RealtimeFileSchema {
 public:
    RealtimeFileSchema(const std::shared_ptr<arrow::Schema>& schema, int64_t schema_id,
                       const std::shared_ptr<SchemaManager>& schema_manager);

    Result<std::shared_ptr<arrow::Schema>> GetFileSchema(const DataFileMeta& file) const;

    /// Verifies the historical deduplicate definition and key encoding before opening a sidecar.
    Status ValidateKey(const DataFileMeta& file,
                       const std::shared_ptr<arrow::Field>& key_field) const;

    /// Returns offsets inside the file's inclusive min/max range. Missing or unusable statistics
    /// conservatively return all offsets; missing or incompatible schemas fail the read.
    Result<std::vector<Literal>> SelectOffsets(const DataFileMeta& file,
                                               const RoaringBitmap64& offsets) const;

 private:
    std::shared_ptr<arrow::Schema> schema_;
    int64_t schema_id_;
    std::shared_ptr<SchemaManager> schema_manager_;
};
}  // namespace paimon

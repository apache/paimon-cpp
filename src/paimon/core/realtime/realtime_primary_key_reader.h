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

#include <memory>
#include <vector>

#include "arrow/type_fwd.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/realtime/offset_range.h"
#include "paimon/result.h"

namespace paimon {
class BatchReader;
class MemoryPool;
class RealtimeStoreReadPipeline;

/// Creates the Arrow schema used for PK realtime transport batches.
class RealtimePrimaryKeyLayout {
 public:
    RealtimePrimaryKeyLayout() = delete;
    ~RealtimePrimaryKeyLayout() = delete;

    /// Creates `_VALUE_KIND`, `_SEQUENCE_NUMBER`, then value fields.
    static std::shared_ptr<arrow::Schema> CreateSchema(
        const std::vector<std::shared_ptr<arrow::Field>>& value_fields);
};

class RealtimePrimaryKeyReaderFactory {
 public:
    RealtimePrimaryKeyReaderFactory() = delete;
    ~RealtimePrimaryKeyReaderFactory() = delete;

    static Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CreateForCommit(
        std::vector<std::unique_ptr<BatchReader>>&& readers,
        const std::shared_ptr<arrow::Schema>& key_schema,
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::shared_ptr<MemoryPool>& memory_pool);

    static Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CreateForQuery(
        std::vector<std::unique_ptr<BatchReader>>&& readers, const OffsetRange& visible_offsets,
        const std::shared_ptr<arrow::Schema>& key_schema,
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::shared_ptr<MemoryPool>& memory_pool, const RealtimeStoreReadPipeline& pipeline);
};

}  // namespace paimon

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

#include "paimon/core/realtime/realtime_primary_key_reader.h"

#include <memory>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/key_value_data_file_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/core/realtime/realtime_store_read_pipeline.h"
#include "paimon/status.h"

namespace paimon {
namespace {

template <typename Reader>
void CloseReaders(const std::vector<std::unique_ptr<Reader>>& readers) {
    for (const std::unique_ptr<Reader>& reader : readers) {
        if (reader) {
            reader->Close();
        }
    }
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CreateKeyValueReaders(
    std::vector<std::unique_ptr<BatchReader>>&& readers,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    std::vector<std::unique_ptr<KeyValueRecordReader>> result;
    result.reserve(readers.size());
    ScopeGuard readers_guard([&readers, &result]() {
        CloseReaders(readers);
        CloseReaders(result);
    });
    for (std::unique_ptr<BatchReader>& reader : readers) {
        if (!reader) {
            return Status::Invalid("real-time store returned a null reader");
        }
        result.push_back(std::make_unique<KeyValueDataFileRecordReader>(
            std::move(reader), key_schema, value_schema,
            /*level=*/KeyValue::UNKNOWN_LEVEL, memory_pool));
    }
    readers_guard.Release();
    return result;
}

}  // namespace

std::shared_ptr<arrow::Schema> RealtimePrimaryKeyLayout::CreateSchema(
    const std::vector<std::shared_ptr<arrow::Field>>& value_fields) {
    arrow::FieldVector fields = {
        DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())->WithNullable(false),
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())
            ->WithNullable(false)};
    fields.insert(fields.end(), value_fields.begin(), value_fields.end());
    return arrow::schema(std::move(fields));
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>>
RealtimePrimaryKeyReaderFactory::CreateForCommit(
    std::vector<std::unique_ptr<BatchReader>>&& readers,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    return CreateKeyValueReaders(std::move(readers), key_schema, value_schema, memory_pool);
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>>
RealtimePrimaryKeyReaderFactory::CreateForQuery(std::vector<std::unique_ptr<BatchReader>>&& readers,
                                                const OffsetRange& visible_offsets,
                                                const std::shared_ptr<arrow::Schema>& key_schema,
                                                const std::shared_ptr<arrow::Schema>& value_schema,
                                                const std::shared_ptr<MemoryPool>& memory_pool,
                                                const RealtimeStoreReadPipeline& pipeline) {
    ScopeGuard readers_guard([&readers]() { CloseReaders(readers); });
    for (std::unique_ptr<BatchReader>& reader : readers) {
        if (!reader) {
            return Status::Invalid("real-time store returned a null reader");
        }
        PAIMON_ASSIGN_OR_RAISE(reader, pipeline.Wrap(std::move(reader), visible_offsets));
    }
    readers_guard.Release();
    return CreateKeyValueReaders(std::move(readers), key_schema, value_schema, memory_pool);
}

}  // namespace paimon

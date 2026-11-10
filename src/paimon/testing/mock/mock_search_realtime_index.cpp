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

#include "paimon/testing/mock/mock_search_realtime_index.h"

#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/realtime/realtime_utils.h"

namespace paimon::test {

MockRealtimeIndex::MockRealtimeIndex(std::shared_ptr<arrow::Schema> schema,
                                     IndexReaderCapabilities::SearchType search_type,
                                     const std::shared_ptr<MockSearchState>& state)
    : schema_(std::move(schema)), search_type_(search_type), state_(state) {}

Status MockRealtimeIndex::AddBatch(::ArrowArray* batch) {
    if (!batch || !batch->release) {
        return Status::Invalid("Mock real-time index batch is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> imported,
                                      arrow::ImportArray(batch, arrow::struct_(schema_->fields())));
    auto data = checked_pointer_cast<arrow::StructArray>(imported);
    auto offsets = checked_pointer_cast<arrow::Int64Array>(data->field(0));
    if (data->null_count() != 0 || offsets->null_count() != 0) {
        return Status::Invalid("Mock real-time index requires non-null rows and offsets");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    for (int64_t row = 0; row < offsets->length(); ++row) {
        offsets_.Add(offsets->Value(row));
    }
    return Status::OK();
}

Result<std::shared_ptr<RealtimeIndexReader>> MockRealtimeIndex::CreateReader(
    const OffsetRange& offset_range) const {
    if (offset_range.begin < 0 || offset_range.begin > offset_range.end) {
        return Status::Invalid("Invalid mock real-time index reader bounds");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    return std::shared_ptr<RealtimeIndexReader>(
        std::make_shared<MockSearchReader>(search_type_, offset_range, offsets_, state_));
}

uint64_t MockRealtimeIndex::GetMemoryUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return offsets_.GetSizeInBytes();
}

MockRealtimeIndexFactory::MockRealtimeIndexFactory(std::string identifier,
                                                   IndexReaderCapabilities::SearchType search_type,
                                                   const std::shared_ptr<MockSearchState>& state)
    : identifier_(std::move(identifier) + REALTIME_INDEX_IDENTIFIER_SUFFIX),
      search_type_(search_type),
      state_(state) {}

const char* MockRealtimeIndexFactory::Identifier() const {
    return identifier_.c_str();
}

Result<std::shared_ptr<RealtimeIndex>> MockRealtimeIndexFactory::Create(
    ::ArrowSchema* write_schema, const std::map<std::string, std::string>&,
    const std::shared_ptr<MemoryPool>&) const {
    if (!write_schema || !write_schema->release || !state_) {
        return Status::Invalid("Mock real-time index requires a schema and search state");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(write_schema));
    PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateOffsetField(schema));
    if (schema->num_fields() != 2 ||
        schema->field(0)->name() != SpecialFields::RealtimeOffset().Name() ||
        schema->field(1)->name() == SpecialFields::RealtimeOffset().Name()) {
        return Status::Invalid("Mock real-time index requires offset and one indexed field");
    }
    if (search_type_ != IndexReaderCapabilities::SearchType::VECTOR_SEARCH &&
        search_type_ != IndexReaderCapabilities::SearchType::FULL_TEXT_SEARCH) {
        return Status::Invalid("Mock real-time index requires vector or full-text search");
    }
    return std::shared_ptr<RealtimeIndex>(
        std::make_shared<MockRealtimeIndex>(std::move(schema), search_type_, state_));
}

}  // namespace paimon::test

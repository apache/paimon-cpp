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

#include "paimon/core/realtime/index/realtime_index_set.h"

#include <cstring>
#include <set>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/realtime/realtime_utils.h"
#include "paimon/macros.h"
#include "paimon/realtime/index/realtime_index.h"
#include "paimon/realtime/index/realtime_index_factory.h"

namespace paimon {

const char RealtimeIndexSet::kIndexPrefix[] = "realtime.index.";
const char RealtimeIndexSet::kColumnsSuffix[] = ".columns";

namespace {
bool IsColumnsOption(const std::string& key) {
    return StringUtils::StartsWith(key, RealtimeIndexSet::kIndexPrefix) &&
           StringUtils::EndsWith(key, RealtimeIndexSet::kColumnsSuffix);
}
}  // namespace

bool RealtimeIndexSet::IsEnabled(const std::map<std::string, std::string>& options) {
    for (const auto& option : options) {
        if (IsColumnsOption(option.first)) {
            return true;
        }
    }
    return false;
}

Result<std::shared_ptr<RealtimeIndexSet>> RealtimeIndexSet::Create(
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool) {
    if (!IsEnabled(options)) {
        return std::shared_ptr<RealtimeIndexSet>();
    }
    PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateOffsetField(write_schema));
    const size_t prefix_length = std::strlen(kIndexPrefix);
    const size_t suffix_length = std::strlen(kColumnsSuffix);
    std::multimap<std::string, Entry> indexes;
    for (const auto& [key, value] : options) {
        if (!IsColumnsOption(key)) {
            continue;
        }
        if (key.size() <= prefix_length + suffix_length) {
            return Status::Invalid("invalid real-time index option: ", key);
        }
        const std::string identifier =
            key.substr(prefix_length, key.size() - prefix_length - suffix_length);
        std::set<std::string> declared;
        for (std::string name : StringUtils::Split(value, ",", /*ignore_empty=*/true)) {
            StringUtils::Trim(&name);
            if (name.empty()) {
                return Status::Invalid(key, " contains an empty field");
            }
            if (!declared.insert(name).second) {
                continue;
            }
            const int32_t position = write_schema->GetFieldIndex(name);
            if (position < 0 || name == SpecialFields::RealtimeOffset().Name()) {
                return Status::Invalid("invalid real-time index field: ", name);
            }
            const std::shared_ptr<arrow::Field>& field = write_schema->field(position);
            PAIMON_ASSIGN_OR_RAISE(FieldType field_type, FieldTypeUtils::ConvertToFieldType(field));
            const std::shared_ptr<arrow::Schema> index_schema = arrow::schema(
                {write_schema->GetFieldByName(SpecialFields::RealtimeOffset().Name()), field});
            ArrowSchema c_schema;
            ArrowSchemaMarkReleased(&c_schema);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*index_schema, &c_schema));
            ScopeGuard schema_guard([&c_schema]() { ArrowSchemaRelease(&c_schema); });
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeIndex> index,
                                   RealtimeIndexFactory::Get(identifier, &c_schema, options, pool));
            if (!index) {
                return Status::Invalid("real-time index factory returned a null index");
            }
            indexes.emplace(name, Entry{position, field_type, std::move(index)});
        }
        if (declared.empty()) {
            return Status::Invalid(key, " must not be empty");
        }
    }
    return std::shared_ptr<RealtimeIndexSet>(
        new RealtimeIndexSet(write_schema, std::move(indexes)));
}

Status RealtimeIndexSet::AddBatch(::ArrowArray* batch, const OffsetRange& offset_range) {
    if (!batch || !batch->release) {
        return Status::Invalid("real-time index batch is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> imported,
                                      arrow::ImportArray(batch, arrow::struct_(schema_->fields())));
    const std::shared_ptr<arrow::StructArray> data =
        checked_pointer_cast<arrow::StructArray>(imported);
    if (data->null_count() != 0) {
        return Status::Invalid("real-time index requires non-null rows");
    }
    const std::shared_ptr<arrow::Field> offset_field =
        schema_->GetFieldByName(SpecialFields::RealtimeOffset().Name());
    const std::shared_ptr<arrow::Int64Array> offsets =
        checked_pointer_cast<arrow::Int64Array>(data->GetFieldByName(offset_field->name()));
    if (offset_range.begin < 0 || offset_range.begin >= offset_range.end ||
        offsets->null_count() != 0) {
        return Status::Invalid("real-time index requires valid offsets");
    }
    int64_t previous = offset_range.begin - 1;
    for (int64_t row = 0; row < offsets->length(); ++row) {
        const int64_t offset = offsets->Value(row);
        if (offset <= previous || offset >= offset_range.end) {
            return Status::Invalid("real-time index offsets must increase within batch bounds");
        }
        previous = offset;
    }
    if (offset_range.begin < end_offset_) {
        return Status::Invalid("real-time index batches must have ordered disjoint bounds");
    }
    for (const auto& item : indexes_) {
        const Entry& entry = item.second;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> projected,
            arrow::StructArray::Make({offsets, data->field(entry.position)},
                                     {offset_field, schema_->field(entry.position)}));
        ArrowArray index_batch;
        ArrowArrayMarkReleased(&index_batch);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*projected, &index_batch));
        ScopeGuard batch_guard([&index_batch]() { ArrowArrayRelease(&index_batch); });
        PAIMON_RETURN_NOT_OK(entry.index->AddBatch(&index_batch));
    }
    end_offset_ = offset_range.end;
    return Status::OK();
}

std::vector<std::shared_ptr<RealtimeIndex>> RealtimeIndexSet::FindIndexes(
    const std::string& field_name, FieldType field_type) const {
    std::vector<std::shared_ptr<RealtimeIndex>> result;
    const auto range = indexes_.equal_range(field_name);
    for (auto iter = range.first; iter != range.second; ++iter) {
        if (iter->second.type == field_type) {
            result.push_back(iter->second.index);
        }
    }
    return result;
}

std::vector<std::shared_ptr<RealtimeIndex>> RealtimeIndexSet::FindIndexes(
    const std::string& field_name) const {
    std::vector<std::shared_ptr<RealtimeIndex>> result;
    const auto range = indexes_.equal_range(field_name);
    for (auto iter = range.first; iter != range.second; ++iter) {
        result.push_back(iter->second.index);
    }
    return result;
}

uint64_t RealtimeIndexSet::GetMemoryUsage() const {
    uint64_t bytes = 0;
    for (const auto& item : indexes_) {
        bytes += item.second.index->GetMemoryUsage();
    }
    return bytes;
}

}  // namespace paimon

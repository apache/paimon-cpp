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

#include "paimon/core/realtime/realtime_file_schema.h"

#include <algorithm>
#include <optional>
#include <string>

#include "arrow/api.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/realtime/realtime_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/defs.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

RealtimeFileSchema::RealtimeFileSchema(const std::shared_ptr<arrow::Schema>& schema,
                                       int64_t schema_id,
                                       const std::shared_ptr<SchemaManager>& schema_manager)
    : schema_(schema), schema_id_(schema_id), schema_manager_(schema_manager) {}

Result<std::shared_ptr<arrow::Schema>> RealtimeFileSchema::GetFileSchema(
    const DataFileMeta& file) const {
    PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateOffsetField(schema_));
    if (file.schema_id == schema_id_) {
        return schema_;
    }
    if (!schema_manager_) {
        return Status::Invalid("historical realtime file schema requires a schema manager");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> file_schema,
                           schema_manager_->ReadSchema(file.schema_id));
    std::shared_ptr<arrow::Schema> schema =
        DataField::ConvertDataFieldsToArrowSchema(file_schema->Fields());
    PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateOffsetField(schema));
    const std::string& offset_name = SpecialFields::RealtimeOffset().Name();
    PAIMON_ASSIGN_OR_RAISE(DataField current_offset, DataField::ConvertArrowFieldToDataField(
                                                         schema_->GetFieldByName(offset_name)));
    PAIMON_ASSIGN_OR_RAISE(DataField file_offset, file_schema->GetField(offset_name));
    if (current_offset.Id() != file_offset.Id()) {
        return Status::Invalid("historical realtime offset field ID is incompatible");
    }
    return schema;
}

Status RealtimeFileSchema::ValidateKey(const DataFileMeta& file,
                                       const std::shared_ptr<arrow::Field>& key_field) const {
    if (file.schema_id == schema_id_) {
        return Status::OK();
    }
    PAIMON_RETURN_NOT_OK(GetFileSchema(file).status());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> file_schema,
                           schema_manager_->ReadSchema(file.schema_id));
    auto iter = file_schema->Options().find(Options::REALTIME_DEDUPLICATE_KEY_FIELDS);
    if (iter == file_schema->Options().end()) {
        return Status::Invalid("historical file schema has no realtime deduplicate key definition");
    }
    std::string key_name = iter->second;
    StringUtils::Trim(&key_name);
    PAIMON_ASSIGN_OR_RAISE(DataField file_key, file_schema->GetField(key_name));
    PAIMON_ASSIGN_OR_RAISE(DataField current_key,
                           DataField::ConvertArrowFieldToDataField(key_field));
    if (file_key.Id() != current_key.Id() ||
        !file_key.Type()->Equals(current_key.Type(), /*check_metadata=*/true) ||
        file_key.ArrowField()->nullable() || current_key.ArrowField()->nullable()) {
        return Status::Invalid("historical realtime deduplicate key is incompatible");
    }
    return Status::OK();
}

Result<std::vector<Literal>> RealtimeFileSchema::SelectOffsets(
    const DataFileMeta& file, const RoaringBitmap64& offsets) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, GetFileSchema(file));
    const std::string& offset_name = SpecialFields::RealtimeOffset().Name();
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> fields,
                           DataField::ConvertArrowSchemaToDataFields(schema));
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> write_fields,
                           DataField::ProjectFields(fields, file.write_cols));
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> stats_fields,
                           DataField::ProjectFields(write_fields, file.value_stats_cols));
    std::optional<int32_t> position;
    auto iter = std::find_if(
        stats_fields.begin(), stats_fields.end(),
        [&offset_name](const DataField& field) { return field.Name() == offset_name; });
    if (iter != stats_fields.end()) {
        position = static_cast<int32_t>(iter - stats_fields.begin());
    }
    const BinaryRow& min_values = file.value_stats.MinValues();
    const BinaryRow& max_values = file.value_stats.MaxValues();
    const bool has_stats = position && *position >= 0 && *position < min_values.GetFieldCount() &&
                           *position < max_values.GetFieldCount() &&
                           !min_values.IsNullAt(*position) && !max_values.IsNullAt(*position) &&
                           min_values.GetLong(*position) >= 0 &&
                           min_values.GetLong(*position) <= max_values.GetLong(*position);
    std::vector<Literal> literals;
    if (has_stats) {
        for (RoaringBitmap64::Iterator iter = offsets.EqualOrLarger(min_values.GetLong(*position));
             iter != offsets.End() && *iter <= max_values.GetLong(*position); ++iter) {
            literals.emplace_back(*iter);
        }
    } else {
        literals.reserve(offsets.Cardinality());
        for (RoaringBitmap64::Iterator iter = offsets.Begin(); iter != offsets.End(); ++iter) {
            literals.emplace_back(*iter);
        }
    }
    return literals;
}

}  // namespace paimon

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

#include "paimon/core/realtime/realtime_schema_layout.h"

#include <utility>

#include "arrow/api.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/status.h"

namespace paimon {
namespace {

std::shared_ptr<arrow::Field> ValueKindField() {
    return DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())->WithNullable(false);
}

std::shared_ptr<arrow::Field> RealtimeOffsetField() {
    return DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset());
}

std::shared_ptr<arrow::Schema> Prepend(const arrow::FieldVector& prefix,
                                       const std::shared_ptr<arrow::Schema>& schema) {
    arrow::FieldVector fields = prefix;
    fields.insert(fields.end(), schema->fields().begin(), schema->fields().end());
    return arrow::schema(std::move(fields), schema->metadata());
}

}  // namespace

Result<std::unique_ptr<RealtimeSchemaLayout>> RealtimeSchemaLayout::Create(
    RealtimeStoreMode mode, const std::shared_ptr<arrow::Schema>& user_schema) {
    if (!user_schema) {
        return Status::Invalid("real-time schema layout requires a user schema");
    }
    if (mode != RealtimeStoreMode::APPEND_ONLY && mode != RealtimeStoreMode::PRIMARY_KEY) {
        return Status::Invalid("unknown real-time store mode");
    }
    return std::unique_ptr<RealtimeSchemaLayout>(new RealtimeSchemaLayout(mode, user_schema));
}

RealtimeSchemaLayout::RealtimeSchemaLayout(RealtimeStoreMode mode,
                                           const std::shared_ptr<arrow::Schema>& user_schema)
    : user_schema_(user_schema), input_schema_(Prepend({RealtimeOffsetField()}, user_schema_)) {
    if (mode == RealtimeStoreMode::APPEND_ONLY) {
        store_write_schema_ = input_schema_;
        store_commit_schema_ = Prepend({ValueKindField()}, input_schema_);
        commit_schema_ = user_schema_;
        query_schema_ = Prepend({ValueKindField()}, user_schema_);
        return;
    }

    store_write_schema_ = SpecialFields::CompleteSequenceAndValueKindField(input_schema_);
    store_commit_schema_ = store_write_schema_;
    commit_schema_ = SpecialFields::CompleteSequenceAndValueKindField(user_schema_);
    query_schema_ = commit_schema_;
}

}  // namespace paimon

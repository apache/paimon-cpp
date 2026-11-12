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

#include "paimon/core/casting/casted_row.h"

#include "arrow/type.h"
#include "paimon/common/types/data_field.h"

namespace paimon {
Result<std::unique_ptr<CastedRow>> CastedRow::Create(
    const std::vector<std::shared_ptr<CastExecutor>>& cast_executors,
    const std::vector<DataField>& src_fields, const std::vector<DataField>& target_fields,
    const std::shared_ptr<InternalRow>& row) {
    if (src_fields.size() != target_fields.size() || src_fields.size() != cast_executors.size() ||
        src_fields.size() != static_cast<size_t>(row->GetFieldCount())) {
        return Status::Invalid(
            "CastedRow create failed, src_fields & target_fields & cast_executors & row size "
            "mismatch");
    }
    std::vector<InternalRow::FieldGetterFunc> field_getters;
    field_getters.reserve(src_fields.size());
    std::vector<arrow::Type::type> src_types;
    src_types.reserve(src_fields.size());
    std::vector<std::shared_ptr<arrow::DataType>> target_types;
    target_types.reserve(target_fields.size());
    for (size_t i = 0; i < src_fields.size(); ++i) {
        const auto& type = src_fields[i].Type();
        PAIMON_ASSIGN_OR_RAISE(InternalRow::FieldGetterFunc getter,
                               InternalRow::CreateFieldGetter(i, type, /*use_view=*/false));
        field_getters.push_back(std::move(getter));
        src_types.push_back(type->id());
        target_types.push_back(target_fields[i].Type());
    }
    return std::unique_ptr<CastedRow>(new CastedRow(cast_executors, std::move(field_getters),
                                                    std::move(src_types), std::move(target_types),
                                                    row));
}

}  // namespace paimon

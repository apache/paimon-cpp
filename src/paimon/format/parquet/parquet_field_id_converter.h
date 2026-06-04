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
#include <string>

#include "arrow/api.h"
#include "paimon/result.h"

namespace paimon::parquet {

class ParquetFieldIdConverter {
 public:
    ParquetFieldIdConverter() = delete;
    ~ParquetFieldIdConverter() = delete;

    static const char PARQUET_FIELD_ID[];
    // Iterate through all fields in the Schema.
    // For each field, copy the value of 'paimon.id' to 'PARQUET:field_id'.
    static Result<std::shared_ptr<arrow::Schema>> AddParquetIdsFromPaimonIds(
        const std::shared_ptr<arrow::Schema>& schema);
    // Iterate through all fields in the Schema.
    // For each field, copy the value of 'PARQUET:field_id' to 'paimon.id'.
    static Result<std::shared_ptr<arrow::Schema>> GetPaimonIdsFromParquetIds(
        const std::shared_ptr<arrow::Schema>& schema);

 private:
    enum class IdConvertType {
        PARQUET_TO_PAIMON_ID = 1,
        PAIMON_TO_PARQUET_ID = 2,
    };

    static arrow::Result<std::shared_ptr<arrow::Field>> ProcessField(
        const std::shared_ptr<arrow::Field>& field, IdConvertType convert_type);

    static arrow::Result<std::shared_ptr<const arrow::KeyValueMetadata>> CopyId(
        const std::shared_ptr<const arrow::KeyValueMetadata>& metadata, IdConvertType convert_type);
};

}  // namespace paimon::parquet

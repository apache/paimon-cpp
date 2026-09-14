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

#include "arrow/api.h"
#include "paimon/result.h"

namespace paimon::parquet {

class ParquetReadTypeAdapter {
 public:
    ParquetReadTypeAdapter() = delete;
    ~ParquetReadTypeAdapter() = delete;

    static Result<std::shared_ptr<arrow::DataType>> NormalizeFileType(
        const std::shared_ptr<arrow::DataType>& src_data_type);

    static Result<bool> NeedsArrayConversion(
        const std::shared_ptr<arrow::DataType>& src_data_type,
        const std::shared_ptr<arrow::DataType>& target_data_type);

    static Result<std::shared_ptr<arrow::Array>> AdaptArray(
        const std::shared_ptr<arrow::Array>& array,
        const std::shared_ptr<arrow::DataType>& target_data_type,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

 private:
    static Result<std::shared_ptr<arrow::Array>> AdaptArrayImpl(
        const std::shared_ptr<arrow::Array>& array,
        const std::shared_ptr<arrow::DataType>& target_data_type,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    static Result<bool> NeedsArrayConversionImpl(const std::shared_ptr<arrow::Field>& src_field,
                                                 const std::shared_ptr<arrow::Field>& target_field);

    static bool IsCompatibleBlob(const std::shared_ptr<arrow::Field>& src_field,
                                 const std::shared_ptr<arrow::Field>& target_field);
};

}  // namespace paimon::parquet

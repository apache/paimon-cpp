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

#include "arrow/memory_pool.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class DataType;
}  // namespace arrow

namespace paimon::parquet {

/// Converts logical FixedSizeList VECTOR arrays to Parquet LIST arrays.
class ParquetVectorConverter {
 public:
    ParquetVectorConverter() = delete;
    ~ParquetVectorConverter() = delete;

    static Result<std::shared_ptr<arrow::Array>> ConvertToWriteType(
        const std::shared_ptr<arrow::Array>& array, arrow::MemoryPool* pool);

    static std::shared_ptr<arrow::DataType> GetWriteType(
        const std::shared_ptr<arrow::DataType>& logical_type);
};

}  // namespace paimon::parquet

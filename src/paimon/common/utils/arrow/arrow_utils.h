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

#pragma once

#include <cstdint>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/type_fwd.h"
#include "paimon/result.h"

namespace paimon {

class PAIMON_EXPORT ArrowUtils {
 public:
    ArrowUtils() = delete;
    ~ArrowUtils() = delete;

    static const char* kArrowSchemaMetadataKey;

    static Result<std::shared_ptr<arrow::Schema>> DataTypeToSchema(
        const std::shared_ptr<arrow::DataType>& data_type);

    static Result<std::vector<int32_t>> CreateProjection(
        const std::shared_ptr<arrow::Schema>& src_schema, const arrow::FieldVector& target_fields);

    static Status CheckNullabilityMatch(const std::shared_ptr<arrow::Schema>& schema,
                                        const std::shared_ptr<arrow::Array>& data);

    // For struct array, arrow is unsafe for fields() and field(); for dict array, arrow is unsafe
    // for dictionary(). Therefore, access array in advance before merge sort and projection to
    // avoid subsequent multi-threading problems.
    static void TraverseArray(const std::shared_ptr<arrow::Array>& array);

    static uint64_t GetArrayMemoryUsage(const std::shared_ptr<arrow::ArrayData>& data);

    static Result<std::shared_ptr<arrow::StructArray>> RemoveFieldFromStructArray(
        const std::shared_ptr<arrow::StructArray>& struct_array, const std::string& field_name);

    /// Returns a RecordBatch whose columns, including their nested children, all have a zero
    /// offset, as required by `BatchReader`. Offsets are rebased by slicing buffers (zero copy)
    /// wherever the layout allows it; only layouts that cannot be rebased fall back to a full copy.
    static Result<std::shared_ptr<arrow::RecordBatch>> NormalizeRecordBatchOffsets(
        const std::shared_ptr<arrow::RecordBatch>& record_batch, arrow::MemoryPool* pool);

    /// Returns an Array with zero offsets. Struct children are also sliced to the parent's visible
    /// range so the result can be exported and imported as a RecordBatch.
    static Result<std::shared_ptr<arrow::Array>> NormalizeArrayOffsets(
        const std::shared_ptr<arrow::Array>& array, arrow::MemoryPool* pool);

    static bool EqualsIgnoreNullable(const std::shared_ptr<arrow::DataType>& type,
                                     const std::shared_ptr<arrow::DataType>& other_type);

    /// Normalize and resolve a compression string to an Arrow compression type.
    /// Handles "none" and empty string by mapping them to "uncompressed".
    static Result<arrow::Compression::type> GetCompressionType(const std::string& compression);

 private:
    static Status InnerCheckNullabilityMatch(const std::shared_ptr<arrow::Field>& field,
                                             const std::shared_ptr<arrow::Array>& data);
};

}  // namespace paimon

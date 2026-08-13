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
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/result.h"

namespace paimon {

/// Utility class for nested column pruning and map key selection.
class PAIMON_EXPORT NestedProjectionUtils {
 public:
    NestedProjectionUtils() = delete;
    ~NestedProjectionUtils() = delete;

    static std::shared_ptr<arrow::Field> FindFieldByName(const arrow::FieldVector& fields,
                                                         const std::string& name);

    /// Extract the paimon field ID from an Arrow field's metadata ("paimon.id").
    /// @return The paimon.id in metadata, or return bad status if the metadata key is not present
    /// or convert error.
    static Result<int32_t> GetPaimonFieldId(const std::shared_ptr<arrow::Field>& field);

    /// Find a child field in a STRUCT DataType by paimon field ID.
    /// @return The specific arrow field, or return bad status if no child has the given ID.
    static Result<std::shared_ptr<arrow::Field>> FindFieldByPaimonId(
        const std::shared_ptr<arrow::DataType>& struct_type, int32_t field_id);

    /// Recursively prune `data_type` so that only the sub-fields requested by
    /// `read_type` are retained. Matching is done by paimon field ID to support
    /// schema evolution (field renames).
    ///
    /// Supported nesting: STRUCT, LIST (element recurse), MAP (key/value recurse).
    /// For atomic types, `data_type` is returned as-is.
    ///
    /// @return std::nullopt when all sub-fields of a STRUCT are pruned away
    /// (caller should skip this field entirely, mirroring Java's null return).
    static Result<std::optional<std::shared_ptr<arrow::DataType>>> PruneDataType(
        const std::shared_ptr<arrow::DataType>& read_type,
        const std::shared_ptr<arrow::DataType>& data_type);

    /// @return true if `read_schema` requests a nested sub-field projection against
    /// `file_schema` (same top-level field, but nested STRUCT/LIST/MAP subtree is pruned).
    static Result<bool> HasNestedSubfieldProjection(
        const std::shared_ptr<arrow::Schema>& file_schema,
        const std::shared_ptr<arrow::Schema>& read_schema);

    /// Parse the "paimon.map.selected-keys" metadata from an Arrow field.
    /// @return an empty vector if has no metadata, or the specific metadata key is absent.
    /// The metadata value is a comma-separated string, e.g. "key1,key2".
    /// Empty tokens are preserved ("" means selecting empty-string keys), and duplicate
    /// selected keys are rejected as invalid.
    static Result<std::vector<std::string>> GetMapSelectedKeys(
        const std::shared_ptr<arrow::Field>& field);

    /// @return true when `field` is a selected-key MAP projection: a STRUCT carrying
    /// `paimon.map.selected-keys` metadata.
    static bool IsMapSharedShreddingAccessField(const std::shared_ptr<arrow::Field>& field);

    /// Validates a selected-key MAP projection and returns its selected keys. The field must be a
    /// non-empty STRUCT, its metadata key count must match its child count, and all children must
    /// have the same value type.
    static Result<std::vector<std::string>> ValidateMapSharedShreddingAccessField(
        const std::shared_ptr<arrow::Field>& field);

    /// Rewrites a selected-key STRUCT projection to use the data file's complete MAP value type
    /// for every child before materialization. Cpp paimon does not support schema evolution for
    /// for field inside the MAP value.
    static Result<std::shared_ptr<arrow::DataType>> BuildMapSharedShreddingAccessDataType(
        const std::shared_ptr<arrow::Field>& read_field,
        const std::shared_ptr<arrow::DataType>& data_type);

    /// Returns a string view for a MAP key stored as string or dictionary<string|large_string>.
    static Result<std::string_view> GetMapKeyViewAt(const std::shared_ptr<arrow::Array>& key_array,
                                                    int64_t entry_idx);

    /// Filter a MapArray so that only entries whose key is in `selected_keys` are kept.
    /// Supports string keys and dictionary<string|large_string> keys.
    /// The output map entry order follows
    /// `selected_keys` order, and duplicate selected keys are rejected.
    /// @return the original array unchanged if `selected_keys` is empty.
    static Result<std::shared_ptr<arrow::Array>> FilterMapArrayBySelectedKeys(
        const std::shared_ptr<arrow::Array>& map_array,
        const std::vector<std::string>& selected_keys, arrow::MemoryPool* pool);

    /// Reshape `array` to `read_type`, null-filling nested fields added by schema
    /// evolution. No-op when types match. STRUCT matches children by paimon field id;
    /// LIST/MAP recurse into items, preserving offsets and validity.
    static Result<std::shared_ptr<arrow::Array>> AlignArrayToReadType(
        const std::shared_ptr<arrow::Array>& array,
        const std::shared_ptr<arrow::DataType>& read_type, arrow::MemoryPool* pool);

 private:
    static Result<bool> HasNestedSubfieldProjectionType(
        const std::shared_ptr<arrow::DataType>& file_type,
        const std::shared_ptr<arrow::DataType>& read_type);
};

}  // namespace paimon

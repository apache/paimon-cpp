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
#include <optional>
#include <vector>

#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {

/// Adapts logical field values to the physical key type stored by range-bitmap indexes.
class RangeBitmapTypeAdapter {
 public:
    static Result<std::unique_ptr<RangeBitmapTypeAdapter>> Create(
        const std::shared_ptr<arrow::DataType>& arrow_type);

    FieldType GetStorageType() const;

    Result<Literal> ToStorageLiteral(const Literal& literal) const;

    Result<std::vector<Literal>> ToStorageLiterals(const std::vector<Literal>& literals) const;

 private:
    RangeBitmapTypeAdapter(FieldType logical_type, FieldType storage_type,
                           std::optional<int32_t> timestamp_precision);

    FieldType logical_type_;
    FieldType storage_type_;
    std::optional<int32_t> timestamp_precision_;
};

}  // namespace paimon

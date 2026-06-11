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
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
class InternalRow;
class MemoryPool;

/// Serializer for `DataFileMeta` with 1.0 snapshot version.
class DataFileMeta10Serializer : public ObjectSerializer<std::shared_ptr<DataFileMeta>> {
 public:
    static const std::shared_ptr<arrow::DataType>& DataType();

    explicit DataFileMeta10Serializer(const std::shared_ptr<MemoryPool>& pool)
        : ObjectSerializer<std::shared_ptr<DataFileMeta>>(DataType(), pool) {}

    Result<BinaryRow> ToRow(const std::shared_ptr<DataFileMeta>& meta) const override;
    Result<std::shared_ptr<DataFileMeta>> FromRow(const InternalRow& row) const override;
};

}  // namespace paimon

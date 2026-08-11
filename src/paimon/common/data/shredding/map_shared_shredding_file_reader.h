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

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"

namespace paimon {

class MapFieldReadPlan {
 public:
    virtual ~MapFieldReadPlan() = default;

    MapFieldReadPlan(const std::shared_ptr<arrow::Field>& logical_field,
                     const std::shared_ptr<arrow::Field>& physical_read_field)
        : logical_field_(logical_field), physical_read_field_(physical_read_field) {}

    const std::shared_ptr<arrow::Field>& LogicalField() const {
        return logical_field_;
    }

    const std::shared_ptr<arrow::Field>& PhysicalReadField() const {
        return physical_read_field_;
    }

    virtual Result<std::shared_ptr<arrow::Array>> Materialize(
        const std::shared_ptr<arrow::Array>& physical_array,
        arrow::MemoryPool* arrow_pool) const = 0;

 private:
    std::shared_ptr<arrow::Field> logical_field_;
    std::shared_ptr<arrow::Field> physical_read_field_;
};

class MapFieldReadPlanFactory {
 public:
    static Result<std::unique_ptr<MapFieldReadPlan>> CreateFullMapReadPlan(
        const std::shared_ptr<arrow::Field>& logical_map_field,
        const MapSharedShreddingFieldMeta& meta, const std::vector<std::string>& selected_keys);

    static Result<std::unique_ptr<MapFieldReadPlan>> CreateSharedSelectedKeysReadPlan(
        const std::shared_ptr<arrow::Field>& selected_keys_field,
        const MapSharedShreddingFieldMeta& meta, const std::vector<std::string>& selected_keys);

    static Result<std::unique_ptr<MapFieldReadPlan>> CreateDefaultSelectedKeysReadPlan(
        const std::shared_ptr<arrow::Field>& file_map_field,
        const std::shared_ptr<arrow::Field>& selected_keys_field,
        const std::vector<std::string>& selected_keys);
};

class MapSharedShreddingFileReader : public FileBatchReader {
 public:
    MapSharedShreddingFileReader(
        std::unique_ptr<FileBatchReader>&& reader,
        std::map<std::string, std::unique_ptr<MapFieldReadPlan>>&& field_read_plans,
        const std::shared_ptr<MemoryPool>& pool);

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Result<ReadBatch> NextBatch() override;

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override;

    void Close() override;

    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override;

    Result<uint64_t> GetNumberOfRows() const override;

    bool SupportPreciseBitmapSelection() const override;

 private:
    static Result<std::shared_ptr<arrow::Field>> ToLogicalMapField(
        const std::shared_ptr<arrow::Field>& physical_field);

 private:
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::unique_ptr<FileBatchReader> reader_;
    std::map<std::string, std::unique_ptr<MapFieldReadPlan>> field_read_plans_;
};

}  // namespace paimon

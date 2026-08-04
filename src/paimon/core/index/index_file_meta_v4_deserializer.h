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

#include <cassert>
#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_file_meta_v2_deserializer.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/result.h"

namespace paimon {
class MemoryPool;

class IndexFileMetaV4Deserializer : public ObjectSerializer<std::shared_ptr<IndexFileMeta>> {
 public:
    static const std::shared_ptr<arrow::DataType>& GlobalIndexMetaDataType() {
        static std::shared_ptr<arrow::DataType> schema = arrow::struct_({
            arrow::field("_ROW_RANGE_START", arrow::int64(), /*nullable=*/false),
            arrow::field("_ROW_RANGE_END", arrow::int64(), /*nullable=*/false),
            arrow::field("_INDEX_FIELD_ID", arrow::int32(), /*nullable=*/false),
            arrow::field("_EXTRA_FIELD_IDS",
                         arrow::list(arrow::field("item", arrow::int32(), /*nullable=*/false)),
                         /*nullable=*/true),
            arrow::field("_INDEX_META", arrow::binary(), /*nullable=*/true),
        });
        return schema;
    }

    static const std::shared_ptr<arrow::DataType>& DataType() {
        static std::shared_ptr<arrow::DataType> schema = arrow::struct_({
            arrow::field("_INDEX_TYPE", arrow::utf8(), false),
            arrow::field("_FILE_NAME", arrow::utf8(), false),
            arrow::field("_FILE_SIZE", arrow::int64(), false),
            arrow::field("_ROW_COUNT", arrow::int64(), false),
            arrow::field("_DELETIONS_VECTORS_RANGES",
                         arrow::list(arrow::field("item", DeletionVectorMeta::DataType(), true)),
                         true),
            arrow::field("_EXTERNAL_PATH", arrow::utf8(), true),
            arrow::field("_GLOBAL_INDEX", GlobalIndexMetaDataType(), true),
        });
        return schema;
    }

    explicit IndexFileMetaV4Deserializer(const std::shared_ptr<MemoryPool>& pool)
        : ObjectSerializer<std::shared_ptr<IndexFileMeta>>(DataType(), pool) {}

    Result<BinaryRow> ToRow(const std::shared_ptr<IndexFileMeta>& meta) const override {
        assert(false);
        return Status::Invalid("IndexFileMetaV4Deserializer to row is not valid");
    }

    Result<std::shared_ptr<IndexFileMeta>> FromRow(const InternalRow& row) const override {
        auto file_type = row.GetString(0);
        auto file_name = row.GetString(1);
        auto file_size = row.GetLong(2);
        auto row_count = row.GetLong(3);
        std::optional<LinkedHashMap<std::string, DeletionVectorMeta>> dv_ranges;
        if (!row.IsNullAt(4)) {
            dv_ranges = IndexFileMetaV2Deserializer::RowArrayDataToDvRanges(row.GetArray(4).get());
        }
        std::optional<std::string> external_path;
        if (!row.IsNullAt(5)) {
            external_path = row.GetString(5).ToString();
        }
        std::optional<GlobalIndexMeta> global_index_meta;
        if (!row.IsNullAt(6)) {
            std::shared_ptr<InternalRow> global_index_meta_row =
                row.GetRow(6, GlobalIndexMetaDataType()->num_fields());
            assert(global_index_meta_row);
            int64_t row_range_start = global_index_meta_row->GetLong(0);
            int64_t row_range_end = global_index_meta_row->GetLong(1);
            int32_t index_field_id = global_index_meta_row->GetInt(2);
            std::optional<std::vector<int32_t>> extra_field_ids;
            if (!global_index_meta_row->IsNullAt(3)) {
                std::shared_ptr<InternalArray> array = global_index_meta_row->GetArray(3);
                if (!array) {
                    return Status::Invalid(
                        "GlobalIndexMeta FromRow failed with nullptr extra field ids");
                }
                PAIMON_ASSIGN_OR_RAISE(extra_field_ids, array->ToIntArray());
            }
            std::shared_ptr<Bytes> index_meta;
            if (!global_index_meta_row->IsNullAt(4)) {
                index_meta = global_index_meta_row->GetBinary(4);
                assert(index_meta);
            }
            global_index_meta = GlobalIndexMeta(row_range_start, row_range_end, index_field_id,
                                                extra_field_ids, index_meta);
        }
        return std::make_shared<IndexFileMeta>(file_type.ToString(), file_name.ToString(),
                                               file_size, row_count, dv_ranges, external_path,
                                               global_index_meta);
    }
};

}  // namespace paimon

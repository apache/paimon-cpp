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

#include "paimon/core/manifest/index_manifest_entry_serializer.h"

#include <cassert>
#include <optional>
#include <string>
#include <utility>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_file_meta_serializer.h"
#include "paimon/core/index/index_file_meta_v2_deserializer.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/status.h"

namespace paimon {
Result<IndexManifestEntry> IndexManifestEntrySerializer::ConvertFrom(int32_t version,
                                                                     const InternalRow& row) const {
    if (version != VERSION) {
        return Status::Invalid("Unsupported version", std::to_string(version));
    }
    auto kind = row.GetByte(0);
    PAIMON_ASSIGN_OR_RAISE(FileKind file_kind, FileKind::FromByteValue(kind));
    auto partition_bytes = row.GetBinary(1);
    PAIMON_ASSIGN_OR_RAISE(BinaryRow partition,
                           SerializationUtils::DeserializeBinaryRow(partition_bytes));
    auto bucket = row.GetInt(2);

    // for IndexFileMeta
    auto index_type = row.GetString(3).ToString();
    auto file_name = row.GetString(4).ToString();
    int64_t file_size = row.GetLong(5);
    int64_t row_count = row.GetLong(6);
    std::optional<LinkedHashMap<std::string, DeletionVectorMeta>> dv_ranges;
    if (!row.IsNullAt(7)) {
        auto dv_range_array = row.GetArray(7);
        assert(dv_range_array);
        dv_ranges = IndexFileMetaV2Deserializer::RowArrayDataToDvRanges(dv_range_array.get());
    }
    std::optional<std::string> external_path;
    if (!row.IsNullAt(8)) {
        external_path = row.GetString(8).ToString();
    }
    // for global index meta
    std::optional<GlobalIndexMeta> global_index_meta;
    if (!row.IsNullAt(9)) {
        std::shared_ptr<InternalRow> global_index_meta_row =
            row.GetRow(9, GlobalIndexMeta::NUM_FIELDS);
        assert(global_index_meta_row);
        PAIMON_ASSIGN_OR_RAISE(global_index_meta, GlobalIndexMeta::FromRow(*global_index_meta_row));
    }
    auto index_file_meta = std::make_shared<IndexFileMeta>(
        index_type, file_name, file_size, row_count, dv_ranges, external_path, global_index_meta);

    return IndexManifestEntry(file_kind, partition, bucket, index_file_meta);
}

Result<BinaryRow> IndexManifestEntrySerializer::ToRow(const IndexManifestEntry& record) const {
    BinaryRow row(GetDataType()->num_fields());
    BinaryRowWriter writer(&row, 32 * 1024, pool_.get());

    writer.WriteInt(0, GetVersion());
    writer.WriteByte(1, record.kind.ToByteValue());
    auto partition_bytes = SerializationUtils::SerializeBinaryRow(record.partition, pool_.get());
    assert(partition_bytes);
    writer.WriteBinary(2, *partition_bytes);
    writer.WriteInt(3, record.bucket);
    IndexFileMetaSerializer::WriteIndexFileMeta(4, record.index_file, &writer, pool_.get());
    writer.Complete();
    return row;
}

}  // namespace paimon

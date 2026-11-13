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

#include <map>
#include <optional>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/utils/math.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/io/data_file_meta_serializer.h"
#include "paimon/core/table/format/format_data_split.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/core/table/source/fallback_data_split.h"
#include "paimon/core/table/source/query_auth_split.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/global_index/indexed_split.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/table/source/data_split.h"
namespace paimon {
struct DataFileMeta;
namespace {

/// Checks a serialized count before allocating: each entry needs at least `min_entry_bytes`.
Result<int32_t> ReadSplitCount(DataInputStream* in, int64_t min_entry_bytes,
                               const char* split_type) {
    PAIMON_ASSIGN_OR_RAISE(int32_t count, in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t pos, in->GetPos());
    PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in->Length());
    if (count < 0 || count * min_entry_bytes > stream_length - pos) {
        return Status::Invalid(
            fmt::format("invalid {}, {} entries do not fit the split", split_type, count));
    }
    return count;
}

/// Java DataInput.readBoolean() treats any nonzero byte as true. Reading into a C++ bool would
/// create an invalid representation for a byte other than 0 or 1.
Result<bool> ReadSplitFlag(DataInputStream* in) {
    PAIMON_ASSIGN_OR_RAISE(int8_t value, in->ReadValue<int8_t>());
    return value != 0;
}

Status WriteDataSplit(const std::shared_ptr<DataSplitImpl>& data_split_impl,
                      MemorySegmentOutputStream* out, const std::shared_ptr<MemoryPool>& pool) {
    out->WriteValue<int64_t>(DataSplitImpl::MAGIC);
    out->WriteValue<int32_t>(DataSplitImpl::VERSION);
    out->WriteValue<int64_t>(data_split_impl->SnapshotId());

    PAIMON_RETURN_NOT_OK(SerializationUtils::SerializeBinaryRow(data_split_impl->Partition(), out));
    out->WriteValue<int32_t>(data_split_impl->Bucket());
    out->WriteString(data_split_impl->BucketPath());

    std::optional<int32_t> total_buckets = data_split_impl->TotalBuckets();
    if (total_buckets == std::nullopt) {
        out->WriteValue<bool>(false);
    } else {
        out->WriteValue<bool>(true);
        out->WriteValue<int32_t>(total_buckets.value());
    }

    DataFileMetaSerializer serializer(pool);
    PAIMON_RETURN_NOT_OK(serializer.SerializeList(data_split_impl->BeforeFiles(), out));

    DeletionFile::SerializeList(data_split_impl->BeforeDeletionFiles(), out);
    PAIMON_RETURN_NOT_OK(serializer.SerializeList(data_split_impl->DataFiles(), out));
    DeletionFile::SerializeList(data_split_impl->DeletionFiles(), out);
    out->WriteValue<bool>(data_split_impl->IsStreaming());
    out->WriteValue<bool>(data_split_impl->RawConvertible());
    return Status::OK();
}

Result<std::shared_ptr<DataSplitImpl>> ReadDataSplitWithoutMagicNumber(
    int64_t magic, DataInputStream* in, const std::shared_ptr<MemoryPool>& pool) {
    int32_t version = 1;
    if (magic == DataSplitImpl::MAGIC) {
        PAIMON_ASSIGN_OR_RAISE(version, in->ReadValue<int32_t>());
    }

    // version 1 does not write magic number in, so the first long is snapshot id.
    int64_t snapshot_id = magic;
    if (version != 1) {
        PAIMON_ASSIGN_OR_RAISE(snapshot_id, in->ReadValue<int64_t>());
    }

    PAIMON_ASSIGN_OR_RAISE(BinaryRow partition,
                           SerializationUtils::DeserializeBinaryRow(in, pool.get()));
    int32_t bucket = -1;
    PAIMON_ASSIGN_OR_RAISE(bucket, in->ReadValue<int32_t>());
    std::string bucket_path;
    PAIMON_ASSIGN_OR_RAISE(bucket_path, in->ReadString());

    std::optional<int32_t> total_buckets;
    if (version >= 6) {
        PAIMON_ASSIGN_OR_RAISE(bool total_buckets_exist, ReadSplitFlag(in));
        if (total_buckets_exist) {
            PAIMON_ASSIGN_OR_RAISE(total_buckets, in->ReadValue<int32_t>());
        }
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<ObjectSerializer<std::shared_ptr<DataFileMeta>>> data_file_serializer,
        DataSplitImpl::GetFileMetaSerializer(version, pool));
    std::vector<std::shared_ptr<DataFileMeta>> before_files;
    PAIMON_ASSIGN_OR_RAISE(before_files, data_file_serializer->DeserializeList(in));
    // compatible for deletion file
    std::vector<std::optional<DeletionFile>> before_deletion_files;
    PAIMON_ASSIGN_OR_RAISE(before_deletion_files, DeletionFile::DeserializeList(in, version));

    std::vector<std::shared_ptr<DataFileMeta>> data_files;
    PAIMON_ASSIGN_OR_RAISE(data_files, data_file_serializer->DeserializeList(in));
    // compatible for deletion file
    std::vector<std::optional<DeletionFile>> data_deletion_files;
    PAIMON_ASSIGN_OR_RAISE(data_deletion_files, DeletionFile::DeserializeList(in, version));

    bool is_streaming = false;
    PAIMON_ASSIGN_OR_RAISE(is_streaming, ReadSplitFlag(in));
    bool raw_convertible = false;
    PAIMON_ASSIGN_OR_RAISE(raw_convertible, ReadSplitFlag(in));

    DataSplitImpl::Builder builder(partition, bucket, bucket_path, std::move(data_files));
    builder.WithTotalBuckets(total_buckets)
        .WithSnapshot(snapshot_id)
        .WithBeforeFiles(std::move(before_files))
        .IsStreaming(is_streaming)
        .RawConvertible(raw_convertible);
    if (!before_deletion_files.empty()) {
        builder.WithBeforeDeletionFiles(before_deletion_files);
    }
    if (!data_deletion_files.empty()) {
        builder.WithDataDeletionFiles(data_deletion_files);
    }
    return builder.Build();
}

Status WriteIndexedSplit(const std::shared_ptr<IndexedSplitImpl>& indexed_split_impl,
                         MemorySegmentOutputStream* out, const std::shared_ptr<MemoryPool>& pool) {
    out->WriteValue<int64_t>(IndexedSplitImpl::MAGIC);
    out->WriteValue<int32_t>(IndexedSplitImpl::VERSION);
    auto inner_split_impl =
        std::dynamic_pointer_cast<DataSplitImpl>(indexed_split_impl->GetDataSplit());
    if (!inner_split_impl) {
        return Status::Invalid("inner split in IndexedSplit is supposed to be DataSplit");
    }
    PAIMON_RETURN_NOT_OK(WriteDataSplit(inner_split_impl, out, pool));
    auto row_ranges = indexed_split_impl->RowRanges();
    out->WriteValue<int32_t>(row_ranges.size());
    for (const auto& range : row_ranges) {
        out->WriteValue<int64_t>(range.from);
        out->WriteValue<int64_t>(range.to);
    }

    auto scores = indexed_split_impl->Scores();
    if (!scores.empty()) {
        out->WriteValue<bool>(true);
        out->WriteValue<int32_t>(scores.size());
        for (float score : scores) {
            out->WriteValue<float>(CanonicalizeFloatingPoint(score));
        }
    } else {
        out->WriteValue<bool>(false);
    }
    return Status::OK();
}

/// Reads an IndexedSplit after its magic number.
Result<std::shared_ptr<IndexedSplitImpl>> ReadIndexedSplitWithoutMagicNumber(
    DataInputStream* in, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(int32_t version, in->ReadValue<int32_t>());
    if (version != IndexedSplitImpl::VERSION) {
        return Status::Invalid(fmt::format("Unsupported IndexedSplit version: {}", version));
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t data_split_magic, in->ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplitImpl> data_split,
                           ReadDataSplitWithoutMagicNumber(data_split_magic, in, pool));
    PAIMON_ASSIGN_OR_RAISE(int32_t range_size,
                           ReadSplitCount(in, 2 * sizeof(int64_t), "IndexedSplit"));
    std::vector<Range> row_ranges;
    row_ranges.reserve(range_size);
    for (int32_t i = 0; i < range_size; ++i) {
        PAIMON_ASSIGN_OR_RAISE(int64_t range_from, in->ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(int64_t range_to, in->ReadValue<int64_t>());
        row_ranges.emplace_back(range_from, range_to);
    }
    std::vector<float> scores;
    PAIMON_ASSIGN_OR_RAISE(bool has_scores, ReadSplitFlag(in));
    if (has_scores) {
        PAIMON_ASSIGN_OR_RAISE(int32_t scores_length,
                               ReadSplitCount(in, sizeof(float), "IndexedSplit"));
        scores.resize(scores_length);
        for (int32_t i = 0; i < scores_length; ++i) {
            PAIMON_ASSIGN_OR_RAISE(float score, in->ReadValue<float>());
            scores[i] = score;
        }
    }
    return std::make_shared<IndexedSplitImpl>(data_split, row_ranges, scores);
}

/// Java `QueryAuthSplit.writeString` of a non-null string: its length, then the UTF-8 bytes.
void WriteAuthString(const std::string& value, MemorySegmentOutputStream* out) {
    out->WriteValue<int32_t>(static_cast<int32_t>(value.size()));
    out->Write(value.data(), static_cast<uint32_t>(value.size()));
}

/// A null string reads as an empty one, which the rules refuse.
Result<std::string> ReadAuthString(DataInputStream* in) {
    PAIMON_ASSIGN_OR_RAISE(int32_t length, in->ReadValue<int32_t>());
    if (length < 0) {
        return std::string();
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t pos, in->GetPos());
    PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in->Length());
    if (length > stream_length - pos) {
        return Status::Invalid(
            fmt::format("invalid QueryAuthSplit, a string of {} bytes overruns the split", length));
    }
    std::string value(length, '\0');
    PAIMON_RETURN_NOT_OK(in->Read(value.data(), length));
    return value;
}

Status WriteWithSplitSerializer(const std::shared_ptr<Split>& split, MemorySegmentOutputStream* out,
                                const std::shared_ptr<MemoryPool>& pool) {
    out->WriteValue<int64_t>(QueryAuthSplit::kSplitSerializerMagic);
    out->WriteValue<int32_t>(QueryAuthSplit::kSplitSerializerVersion);
    if (auto data_split_impl = std::dynamic_pointer_cast<DataSplitImpl>(split)) {
        out->WriteValue<int32_t>(QueryAuthSplit::kSplitSerializerDataSplit);
        return WriteDataSplit(data_split_impl, out, pool);
    }
    if (auto indexed_split_impl = std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
        out->WriteValue<int32_t>(QueryAuthSplit::kSplitSerializerIndexedSplit);
        return WriteIndexedSplit(indexed_split_impl, out, pool);
    }
    return Status::Invalid("a QueryAuthSplit can only wrap a DataSplit or an IndexedSplit");
}

/// Java `QueryAuthSplit.serialize`, framed by `SplitSerializer`.
Status WriteQueryAuthSplitWithSplitSerializer(const QueryAuthSplit& split,
                                              MemorySegmentOutputStream* out,
                                              const std::shared_ptr<MemoryPool>& pool) {
    out->WriteValue<int64_t>(QueryAuthSplit::kSplitSerializerMagic);
    out->WriteValue<int32_t>(QueryAuthSplit::kSplitSerializerVersion);
    out->WriteValue<int32_t>(QueryAuthSplit::kSplitSerializerQueryAuthSplit);
    PAIMON_RETURN_NOT_OK(WriteWithSplitSerializer(split.GetSplit(), out, pool));
    const std::shared_ptr<TableQueryAuthResult>& auth_result = split.GetAuthResult();
    if (auth_result == nullptr) {
        return Status::Invalid("a QueryAuthSplit must carry the rules it was planned with");
    }
    out->WriteValue<bool>(true);
    out->WriteValue<bool>(true);
    out->WriteValue<int32_t>(static_cast<int32_t>(auth_result->Filter().size()));
    for (const std::string& filter : auth_result->Filter()) {
        WriteAuthString(filter, out);
    }
    out->WriteValue<bool>(true);
    out->WriteValue<int32_t>(static_cast<int32_t>(auth_result->ColumnMasking().size()));
    for (const auto& [column, transform] : auth_result->ColumnMasking()) {
        WriteAuthString(column, out);
        WriteAuthString(transform, out);
    }
    return Status::OK();
}

void WriteFieldIds(const std::map<std::string, int32_t>& field_ids,
                   MemorySegmentOutputStream* out) {
    out->WriteValue<int32_t>(static_cast<int32_t>(field_ids.size()));
    for (const auto& [field, id] : field_ids) {
        WriteAuthString(field, out);
        out->WriteValue<int32_t>(id);
    }
}

/// A split without a scope, as Java wrote it, is written in the Java format alone.
Status WriteQueryAuthSplit(const QueryAuthSplit& split, MemorySegmentOutputStream* out,
                           const std::shared_ptr<MemoryPool>& pool) {
    const std::optional<QueryAuthScope>& scope = split.GetScope();
    if (!scope) {
        return WriteQueryAuthSplitWithSplitSerializer(split, out, pool);
    }
    out->WriteValue<int64_t>(QueryAuthSplit::kScopedMagic);
    out->WriteValue<int32_t>(QueryAuthSplit::kScopedVersion);
    PAIMON_RETURN_NOT_OK(WriteQueryAuthSplitWithSplitSerializer(split, out, pool));
    WriteFieldIds(scope->authorized_field_ids, out);
    WriteFieldIds(scope->rule_field_ids, out);
    WriteAuthString(scope->table_path, out);
    WriteAuthString(scope->branch, out);
    return Status::OK();
}

/// Reads a split Java `SplitSerializer` wrote, after its magic number; `in_query_auth_split` when
/// it is the split a `QueryAuthSplit` wraps.
Result<std::shared_ptr<Split>> ReadWithSplitSerializerWithoutMagicNumber(
    DataInputStream* in, const std::shared_ptr<MemoryPool>& pool, bool in_query_auth_split) {
    PAIMON_ASSIGN_OR_RAISE(int32_t version, in->ReadValue<int32_t>());
    if (version != QueryAuthSplit::kSplitSerializerVersion) {
        return Status::Invalid(fmt::format("Unsupported split serializer version: {}", version));
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t type, in->ReadValue<int32_t>());
    if (type == QueryAuthSplit::kSplitSerializerDataSplit) {
        PAIMON_ASSIGN_OR_RAISE(int64_t magic, in->ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplitImpl> data_split,
                               ReadDataSplitWithoutMagicNumber(magic, in, pool));
        return std::shared_ptr<Split>(data_split);
    }
    if (type == QueryAuthSplit::kSplitSerializerIndexedSplit) {
        PAIMON_ASSIGN_OR_RAISE(int64_t magic, in->ReadValue<int64_t>());
        if (magic != IndexedSplitImpl::MAGIC) {
            return Status::Invalid("invalid IndexedSplit, wrong magic number");
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexedSplitImpl> indexed_split,
                               ReadIndexedSplitWithoutMagicNumber(in, pool));
        return std::shared_ptr<Split>(indexed_split);
    }
    if (type != QueryAuthSplit::kSplitSerializerQueryAuthSplit) {
        return Status::Invalid(fmt::format("Unsupported split type: {}", type));
    }
    // Refused before reading it, so nested wrappers cannot recurse without bound.
    if (in_query_auth_split) {
        return Status::Invalid("invalid QueryAuthSplit, it wraps another QueryAuthSplit");
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t inner_magic, in->ReadValue<int64_t>());
    if (inner_magic != QueryAuthSplit::kSplitSerializerMagic) {
        return Status::Invalid(
            "invalid QueryAuthSplit, its split is not framed by SplitSerializer");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<Split> inner,
        ReadWithSplitSerializerWithoutMagicNumber(in, pool, /*in_query_auth_split=*/true));
    PAIMON_ASSIGN_OR_RAISE(bool has_auth_result, ReadSplitFlag(in));
    if (!has_auth_result) {
        // Rules that cannot be read must not read as no rules.
        return Status::Invalid("invalid QueryAuthSplit, it carries no rules");
    }
    std::vector<std::string> filter;
    PAIMON_ASSIGN_OR_RAISE(bool has_filter, ReadSplitFlag(in));
    if (has_filter) {
        PAIMON_ASSIGN_OR_RAISE(int32_t size, ReadSplitCount(in, sizeof(int32_t), "QueryAuthSplit"));
        for (int32_t i = 0; i < size; ++i) {
            PAIMON_ASSIGN_OR_RAISE(std::string value, ReadAuthString(in));
            filter.push_back(std::move(value));
        }
    }
    std::map<std::string, std::string> column_masking;
    PAIMON_ASSIGN_OR_RAISE(bool has_column_masking, ReadSplitFlag(in));
    if (has_column_masking) {
        PAIMON_ASSIGN_OR_RAISE(int32_t size,
                               ReadSplitCount(in, 2 * sizeof(int32_t), "QueryAuthSplit"));
        for (int32_t i = 0; i < size; ++i) {
            PAIMON_ASSIGN_OR_RAISE(std::string column, ReadAuthString(in));
            PAIMON_ASSIGN_OR_RAISE(std::string transform, ReadAuthString(in));
            if (!column_masking.emplace(std::move(column), std::move(transform)).second) {
                return Status::Invalid("invalid QueryAuthSplit, its rules mask a column twice");
            }
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableQueryAuthResult> auth_result,
                           TableQueryAuthResult::Create(filter, column_masking));
    return std::shared_ptr<Split>(
        std::make_shared<QueryAuthSplit>(inner, auth_result, /*scope=*/std::nullopt));
}

Result<std::map<std::string, int32_t>> ReadFieldIds(DataInputStream* in) {
    PAIMON_ASSIGN_OR_RAISE(int32_t size, ReadSplitCount(in, 2 * sizeof(int32_t), "QueryAuthSplit"));
    std::map<std::string, int32_t> field_ids;
    for (int32_t i = 0; i < size; ++i) {
        PAIMON_ASSIGN_OR_RAISE(std::string field, ReadAuthString(in));
        PAIMON_ASSIGN_OR_RAISE(int32_t id, in->ReadValue<int32_t>());
        if (!field_ids.emplace(std::move(field), id).second) {
            return Status::Invalid("invalid QueryAuthSplit, its scope names a column twice");
        }
    }
    return field_ids;
}

/// Reads a split `WriteQueryAuthSplit` wrote with its scope, after the magic number.
Result<std::shared_ptr<Split>> ReadScopedQueryAuthSplitWithoutMagicNumber(
    DataInputStream* in, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(int32_t version, in->ReadValue<int32_t>());
    if (version != QueryAuthSplit::kScopedVersion) {
        return Status::Invalid(fmt::format("Unsupported QueryAuthSplit version: {}", version));
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t magic, in->ReadValue<int64_t>());
    if (magic != QueryAuthSplit::kSplitSerializerMagic) {
        return Status::Invalid(
            "invalid QueryAuthSplit, its split is not framed by SplitSerializer");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<Split> split,
        ReadWithSplitSerializerWithoutMagicNumber(in, pool, /*in_query_auth_split=*/false));
    auto auth_split = std::dynamic_pointer_cast<QueryAuthSplit>(split);
    if (auth_split == nullptr) {
        return Status::Invalid("invalid QueryAuthSplit, its envelope holds another split");
    }
    QueryAuthScope scope;
    PAIMON_ASSIGN_OR_RAISE(scope.authorized_field_ids, ReadFieldIds(in));
    PAIMON_ASSIGN_OR_RAISE(scope.rule_field_ids, ReadFieldIds(in));
    PAIMON_ASSIGN_OR_RAISE(scope.table_path, ReadAuthString(in));
    PAIMON_ASSIGN_OR_RAISE(scope.branch, ReadAuthString(in));
    return std::shared_ptr<Split>(std::make_shared<QueryAuthSplit>(
        auth_split->GetSplit(), auth_split->GetAuthResult(), std::move(scope)));
}

}  // namespace

Result<std::string> Split::Serialize(const std::shared_ptr<Split>& split,
                                     const std::shared_ptr<MemoryPool>& pool) {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    if (auto query_auth_split = std::dynamic_pointer_cast<QueryAuthSplit>(split)) {
        PAIMON_RETURN_NOT_OK(WriteQueryAuthSplit(*query_auth_split, &out, pool));
    } else if (auto data_split_impl = std::dynamic_pointer_cast<DataSplitImpl>(split)) {
        PAIMON_RETURN_NOT_OK(WriteDataSplit(data_split_impl, &out, pool));
    } else if (auto indexed_split_impl = std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
        PAIMON_RETURN_NOT_OK(WriteIndexedSplit(indexed_split_impl, &out, pool));
    } else if (std::dynamic_pointer_cast<FormatDataSplit>(split) != nullptr) {
        // A format table's plan has no cross-runtime encoding, and one of paimon-cpp's own would
        // let a plan made here be handed to a runtime that cannot read it back.
        return Status::NotImplemented(
            "a FormatDataSplit cannot be serialized: a format table's plan is in-memory only, so "
            "plan and read it within one process");
    } else {
        return Status::Invalid("invalid split, cannot cast to DataSplit or IndexedSplit");
    }
    PAIMON_UNIQUE_PTR<Bytes> bytes =
        MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
    return std::string(bytes->data(), bytes->size());
}

Result<std::shared_ptr<Split>> Split::Deserialize(const char* buffer, size_t length,
                                                  const std::shared_ptr<MemoryPool>& pool) {
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, length);
    DataInputStream in(input_stream);

    int64_t magic = -1;
    PAIMON_ASSIGN_OR_RAISE(magic, in.ReadValue<int64_t>());

    if (magic == QueryAuthSplit::kSplitSerializerMagic || magic == QueryAuthSplit::kScopedMagic) {
        std::shared_ptr<Split> split;
        if (magic == QueryAuthSplit::kScopedMagic) {
            PAIMON_ASSIGN_OR_RAISE(split, ReadScopedQueryAuthSplitWithoutMagicNumber(&in, pool));
        } else {
            PAIMON_ASSIGN_OR_RAISE(split, ReadWithSplitSerializerWithoutMagicNumber(
                                              &in, pool, /*in_query_auth_split=*/false));
        }
        PAIMON_ASSIGN_OR_RAISE(int64_t pos, in.GetPos());
        PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in.Length());
        if (pos != stream_length) {
            return Status::Invalid(
                fmt::format("invalid split byte stream, remaining {} bytes after deserializing",
                            stream_length - pos));
        }
        return split;
    } else if (magic == IndexedSplitImpl::MAGIC) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexedSplitImpl> indexed_split,
                               ReadIndexedSplitWithoutMagicNumber(&in, pool));
        // TODO(lisizhuo.lsz): support fallback split in IndexedSplit
        PAIMON_ASSIGN_OR_RAISE(int64_t pos, in.GetPos());
        PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in.Length());
        if (pos == stream_length) {
            return indexed_split;
        } else if (pos == stream_length - 1) {
            return Status::Invalid(
                "invalid IndexedSplit, do not support FallbackSplit in IndexedSplit");
        } else {
            return Status::Invalid(
                fmt::format("invalid IndexedSplit, remaining {} bytes after deserializing",
                            stream_length - pos));
        }
    } else if (magic == DataSplitImpl::MAGIC) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplitImpl> data_split,
                               ReadDataSplitWithoutMagicNumber(magic, &in, pool));
        PAIMON_ASSIGN_OR_RAISE(int64_t pos, in.GetPos());
        PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in.Length());
        if (pos == stream_length) {
            return data_split;
        } else if (pos == stream_length - 1) {
            PAIMON_ASSIGN_OR_RAISE(bool is_fallback, ReadSplitFlag(&in));
            return std::make_shared<FallbackDataSplit>(data_split, is_fallback);
        } else {
            return Status::Invalid(fmt::format(
                "invalid data split byte stream, remaining {} bytes after deserializing",
                stream_length - pos));
        }
    }
    return Status::Invalid("invalid split, must be DataSplit or IndexedSplit");
}
}  // namespace paimon

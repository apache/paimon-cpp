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

#include "paimon/core/realtime/index/hash/hash_realtime_index.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/realtime/realtime_utils.h"
#include "paimon/defs.h"
#include "paimon/macros.h"
#include "tbb/concurrent_vector.h"

namespace paimon {

struct HashRealtimeIndex::Posting {
    tbb::concurrent_vector<int64_t> offsets;
    std::atomic<size_t> published_size{0};
};

HashRealtimeIndex::Reader::Reader(std::shared_ptr<const HashRealtimeIndex> index,
                                  const OffsetRange& offset_range)
    : index_(std::move(index)), offset_range_(offset_range) {}

Result<std::optional<RoaringBitmap64>> HashRealtimeIndex::Reader::VisitEqual(
    const Literal& literal) {
    return VisitIn({literal});
}

Result<std::optional<RoaringBitmap64>> HashRealtimeIndex::Reader::VisitIn(
    const std::vector<Literal>& literals) {
    RoaringBitmap64 result;
    if (offset_range_.Empty()) {
        return std::optional<RoaringBitmap64>(std::move(result));
    }
    for (const Literal& literal : literals) {
        if (literal.IsNull()) {
            continue;
        }
        if (literal.GetType() != index_->field_type_) {
            return Status::Invalid("real-time hash index literal type mismatch: expected ",
                                   FieldTypeUtils::FieldTypeToString(index_->field_type_),
                                   ", actual ",
                                   FieldTypeUtils::FieldTypeToString(literal.GetType()));
        }
        const std::optional<std::shared_ptr<Posting>> posting =
            index_->postings_.Find(literal.HashCode());
        if (!posting) {
            continue;
        }
        const Posting& entry = *posting.value();
        const size_t published_size = entry.published_size.load(std::memory_order_acquire);
        const auto published_end = entry.offsets.cbegin() + published_size;
        auto begin = std::lower_bound(entry.offsets.cbegin(), published_end, offset_range_.begin);
        const auto end = std::lower_bound(begin, published_end, offset_range_.end);
        // TBB posting segments are not contiguous.
        for (; begin != end; ++begin) {
            result.Add(*begin);
        }
    }
    return std::optional<RoaringBitmap64>(std::move(result));
}

HashRealtimeIndex::HashRealtimeIndex(std::shared_ptr<arrow::Schema> schema, FieldType field_type)
    : schema_(std::move(schema)), field_type_(field_type) {}

Result<std::shared_ptr<RealtimeIndex>> HashRealtimeIndex::Create(
    const std::shared_ptr<arrow::Schema>& schema, const std::shared_ptr<MemoryPool>& pool) {
    if (!pool) {
        return Status::Invalid("real-time hash index requires a memory pool");
    }
    PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateOffsetField(schema));
    if (schema->num_fields() != 2 ||
        schema->field(0)->name() != SpecialFields::RealtimeOffset().Name() ||
        schema->field(1)->name() == SpecialFields::RealtimeOffset().Name()) {
        return Status::Invalid(
            "real-time hash index requires offset and exactly one indexed field");
    }
    PAIMON_ASSIGN_OR_RAISE(FieldType type, FieldTypeUtils::ConvertToFieldType(schema->field(1)));
    switch (type) {
        case FieldType::BOOLEAN:
        case FieldType::TINYINT:
        case FieldType::SMALLINT:
        case FieldType::INT:
        case FieldType::BIGINT:
        case FieldType::FLOAT:
        case FieldType::DOUBLE:
        case FieldType::STRING:
        case FieldType::BINARY:
        case FieldType::DATE:
            break;
        default:
            return Status::NotImplemented("unsupported real-time hash index field type: ",
                                          schema->field(1)->type()->ToString());
    }
    return std::shared_ptr<RealtimeIndex>(new HashRealtimeIndex(schema, type));
}

Status HashRealtimeIndex::AddBatch(::ArrowArray* batch) {
    if (!batch || !batch->release) {
        return Status::Invalid("real-time index batch is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> imported,
                                      arrow::ImportArray(batch, arrow::struct_(schema_->fields())));
    const std::shared_ptr<arrow::StructArray> data =
        checked_pointer_cast<arrow::StructArray>(imported);
    const std::shared_ptr<arrow::Int64Array> offsets = checked_pointer_cast<arrow::Int64Array>(
        data->GetFieldByName(SpecialFields::RealtimeOffset().Name()));
    if (data->null_count() != 0 || offsets->null_count() != 0) {
        return Status::Invalid("real-time index requires non-null offsets and rows");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<Literal> values, LiteralConverter::ConvertLiteralsFromArray(
                                                            *data->field(1), /*own_data=*/false));

    for (int64_t row = 0; row < offsets->length(); ++row) {
        const Literal& literal = values[row];
        if (literal.IsNull()) {
            continue;
        }
        const uint64_t hash = literal.HashCode();
        std::optional<std::shared_ptr<Posting>> found = postings_.Find(hash);
        std::shared_ptr<Posting> posting;
        if (found) {
            posting = found.value();
        } else {
            posting = std::make_shared<Posting>();
            postings_.Insert(hash, posting);
            posting_memory_usage_.fetch_add(
                sizeof(Posting) + sizeof(uint64_t) + sizeof(std::shared_ptr<Posting>),
                std::memory_order_relaxed);
        }
        tbb::concurrent_vector<int64_t>& list = posting->offsets;
        const size_t old_capacity = list.capacity();
        list.push_back(offsets->Value(row));
        // Publish initialized offsets; concurrent_vector::size() may include unfinished ones.
        posting->published_size.store(list.size(), std::memory_order_release);
        posting_memory_usage_.fetch_add((list.capacity() - old_capacity) * sizeof(int64_t),
                                        std::memory_order_relaxed);
    }
    return Status::OK();
}

Result<std::shared_ptr<RealtimeIndexReader>> HashRealtimeIndex::CreateReader(
    const OffsetRange& offset_range) const {
    if (offset_range.begin < 0 || offset_range.begin > offset_range.end) {
        return Status::Invalid("real-time index query offset range is invalid");
    }
    return std::shared_ptr<RealtimeIndexReader>(new Reader(shared_from_this(), offset_range));
}

uint64_t HashRealtimeIndex::GetMemoryUsage() const {
    // Estimates payload/capacity; excludes TBB buckets and allocator overhead.
    return posting_memory_usage_.load(std::memory_order_relaxed) + sizeof(*this);
}

const char* HashRealtimeIndexFactory::Identifier() const {
    return "hash-realtime";
}

Result<std::shared_ptr<RealtimeIndex>> HashRealtimeIndexFactory::Create(
    ::ArrowSchema* write_schema, const std::map<std::string, std::string>&,
    const std::shared_ptr<MemoryPool>& pool) const {
    if (!write_schema || !write_schema->release) {
        return Status::Invalid("real-time index schema is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(write_schema));
    return HashRealtimeIndex::Create(schema, pool);
}

}  // namespace paimon

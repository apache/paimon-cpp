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

#include "paimon/indexer/lumina/lumina_realtime_index.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "lumina/core/Constants.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/core/realtime/realtime_utils.h"
#include "paimon/indexer/lumina/lumina_memory_pool.h"
#include "paimon/indexer/lumina/lumina_utils.h"

namespace paimon::lumina {

LuminaRealtimeIndex::LuminaRealtimeIndex(
    std::shared_ptr<arrow::Schema> schema, LuminaIndexInfo index_info,
    std::unique_ptr<::lumina::extensions::StreamWithFilterExtension> filter,
    std::unique_ptr<::lumina::api::LuminaStreamer> streamer, std::unique_ptr<LuminaMemoryPool> pool)
    : schema_(std::move(schema)),
      index_info_(std::move(index_info)),
      pool_(std::move(pool)),
      filter_(std::move(filter)),
      streamer_(std::move(streamer)) {}

LuminaRealtimeIndex::~LuminaRealtimeIndex() = default;

Result<std::shared_ptr<LuminaRealtimeIndex>> LuminaRealtimeIndex::Create(
    const std::shared_ptr<arrow::Schema>& schema, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& pool) {
    if (!pool) {
        return Status::Invalid("Lumina realtime index requires a memory pool");
    }
    PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateOffsetField(schema));
    if (schema->num_fields() != 2 || schema->field(1)->type()->id() != arrow::Type::LIST ||
        checked_pointer_cast<arrow::ListType>(schema->field(1)->type())->value_type()->id() !=
            arrow::Type::FLOAT) {
        return Status::Invalid("Lumina realtime index requires [offset, LIST<FLOAT>]");
    }
    const std::map<std::string, std::string> lumina_options =
        OptionsUtils::FetchOptionsWithPrefix(LuminaDefines::kOptionKeyPrefix, options);
    if (lumina_options.count(std::string(::lumina::core::kExtensionTagSchema))) {
        return Status::NotImplemented("Lumina realtime tags are not supported");
    }
    PAIMON_ASSIGN_OR_RAISE(LuminaIndexInfo info, LuminaIndexOptions::GetIndexInfo(lumina_options));
    PAIMON_ASSIGN_OR_RAISE(::lumina::api::StreamerOptions streamer_options,
                           LuminaIndexOptions::CreateStreamerOptions(lumina_options));
    auto lumina_pool = std::make_unique<LuminaMemoryPool>(/*track_memory=*/true, pool);
    auto filter = std::make_unique<::lumina::extensions::StreamWithFilterExtension>();
    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::LuminaStreamer native_streamer,
        ::lumina::api::LuminaStreamer::Create(
            streamer_options, ::lumina::core::MemoryResourceConfig(lumina_pool.get())));
    auto streamer = std::make_unique<::lumina::api::LuminaStreamer>(std::move(native_streamer));
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(streamer->Attach(*filter));
    return std::shared_ptr<LuminaRealtimeIndex>(new LuminaRealtimeIndex(
        schema, std::move(info), std::move(filter), std::move(streamer), std::move(lumina_pool)));
}

Status LuminaRealtimeIndex::AddBatch(::ArrowArray* batch) {
    if (!batch || !batch->release) {
        return Status::Invalid("Lumina realtime batch is null or released");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> imported,
                                      arrow::ImportArray(batch, arrow::struct_(schema_->fields())));
    if (write_failed_) {
        return Status::Invalid("Lumina realtime index cannot write after an insertion failure");
    }
    PAIMON_RETURN_NOT_OK_FROM_ARROW(imported->ValidateFull());
    const auto data = checked_pointer_cast<arrow::StructArray>(imported);
    const auto offsets = checked_pointer_cast<arrow::Int64Array>(data->field(0));
    const auto vectors = checked_pointer_cast<arrow::ListArray>(data->field(1));
    const auto values = checked_pointer_cast<arrow::FloatArray>(vectors->values());
    if (data->null_count() != 0 || offsets->null_count() != 0) {
        return Status::Invalid("Lumina realtime rows and offsets must not be null");
    }
    for (int64_t row = 0; row < data->length(); ++row) {
        if (vectors->IsNull(row)) {
            continue;
        }
        if (vectors->value_length(row) != static_cast<int64_t>(index_info_.dimension)) {
            return Status::Invalid("Lumina realtime vector dimension mismatch");
        }
        for (int64_t pos = vectors->value_offset(row); pos < vectors->value_offset(row + 1);
             ++pos) {
            if (values->IsNull(pos) || !std::isfinite(values->Value(pos))) {
                return Status::Invalid(
                    "Lumina realtime vector elements must be finite non-null floats");
            }
        }
    }
    for (int64_t row = 0; row < data->length(); ++row) {
        if (vectors->IsNull(row)) {
            continue;
        }
        const Status status = LuminaToPaimonStatus(streamer_->Insert(
            values->raw_values() + vectors->value_offset(row), offsets->Value(row)));
        if (!status.ok()) {
            write_failed_ = true;
            return status;
        }
    }
    return Status::OK();
}

Result<std::shared_ptr<RealtimeIndexReader>> LuminaRealtimeIndex::CreateReader(
    const OffsetRange& offset_range, const std::shared_ptr<MemoryPool>& pool) const {
    if (offset_range.begin < 0 || offset_range.begin > offset_range.end) {
        return Status::Invalid("Lumina realtime query offset range is invalid");
    }
    if (!pool) {
        return Status::Invalid("Lumina realtime reader requires a query memory pool");
    }
    return std::shared_ptr<RealtimeIndexReader>(
        std::make_shared<LuminaRealtimeIndexReader>(shared_from_this(), offset_range, pool));
}

uint64_t LuminaRealtimeIndex::GetMemoryUsage() const {
    return pool_->GetMemoryUsage();
}

LuminaRealtimeIndexReader::LuminaRealtimeIndexReader(
    std::shared_ptr<const LuminaRealtimeIndex> index, const OffsetRange& offset_range,
    const std::shared_ptr<MemoryPool>& pool)
    : index_(std::move(index)), offset_range_(offset_range), pool_(pool) {}

bool LuminaRealtimeIndexReader::SupportsSearch(SearchType search_type) const {
    return search_type == SearchType::VECTOR_SEARCH;
}

Result<std::shared_ptr<ScoredRealtimeIndexResult>> LuminaRealtimeIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    if (!vector_search || vector_search->field_name != index_->schema_->field(1)->name() ||
        vector_search->limit <= 0) {
        return Status::Invalid("invalid Lumina realtime vector search field or limit");
    }
    if (vector_search->predicate) {
        return Status::NotImplemented("Lumina realtime tags are not supported");
    }
    for (float value : vector_search->query) {
        if (!std::isfinite(value)) {
            return Status::Invalid("Lumina realtime query vector must contain finite floats");
        }
    }
    PAIMON_ASSIGN_OR_RAISE(
        ::lumina::api::SearchOptions options,
        LuminaIndexOptions::CreateSearchOptions(*vector_search, index_->index_info_));
    if (offset_range_.Empty()) {
        return ScoredRealtimeIndexResult::Create(RoaringBitmap64(), {});
    }
    auto filter = [range = offset_range_,
                   user_filter = vector_search->pre_filter](::lumina::core::vector_id_t offset) {
        return offset >= static_cast<uint64_t>(range.begin) &&
               offset < static_cast<uint64_t>(range.end) &&
               (!user_filter || user_filter(static_cast<int64_t>(offset)));
    };
    ::lumina::api::Query query(vector_search->query.data(), vector_search->query.size());
    // Charge scratch allocations to the query, separately from retained index memory.
    LuminaMemoryPool session_pool(/*track_memory=*/false, pool_);
    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::SearchResult hits,
        index_->filter_->SearchWithFilter(query, std::move(filter), options, session_pool));
    std::sort(hits.topk.begin(), hits.topk.end(),
              [](const auto& left, const auto& right) { return left.id < right.id; });
    RoaringBitmap64 offsets;
    std::vector<float> scores;
    scores.reserve(hits.topk.size());
    int64_t previous = -1;
    for (const auto& hit : hits.topk) {
        if (hit.id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return Status::Invalid("Lumina returned an out-of-range realtime offset");
        }
        const int64_t offset = static_cast<int64_t>(hit.id);
        if (offset < offset_range_.begin || offset >= offset_range_.end || offset <= previous) {
            return Status::Invalid("Lumina returned invalid or duplicate realtime offsets");
        }
        offsets.Add(offset);
        scores.push_back(hit.distance);
        previous = offset;
    }
    return ScoredRealtimeIndexResult::Create(std::move(offsets), std::move(scores));
}

// TODO(xinyu.lxy): Dump the memory index with offset-to-(file, row-id) remapping when Lumina
// supports exporting the index structure, excluding skipped rows and respecting file rolling.

}  // namespace paimon::lumina

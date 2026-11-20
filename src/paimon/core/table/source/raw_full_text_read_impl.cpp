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

#include "paimon/core/table/source/raw_full_text_read_impl.h"

#include <algorithm>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/compute/api_vector.h"
#include "arrow/io/memory.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/math.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/core/table/source/filtered_row_id_reader.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/global_index/global_index_writer.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/global_index/lucene/lucene_defs.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/utils/roaring_bitmap64.h"
#include "paimon/utils/row_range_index.h"

namespace paimon {
struct InMemoryGlobalIndexFiles::Storage {
    explicit Storage(const std::shared_ptr<MemoryPool>& pool) : arrow_pool(GetArrowPool(pool)) {}

    std::shared_ptr<arrow::MemoryPool> arrow_pool;
    std::mutex mutex;
    std::map<std::string, std::shared_ptr<arrow::Buffer>> files;
};

class InMemoryGlobalIndexFiles::IndexOutputStream : public OutputStream {
 public:
    IndexOutputStream(const std::string& file_path, const std::shared_ptr<Storage>& storage,
                      const std::shared_ptr<arrow::io::BufferOutputStream>& output)
        : file_path_(file_path), storage_(storage), output_(output) {}

    Result<int64_t> Write(const char* buffer, int64_t size) override {
        PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(size, "write length"));
        if (size > 0 && buffer == nullptr) {
            return Status::Invalid("write buffer must not be null when size is positive");
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(output_->Write(buffer, size));
        return size;
    }

    Status Flush() override {
        return Status::OK();
    }

    Result<int64_t> GetPos() const override {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(int64_t position, output_->Tell());
        return position;
    }

    Result<std::string> GetUri() const override {
        return file_path_;
    }

    Status Close() override {
        if (output_->closed()) {
            return Status::OK();
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Buffer> bytes, output_->Finish());
        std::lock_guard<std::mutex> lock(storage_->mutex);
        storage_->files[file_path_] = std::move(bytes);
        return Status::OK();
    }

 private:
    std::string file_path_;
    std::shared_ptr<Storage> storage_;
    std::shared_ptr<arrow::io::BufferOutputStream> output_;
};

namespace {
class SharedBytesInputStream : public ByteArrayInputStream {
 public:
    SharedBytesInputStream(const std::shared_ptr<arrow::MemoryPool>& pool,
                           const std::shared_ptr<arrow::Buffer>& bytes,
                           const std::string& file_path)
        : ByteArrayInputStream(bytes->size() == 0 ? "" : bytes->data_as<char>(), bytes->size()),
          pool_(pool),
          bytes_(bytes),
          file_path_(file_path) {}

    Result<std::string> GetUri() const override {
        return file_path_;
    }

 private:
    // Arrow buffers free their memory through this pool, which must outlive bytes_.
    std::shared_ptr<arrow::MemoryPool> pool_;
    std::shared_ptr<arrow::Buffer> bytes_;
    std::string file_path_;
};

std::shared_ptr<ScoredGlobalIndexResult> EmptyResult() {
    return std::make_shared<BitmapScoredGlobalIndexResult>(RoaringBitmap64(), std::vector<float>());
}
}  // namespace

InMemoryGlobalIndexFiles::InMemoryGlobalIndexFiles(const std::shared_ptr<MemoryPool>& pool)
    : storage_(std::make_shared<Storage>(pool)) {}

Result<std::string> InMemoryGlobalIndexFiles::NewPath(const std::string& prefix) const {
    std::string uuid;
    if (!UUID::Generate(&uuid)) {
        return Status::IOError("failed to generate a UUID for a temporary full-text index file");
    }
    return fmt::format("memory://global-index/{}-{}.index", prefix, uuid);
}

Result<std::unique_ptr<OutputStream>> InMemoryGlobalIndexFiles::NewOutputStream(
    const std::string& file_path) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::io::BufferOutputStream> output,
        arrow::io::BufferOutputStream::Create(0, storage_->arrow_pool.get()));
    return std::unique_ptr<OutputStream>(
        std::make_unique<IndexOutputStream>(file_path, storage_, output));
}

Result<int64_t> InMemoryGlobalIndexFiles::GetFileSize(const std::string& file_path) const {
    std::lock_guard<std::mutex> lock(storage_->mutex);
    auto iter = storage_->files.find(file_path);
    if (iter == storage_->files.end()) {
        return Status::NotExist(fmt::format("in-memory index file {} does not exist", file_path));
    }
    return iter->second->size();
}

Result<std::unique_ptr<InputStream>> InMemoryGlobalIndexFiles::GetInputStream(
    const std::string& file_path) const {
    std::shared_ptr<arrow::Buffer> bytes;
    {
        std::lock_guard<std::mutex> lock(storage_->mutex);
        auto iter = storage_->files.find(file_path);
        if (iter == storage_->files.end()) {
            return Status::NotExist(
                fmt::format("in-memory index file {} does not exist", file_path));
        }
        bytes = iter->second;
    }
    return std::unique_ptr<InputStream>(
        std::make_unique<SharedBytesInputStream>(storage_->arrow_pool, bytes, file_path));
}

RawFullTextReadImpl::RawFullTextReadImpl(const std::string& root_path,
                                         const std::shared_ptr<TableSchema>& table_schema,
                                         const CoreOptions& options,
                                         const std::shared_ptr<PredicateFilter>& partition_filter,
                                         const std::shared_ptr<Predicate>& filter,
                                         const DataField& text_field, const std::string& query,
                                         int32_t limit, const std::shared_ptr<Executor>& executor,
                                         const std::shared_ptr<MemoryPool>& pool)
    : root_path_(root_path),
      table_schema_(table_schema),
      options_(options),
      partition_filter_(partition_filter),
      filter_(filter),
      text_field_(text_field),
      query_(query),
      limit_(limit),
      executor_(executor),
      pool_(pool) {}

Result<std::shared_ptr<ScoredGlobalIndexResult>> RawFullTextReadImpl::Search(
    int64_t snapshot_id, const std::string& index_type, const RowRangeIndex& raw_rows) const {
    if (raw_rows.Ranges().empty()) {
        return EmptyResult();
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                           GlobalIndexerFactory::Get(index_type, options_.ToMap()));
    if (!indexer || !indexer->SupportsFullTextSearch()) {
        return Status::NotImplemented(fmt::format(
            "cannot search unindexed rows with index type {}, which has no registered global "
            "indexer that supports full-text search",
            index_type));
    }
    PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot,
                           FilteredRowIdReader::LoadSnapshot(options_, root_path_, snapshot_id));
    auto index_schema = DataField::ConvertDataFieldsToArrowSchema({text_field_});
    auto files = std::make_shared<InMemoryGlobalIndexFiles>(pool_);
    std::shared_ptr<GlobalIndexWriter> writer;

    std::vector<std::string> read_fields = {text_field_.Name()};
    if (filter_) {
        std::set<std::string> filter_fields;
        PAIMON_RETURN_NOT_OK(PredicateUtils::GetAllNames(filter_, &filter_fields));
        for (const DataField& field : table_schema_->Fields()) {
            if (field.Name() != text_field_.Name() && filter_fields.count(field.Name()) > 0) {
                read_fields.push_back(field.Name());
            }
        }
    }

    std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(pool_);
    std::shared_ptr<PredicateFilter> bound_filter;
    const int64_t first_row_id = raw_rows.Ranges().front().from;
    const int64_t last_relative_id = raw_rows.Ranges().back().to - first_row_id;
    // Lucene requires contiguous document ids. Dense ids also keep the row count within int64_t.
    const bool use_relative_ids =
        index_type != lucene::kIdentifier && last_relative_id < std::numeric_limits<int64_t>::max();
    std::vector<int64_t> document_row_ids;
    RoaringBitmap64 matching_documents;
    auto consumer = [&](const arrow::StructArray& batch,
                        const arrow::Int64Array& row_ids) -> Status {
        std::shared_ptr<arrow::Array> text_array = batch.GetFieldByName(text_field_.Name());
        if (!text_array) {
            return Status::Invalid(
                fmt::format("unindexed rows are missing text column {}", text_field_.Name()));
        }
        PAIMON_ASSIGN_OR_RAISE(text_array, NestedProjectionUtils::AlignArrayToReadType(
                                               text_array, arrow::utf8(), arrow_pool.get()));
        std::vector<char> accepted;
        if (filter_) {
            if (!bound_filter) {
                const auto& struct_type = checked_cast<const arrow::StructType&>(*batch.type());
                std::map<std::string, int32_t> field_indexes =
                    ObjectUtils::CreateIdentifierToIndexMap(
                        struct_type.fields(),
                        [](const std::shared_ptr<arrow::Field>& field) { return field->name(); });
                PAIMON_ASSIGN_OR_RAISE(
                    std::shared_ptr<Predicate> bound_predicate,
                    PredicateUtils::CreatePickedFieldFilter(filter_, field_indexes));
                bound_filter = std::dynamic_pointer_cast<PredicateFilter>(bound_predicate);
                if (!bound_filter) {
                    return Status::Invalid(fmt::format(
                        "cannot evaluate row filter {} on unindexed rows", filter_->ToString()));
                }
            }
            PAIMON_ASSIGN_OR_RAISE(accepted, bound_filter->Test(batch, arrow_pool.get()));
        }

        arrow::BooleanBuilder selected(arrow_pool.get());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(selected.Reserve(batch.length()));
        std::vector<int64_t> document_ids;
        document_ids.reserve(batch.length());
        for (int64_t i = 0; i < batch.length(); ++i) {
            int64_t row_id = row_ids.Value(i);
            bool include = text_array->IsValid(i) && raw_rows.Intersects(row_id, row_id);
            selected.UnsafeAppend(include);
            if (!include) {
                continue;
            }
            int64_t document_id = row_id - first_row_id;
            if (!use_relative_ids) {
                document_id = static_cast<int64_t>(document_row_ids.size());
                document_row_ids.push_back(row_id);
            }
            if (filter_ && accepted[i]) {
                matching_documents.Add(document_id);
            }
            document_ids.push_back(document_id);
        }
        if (document_ids.empty()) {
            return Status::OK();
        }
        if (!writer) {
            ::ArrowSchema c_schema;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*index_schema, &c_schema));
            ScopeGuard guard([&c_schema]() { ArrowSchemaRelease(&c_schema); });
            PAIMON_ASSIGN_OR_RAISE(
                writer, indexer->CreateWriter(text_field_.Name(), &c_schema, files, pool_));
        }
        if (static_cast<int64_t>(document_ids.size()) != batch.length()) {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> mask,
                                              selected.Finish());
            arrow::compute::ExecContext context(arrow_pool.get());
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                arrow::Datum filtered,
                arrow::compute::Filter(text_array, mask, arrow::compute::FilterOptions::Defaults(),
                                       &context));
            text_array = filtered.make_array();
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> document_batch,
            arrow::StructArray::Make({text_array}, index_schema->fields()));
        ::ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*document_batch, &c_array));
        ScopeGuard guard([&c_array]() { ArrowArrayRelease(&c_array); });
        return writer->AddBatch(&c_array, std::move(document_ids));
    };
    PAIMON_RETURN_NOT_OK(FilteredRowIdReader::ReadRows(
        root_path_, table_schema_, options_, partition_filter_, snapshot, raw_rows, read_fields,
        /*predicate=*/nullptr, executor_, pool_, consumer));
    if (!writer || (filter_ && matching_documents.IsEmpty())) {
        return EmptyResult();
    }

    PAIMON_ASSIGN_OR_RAISE(std::vector<GlobalIndexIOMeta> index_metas, writer->Finish());
    if (index_metas.empty()) {
        return EmptyResult();
    }
    const int64_t reader_row_count =
        use_relative_ids ? last_relative_id + 1 : static_cast<int64_t>(document_row_ids.size());
    std::shared_ptr<GlobalIndexReader> reader;
    {
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*index_schema, &c_schema));
        ScopeGuard guard([&c_schema]() { ArrowSchemaRelease(&c_schema); });
        PAIMON_ASSIGN_OR_RAISE(
            reader, indexer->CreateReader(&c_schema, files, index_metas, reader_row_count,
                                          std::nullopt, pool_));
    }
    std::optional<RoaringBitmap64> include_documents;
    if (filter_) {
        include_documents = std::move(matching_documents);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredGlobalIndexResult> result,
                           reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                               text_field_.Name(), query_, limit_, std::move(include_documents))));
    if (!result) {
        return Status::NotImplemented(
            fmt::format("global index type {} does not answer full-text searches", index_type));
    }

    std::vector<std::pair<int64_t, float>> hits;
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScoredGlobalIndexResult::ScoredIterator> iterator,
                           result->CreateScoredIterator());
    while (iterator->HasNext()) {
        std::pair<int64_t, float> hit = iterator->NextWithScore();
        if (hit.first < 0 || hit.first >= reader_row_count) {
            return Status::Invalid(fmt::format("temporary {} index returned row {} outside [0, {})",
                                               index_type, hit.first, reader_row_count));
        }
        hits.emplace_back(use_relative_ids ? first_row_id + hit.first : document_row_ids[hit.first],
                          hit.second);
    }
    if (!use_relative_ids) {
        std::sort(hits.begin(), hits.end(),
                  [](const auto& left, const auto& right) { return left.first < right.first; });
    }
    RoaringBitmap64 row_ids;
    std::vector<float> scores;
    scores.reserve(hits.size());
    for (const auto& [row_id, score] : hits) {
        row_ids.Add(row_id);
        scores.push_back(score);
    }
    return std::shared_ptr<ScoredGlobalIndexResult>(
        std::make_shared<BitmapScoredGlobalIndexResult>(std::move(row_ids), std::move(scores)));
}

}  // namespace paimon

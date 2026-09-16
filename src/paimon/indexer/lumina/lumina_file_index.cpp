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

#include "paimon/indexer/lumina/lumina_file_index.h"

#include <cstring>
#include <map>
#include <utility>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "lumina/api/LuminaBuilder.h"
#include "lumina/core/Types.h"
#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/io/offset_input_stream.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/file_index/scored_file_index_result.h"
#include "paimon/indexer/lumina/lumina_file_writer.h"
#include "paimon/indexer/lumina/lumina_search_utils.h"
#include "paimon/indexer/lumina/lumina_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon::lumina {
namespace {

Result<std::shared_ptr<arrow::Schema>> ImportVectorSchema(::ArrowSchema* c_schema,
                                                          const std::string& owner) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(c_schema));
    if (schema->num_fields() == 0) {
        return Status::Invalid(fmt::format("{} requires at least one field", owner));
    }
    std::shared_ptr<arrow::ListType> list_type =
        std::dynamic_pointer_cast<arrow::ListType>(schema->field(0)->type());
    if (!list_type || list_type->value_type()->id() != arrow::Type::FLOAT) {
        return Status::Invalid(fmt::format("{} field type must be list[float]", owner));
    }
    return schema;
}

}  // namespace

LuminaFileIndexWriter::LuminaFileIndexWriter(std::string field_name,
                                             std::shared_ptr<arrow::DataType> arrow_type,
                                             const LuminaIndexInfo& index_info,
                                             ::lumina::api::BuilderOptions&& builder_options,
                                             std::vector<LuminaTagField>&& tag_fields,
                                             std::shared_ptr<LuminaMemoryPool> pool)
    : field_name_(std::move(field_name)),
      arrow_type_(std::move(arrow_type)),
      index_info_(index_info),
      builder_options_(std::move(builder_options)),
      tag_fields_(std::move(tag_fields)),
      pool_(std::move(pool)) {}

Status LuminaFileIndexWriter::AddBatch(::ArrowArray* batch) {
    if (serialized_) {
        return Status::Invalid("Cannot add data after serializing a Lumina File Index");
    }
    if (!batch || !batch->release) {
        return Status::Invalid("Lumina File Index batch cannot be null or released");
    }
    if (batch->length < 0 ||
        row_count_ > static_cast<int64_t>(RoaringBitmap32::MAX_VALUE) - batch->length) {
        return Status::Invalid("Lumina File Index row count exceeds the bitmap32 limit");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(batch, arrow_type_));
    if (array->null_count() != 0) {
        return Status::Invalid("Lumina File Index struct array must not contain null rows");
    }
    std::shared_ptr<arrow::StructArray> struct_array =
        std::dynamic_pointer_cast<arrow::StructArray>(array);
    if (!struct_array) {
        return Status::Invalid("Lumina File Index input must be a struct array");
    }
    std::shared_ptr<arrow::ListArray> vectors =
        std::dynamic_pointer_cast<arrow::ListArray>(struct_array->GetFieldByName(field_name_));
    if (!vectors) {
        return Status::Invalid("Lumina File Index field must be a list array");
    }

    PAIMON_RETURN_NOT_OK(accumulator_.AddBatch(struct_array, vectors, index_info_.dimension,
                                               tag_fields_, row_count_));
    row_count_ += array->length();
    return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<Bytes>> LuminaFileIndexWriter::SerializedBytes() const {
    if (serialized_) {
        return Status::Invalid("Lumina File Index has already been serialized");
    }
    serialized_ = true;
    if (accumulator_.IndexedCount() == 0) {
        return PAIMON_UNIQUE_PTR<Bytes>();
    }

    PAIMON_ASSIGN_OR_RAISE(::lumina::api::LuminaBuilder builder,
                           accumulator_.Build(builder_options_, index_info_.dimension,
                                              !tag_fields_.empty(), pool_.get()));

    auto segment_output = std::make_unique<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_->GetPaimonPool());
    std::shared_ptr<ByteArrayOutputStream> output =
        std::make_shared<ByteArrayOutputStream>(std::move(segment_output));
    auto file_writer = std::make_unique<LuminaFileWriter>(output);
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(
        builder.Dump(std::move(file_writer), ::lumina::api::IOOptions()));
    return output->Finish(pool_->GetPaimonPool().get());
}

LuminaFileIndexReader::LuminaFileIndexReader(
    const LuminaIndexInfo& index_info, std::unique_ptr<::lumina::api::LuminaSearcher>&& searcher,
    std::unique_ptr<::lumina::extensions::SearchWithFilterExtension>&& searcher_with_filter,
    std::unique_ptr<::lumina::extensions::experimental::SearchWithTagExtension>&& searcher_with_tag,
    std::shared_ptr<LuminaMemoryPool> pool)
    : index_info_(index_info),
      pool_(std::move(pool)),
      searcher_(std::move(searcher)),
      searcher_with_filter_(std::move(searcher_with_filter)),
      searcher_with_tag_(std::move(searcher_with_tag)) {}

LuminaFileIndexReader::~LuminaFileIndexReader() {
    [[maybe_unused]] ::lumina::core::Status status = searcher_->Close();
}

Result<std::shared_ptr<ScoredFileIndexResult>> LuminaFileIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    PAIMON_ASSIGN_OR_RAISE(::lumina::api::LuminaSearcher::SearchResult search_result,
                           LuminaSearchUtils::ExecuteVectorSearch(
                               *searcher_, *searcher_with_filter_, searcher_with_tag_.get(),
                               vector_search, index_info_, *pool_));

    std::map<int32_t, float> rows_with_scores;
    for (const auto& [row_id, score] : search_result.topk) {
        if (row_id < 0 || row_id > RoaringBitmap32::MAX_VALUE) {
            return Status::Invalid(
                fmt::format("Lumina returned out-of-range file row id {}", row_id));
        }
        if (!rows_with_scores.emplace(static_cast<int32_t>(row_id), score).second) {
            return Status::Invalid(fmt::format("Lumina returned duplicate file row id {}", row_id));
        }
    }
    RoaringBitmap32 row_positions;
    std::vector<float> scores;
    scores.reserve(rows_with_scores.size());
    for (const auto& [row_id, score] : rows_with_scores) {
        row_positions.Add(row_id);
        scores.push_back(score);
    }
    return ScoredFileIndexResult::Create(std::move(row_positions), std::move(scores));
}

Result<std::optional<std::vector<std::string>>> LuminaFileIndexer::GetExtraFieldNames() const {
    return LuminaTagUtils::GetExtraFieldNames(options_);
}

Result<std::shared_ptr<FileIndexWriter>> LuminaFileIndexer::CreateWriter(
    ::ArrowSchema* c_schema, const std::shared_ptr<MemoryPool>& pool) const {
    if (!pool) {
        return Status::Invalid("Lumina File Index memory pool cannot be null");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema,
                           ImportVectorSchema(c_schema, "Lumina File Index writer"));
    PAIMON_ASSIGN_OR_RAISE(LuminaIndexInfo index_info, LuminaIndexOptions::GetIndexInfo(options_));
    PAIMON_ASSIGN_OR_RAISE(std::vector<LuminaTagField> tag_fields,
                           LuminaTagUtils::ParseTagSchema(options_));
    std::shared_ptr<arrow::StructType> struct_type =
        std::dynamic_pointer_cast<arrow::StructType>(arrow::struct_(schema->fields()));
    PAIMON_RETURN_NOT_OK(LuminaTagUtils::ValidateTagFields(*struct_type, tag_fields));
    PAIMON_ASSIGN_OR_RAISE(::lumina::api::BuilderOptions builder_options,
                           LuminaIndexOptions::CreateBuilderOptions(options_));
    auto lumina_pool = std::make_shared<LuminaMemoryPool>(pool);
    return std::make_shared<LuminaFileIndexWriter>(schema->field(0)->name(), std::move(struct_type),
                                                   index_info, std::move(builder_options),
                                                   std::move(tag_fields), std::move(lumina_pool));
}

Result<std::shared_ptr<FileIndexReader>> LuminaFileIndexer::CreateReader(
    ::ArrowSchema* c_schema, int32_t start, int32_t length,
    const std::shared_ptr<InputStream>& input_stream,
    const std::shared_ptr<MemoryPool>& pool) const {
    if (!input_stream || !pool) {
        return Status::Invalid("Lumina File Index reader requires input stream and memory pool");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema,
                           ImportVectorSchema(c_schema, "Lumina File Index reader"));
    (void)schema;
    PAIMON_ASSIGN_OR_RAISE(LuminaIndexInfo index_info, LuminaIndexOptions::GetIndexInfo(options_));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OffsetInputStream> artifact_input,
                           OffsetInputStream::Create(input_stream, length, start));
    auto lumina_pool = std::make_shared<LuminaMemoryPool>(pool);
    PAIMON_ASSIGN_OR_RAISE(
        LuminaSearcherWithExtensions opened,
        LuminaSearchUtils::OpenSearcher(options_, index_info, artifact_input, lumina_pool.get()));
    return std::make_shared<LuminaFileIndexReader>(
        index_info, std::move(opened.searcher), std::move(opened.extensions.searcher_with_filter),
        std::move(opened.extensions.searcher_with_tag), std::move(lumina_pool));
}

}  // namespace paimon::lumina

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

#include "paimon/indexer/lumina/lumina_global_index.h"

#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "lumina/api/LuminaBuilder.h"
#include "lumina/api/LuminaSearcher.h"
#include "lumina/core/Constants.h"
#include "lumina/core/Status.h"
#include "lumina/core/Types.h"
#include "paimon/common/global_index/global_index_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/indexer/lumina/lumina_file_writer.h"
#include "paimon/indexer/lumina/lumina_index_options.h"
#include "paimon/indexer/lumina/lumina_search_utils.h"
#include "paimon/indexer/lumina/lumina_utils.h"
namespace paimon::lumina {
#define CHECK_NOT_NULL(pointer, error_msg)     \
    do {                                       \
        if (!(pointer)) {                      \
            return Status::Invalid(error_msg); \
        }                                      \
    } while (0)

Result<std::shared_ptr<GlobalIndexWriter>> LuminaGlobalIndex::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::DataType> arrow_type,
                                      arrow::ImportType(arrow_schema));
    // check data type
    auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(arrow_type);
    CHECK_NOT_NULL(struct_type, "arrow schema must be struct type when create LuminaIndexWriter");
    auto index_field = struct_type->GetFieldByName(field_name);
    CHECK_NOT_NULL(index_field,
                   fmt::format("field {} not exist in arrow schema when create LuminaIndexWriter",
                               field_name));
    auto list_type = std::dynamic_pointer_cast<arrow::ListType>(index_field->type());
    CHECK_NOT_NULL(list_type, "field type must be list[float] when create LuminaIndexWriter");
    if (list_type->value_type()->id() != arrow::Type::type::FLOAT) {
        return Status::Invalid("field type must be list[float] when create LuminaIndexWriter");
    }

    // check options
    auto lumina_options =
        OptionsUtils::FetchOptionsWithPrefix(LuminaDefines::kOptionKeyPrefix, options_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<LuminaTagField> tag_fields,
                           LuminaTagUtils::ParseTagSchema(lumina_options));
    PAIMON_RETURN_NOT_OK(LuminaTagUtils::ValidateTagFields(*struct_type, tag_fields));
    PAIMON_ASSIGN_OR_RAISE(uint32_t dimension, LuminaIndexOptions::GetDimension(lumina_options));
    PAIMON_ASSIGN_OR_RAISE(::lumina::api::BuilderOptions builder_options,
                           LuminaIndexOptions::CreateBuilderOptions(lumina_options));
    auto lumina_pool = std::make_shared<LuminaMemoryPool>(pool);
    return std::make_shared<LuminaIndexWriter>(
        field_name, arrow_type, dimension, file_writer, std::move(builder_options),
        ::lumina::api::IOOptions(), lumina_options, std::move(tag_fields), lumina_pool);
}

Result<LuminaIndexReader::IndexInfo> LuminaIndexReader::GetIndexInfo(
    const GlobalIndexIOMeta& io_meta) {
    auto meta_bytes = io_meta.metadata;
    if (!meta_bytes) {
        return Status::Invalid("Lumina global index must have meta data");
    }
    std::map<std::string, std::string> lumina_write_options;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::FromJsonString(
        std::string(meta_bytes->data(), meta_bytes->size()), &lumina_write_options));

    return LuminaIndexOptions::GetIndexInfo(lumina_write_options);
}

Result<std::shared_ptr<GlobalIndexReader>> LuminaGlobalIndex::CreateReader(
    ::ArrowSchema* c_arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_manager,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (files.size() != 1) {
        return Status::Invalid("lumina index only has one index file per shard");
    }
    const auto& io_meta = files[0];
    // check data type
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid("LuminaGlobalIndex now only support one field");
    }
    auto index_field = arrow_schema->field(0);
    auto list_type = std::dynamic_pointer_cast<arrow::ListType>(index_field->type());
    CHECK_NOT_NULL(list_type, "field type must be list[float] when create LuminaIndexReader");
    if (list_type->value_type()->id() != arrow::Type::type::FLOAT) {
        return Status::Invalid("field type must be list[float] when create LuminaIndexReader");
    }

    // get index info from meta
    PAIMON_ASSIGN_OR_RAISE(LuminaIndexReader::IndexInfo index_info,
                           LuminaIndexReader::GetIndexInfo(io_meta));

    auto lumina_pool = std::make_shared<LuminaMemoryPool>(pool);
    auto lumina_options =
        OptionsUtils::FetchOptionsWithPrefix(LuminaDefines::kOptionKeyPrefix, options_);
    // get input stream and open index
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> in,
                           file_manager->GetInputStream(io_meta.file_path));
    PAIMON_ASSIGN_OR_RAISE(
        LuminaSearcherWithExtensions opened,
        LuminaSearchUtils::OpenSearcher(lumina_options, index_info, in, lumina_pool.get()));
    return std::make_shared<LuminaIndexReader>(
        index_info, std::move(opened.searcher), std::move(opened.extensions.searcher_with_filter),
        std::move(opened.extensions.searcher_with_tag), lumina_pool);
}

Result<std::optional<std::vector<std::string>>> LuminaGlobalIndex::GetExtraFieldNames() const {
    auto lumina_options =
        OptionsUtils::FetchOptionsWithPrefix(LuminaDefines::kOptionKeyPrefix, options_);
    return LuminaTagUtils::GetExtraFieldNames(lumina_options);
}

LuminaIndexWriter::LuminaIndexWriter(
    const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
    uint32_t dimension, const std::shared_ptr<GlobalIndexFileWriter>& file_manager,
    ::lumina::api::BuilderOptions&& builder_options, ::lumina::api::IOOptions&& io_options,
    const std::map<std::string, std::string>& lumina_options,
    std::vector<LuminaTagField>&& tag_fields, const std::shared_ptr<LuminaMemoryPool>& pool)
    : pool_(pool),
      field_name_(field_name),
      arrow_type_(arrow_type),
      dimension_(dimension),
      file_manager_(file_manager),
      builder_options_(std::move(builder_options)),
      io_options_(std::move(io_options)),
      lumina_options_(lumina_options),
      tag_fields_(std::move(tag_fields)) {}

Status LuminaIndexWriter::AddBatch(::ArrowArray* arrow_array,
                                   std::vector<int64_t>&& relative_row_ids) {
    PAIMON_RETURN_NOT_OK(
        GlobalIndexUtils::CheckRelativeRowIds(arrow_array, relative_row_ids, count_));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(arrow_array, arrow_type_));
    if (array->null_count() != 0) {
        return Status::Invalid("arrow_array in LuminaIndexWriter is invalid, must not null");
    }
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    CHECK_NOT_NULL(struct_array, "invalid input array in LuminaIndexWriter, must be struct array");
    auto field_array = struct_array->GetFieldByName(field_name_);
    CHECK_NOT_NULL(
        field_array,
        fmt::format("invalid input array in LuminaIndexWriter, field {} not in input array",
                    field_name_));
    auto list_field_array = std::dynamic_pointer_cast<arrow::ListArray>(field_array);
    CHECK_NOT_NULL(list_field_array,
                   "invalid input array in LuminaIndexWriter, field array must be list array");

    PAIMON_RETURN_NOT_OK(
        accumulator_.AddBatch(struct_array, list_field_array, dimension_, tag_fields_, count_));

    count_ += array->length();
    return Status::OK();
}

Result<std::vector<GlobalIndexIOMeta>> LuminaIndexWriter::Finish() {
    if (accumulator_.IndexedCount() == 0) {
        return std::vector<GlobalIndexIOMeta>();
    }
    PAIMON_ASSIGN_OR_RAISE(
        ::lumina::api::LuminaBuilder builder,
        accumulator_.Build(builder_options_, dimension_, !tag_fields_.empty(), pool_.get()));

    // dump index
    PAIMON_ASSIGN_OR_RAISE(std::string index_file_name,
                           file_manager_->NewFileName(LuminaDefines::kIdentifier));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out,
                           file_manager_->NewOutputStream(index_file_name));
    auto file_writer = std::make_unique<LuminaFileWriter>(out);
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(builder.Dump(std::move(file_writer), io_options_));
    // prepare GlobalIndexIOMeta
    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, file_manager_->GetFileSize(index_file_name));
    std::string options_json;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::ToJsonString(lumina_options_, &options_json));
    auto meta_bytes = std::make_shared<Bytes>(options_json, pool_->GetPaimonPool().get());
    GlobalIndexIOMeta meta(file_manager_->ToPath(index_file_name), file_size,
                           /*metadata=*/meta_bytes);
    return std::vector<GlobalIndexIOMeta>({meta});
}

LuminaIndexReader::LuminaIndexReader(
    const LuminaIndexReader::IndexInfo& index_info,
    std::unique_ptr<::lumina::api::LuminaSearcher>&& searcher,
    std::unique_ptr<::lumina::extensions::SearchWithFilterExtension>&& searcher_with_filter,
    std::unique_ptr<::lumina::extensions::experimental::SearchWithTagExtension>&& searcher_with_tag,
    const std::shared_ptr<LuminaMemoryPool>& pool)
    : index_info_(index_info),
      pool_(pool),
      searcher_(std::move(searcher)),
      searcher_with_filter_(std::move(searcher_with_filter)),
      searcher_with_tag_(std::move(searcher_with_tag)) {}

Result<std::shared_ptr<ScoredGlobalIndexResult>> LuminaIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    PAIMON_ASSIGN_OR_RAISE(::lumina::api::LuminaSearcher::SearchResult search_result,
                           LuminaSearchUtils::ExecuteVectorSearch(
                               *searcher_, *searcher_with_filter_, searcher_with_tag_.get(),
                               vector_search, index_info_, *pool_));

    // prepare BitmapScoredGlobalIndexResult
    std::map<int64_t, float> id_to_score;
    for (const auto& [id, score] : search_result.topk) {
        id_to_score[id] = score;
    }

    RoaringBitmap64 bitmap;
    std::vector<float> scores;
    scores.reserve(id_to_score.size());
    for (const auto& [id, score] : id_to_score) {
        bitmap.Add(id);
        scores.push_back(score);
    }
    return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap), std::move(scores));
}

}  // namespace paimon::lumina

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

#include "paimon/core/operation/precise_file_index_read.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/abi.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/reader/complete_index_score_file_batch_reader.h"
#include "paimon/common/reader/predicate_batch_reader.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/deletionvectors/bitmap64_deletion_vector.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/file_index_evaluator.h"
#include "paimon/core/io/file_index_options.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/file_index/scored_file_index_result.h"
#include "paimon/format/reader_builder.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
namespace {

Result<std::string> ResolveSearchFieldName(
    const std::string& field_name, const std::map<std::string, std::string>& field_name_mapping,
    const std::string& file_name) {
    auto iter = field_name_mapping.find(field_name);
    if (iter == field_name_mapping.end()) {
        return Status::Invalid(
            fmt::format("Search field '{}' has no matching field ID in data schema for file '{}'",
                        field_name, file_name));
    }
    return iter->second;
}

Status PreparePredicate(
    const std::shared_ptr<Predicate>& predicate,
    const std::map<std::string, DataField>& read_by_name,
    const std::map<int32_t, std::pair<int32_t, DataField>>& data_by_id,
    const std::vector<std::string>& partition_keys, FileIndexEvaluator* indexes,
    std::map<std::shared_ptr<LeafPredicate>, std::shared_ptr<LeafPredicate>>* mapped_leaves) {
    return PreciseIndexEvaluator<RoaringBitmap32, ScoredFileIndexResult>::Prepare(
        predicate, [&](const std::shared_ptr<LeafPredicate>& leaf) -> Status {
            bool full_text = leaf->GetFunction().GetType() == Function::Type::FULL_TEXT_MATCH;
            if (!full_text && ObjectUtils::Contains(partition_keys, leaf->FieldName())) {
                mapped_leaves->emplace(leaf, nullptr);
                return Status::OK();
            }
            std::optional<std::shared_ptr<Predicate>> mapped;
            PAIMON_ASSIGN_OR_RAISE(mapped, FieldMappingBuilder::ReconstructPredicateWithDataFields(
                                               leaf, read_by_name, data_by_id));
            std::shared_ptr<LeafPredicate> mapped_leaf;
            if (mapped) {
                mapped_leaf = std::dynamic_pointer_cast<LeafPredicate>(*mapped);
                if (!mapped_leaf) {
                    return Status::Invalid("Mapped file predicate must be a leaf");
                }
                if (full_text) {
                    PAIMON_ASSIGN_OR_RAISE(
                        [[maybe_unused]] std::shared_ptr<FileIndexReader> reader,
                        indexes->GetSearchReader(mapped_leaf->FieldName(),
                                                 FileIndexReader::SearchType::FULL_TEXT_MATCH));
                }
            }
            mapped_leaves->emplace(leaf, std::move(mapped_leaf));
            return Status::OK();
        });
}

Status PrepareSearch(const std::shared_ptr<InternalReadContext>& context,
                     const std::map<std::string, std::string>& field_names,
                     const std::string& file_name, FileIndexEvaluator* indexes,
                     std::shared_ptr<VectorSearch>* vector_search,
                     std::shared_ptr<FullTextSearch>* full_text_search) {
    if (context->GetVectorSearch()) {
        *vector_search = std::make_shared<VectorSearch>(*context->GetVectorSearch());
        PAIMON_ASSIGN_OR_RAISE(
            (*vector_search)->field_name,
            ResolveSearchFieldName((*vector_search)->field_name, field_names, file_name));
        PAIMON_ASSIGN_OR_RAISE(
            [[maybe_unused]] std::shared_ptr<FileIndexReader> reader,
            indexes->GetSearchReader((*vector_search)->field_name,
                                     FileIndexReader::SearchType::VECTOR_SEARCH));
    }
    if (context->GetFullTextSearch()) {
        *full_text_search = std::make_shared<FullTextSearch>(*context->GetFullTextSearch());
        PAIMON_ASSIGN_OR_RAISE(
            (*full_text_search)->field_name,
            ResolveSearchFieldName((*full_text_search)->field_name, field_names, file_name));
        PAIMON_ASSIGN_OR_RAISE(
            [[maybe_unused]] std::shared_ptr<FileIndexReader> reader,
            indexes->GetSearchReader((*full_text_search)->field_name,
                                     FileIndexReader::SearchType::FULL_TEXT_SEARCH));
    }
    return Status::OK();
}

}  // namespace

Result<std::unique_ptr<PreciseFileIndexRead>> PreciseFileIndexRead::Create(
    const RawFileSplitRead& split_read, const BinaryRow& partition,
    const std::shared_ptr<DataFileMeta>& file, const std::shared_ptr<arrow::Schema>& read_schema,
    const std::shared_ptr<Predicate>& predicate, const FieldMappingBuilder* field_mapping_builder,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::map<std::string, std::string>& extra_format_options) {
    std::shared_ptr<TableSchema> file_schema = split_read.context_->GetTableSchema();
    if (file->schema_id != file_schema->Id()) {
        PAIMON_ASSIGN_OR_RAISE(file_schema,
                               split_read.schema_manager_->ReadSchema(file->schema_id));
    }
    const auto data_schema = DataField::ConvertDataFieldsToArrowSchema(file_schema->Fields());
    if (file->row_count < 0 || file->row_count > RoaringBitmap32::MAX_VALUE) {
        return Status::NotImplemented(
            "Precise file-index evaluation requires bitmap32 physical row positions");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> data_fields,
                           DataField::ConvertArrowSchemaToDataFields(data_schema));
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> read_fields,
                           DataField::ConvertArrowSchemaToDataFields(read_schema));
    auto field_names = FieldMappingBuilder::CreateFieldNameMapping(
        split_read.context_->GetTableSchema()->Fields(), data_fields);
    std::map<std::string, std::string> index_options;
    PAIMON_ASSIGN_OR_RAISE(index_options,
                           FileIndexOptions::RewriteFieldNames(
                               split_read.context_->GetCoreOptions().ToMap(), field_names));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FileIndexEvaluator> indexes,
        FileIndexEvaluator::Create(data_schema, index_options, path_factory, file,
                                   split_read.context_->GetCoreOptions().GetFileSystem(),
                                   split_read.pool_));

    std::map<std::string, DataField> read_by_name;
    for (const auto& field : read_fields) {
        read_by_name.emplace(field.Name(), field);
    }
    // Field ID -> (zero-based position in data_fields, field definition from the file's schema).
    std::map<int32_t, std::pair<int32_t, DataField>> data_by_id;
    for (size_t i = 0; i < data_fields.size(); ++i) {
        data_by_id.emplace(data_fields[i].Id(), std::make_pair(i, data_fields[i]));
    }
    std::map<std::shared_ptr<LeafPredicate>, std::shared_ptr<LeafPredicate>> mapped_leaves;
    // Precheck the entire tree before an empty candidate set can short-circuit it.
    PAIMON_RETURN_NOT_OK(PreparePredicate(predicate, read_by_name, data_by_id,
                                          split_read.context_->GetPartitionKeys(), indexes.get(),
                                          &mapped_leaves));
    std::shared_ptr<VectorSearch> vector_search;
    std::shared_ptr<FullTextSearch> full_text_search;
    PAIMON_RETURN_NOT_OK(PrepareSearch(split_read.context_, field_names, file->file_name,
                                       indexes.get(), &vector_search, &full_text_search));
    return std::unique_ptr<PreciseFileIndexRead>(new PreciseFileIndexRead(
        split_read, partition, file, read_schema, predicate, field_mapping_builder, path_factory,
        extra_format_options, std::move(indexes), mapped_leaves, vector_search, full_text_search));
}

PreciseFileIndexRead::PreciseFileIndexRead(
    const RawFileSplitRead& split_read, const BinaryRow& partition,
    const std::shared_ptr<DataFileMeta>& file, const std::shared_ptr<arrow::Schema>& read_schema,
    const std::shared_ptr<Predicate>& predicate, const FieldMappingBuilder* field_mapping_builder,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::map<std::string, std::string>& extra_format_options,
    std::unique_ptr<FileIndexEvaluator> indexes,
    const std::map<std::shared_ptr<LeafPredicate>, std::shared_ptr<LeafPredicate>>& mapped_leaves,
    const std::shared_ptr<VectorSearch>& vector_search,
    const std::shared_ptr<FullTextSearch>& full_text_search)
    : split_read_(split_read),
      partition_(partition),
      file_(file),
      read_schema_(read_schema),
      predicate_(predicate),
      field_mapping_builder_(field_mapping_builder),
      path_factory_(path_factory),
      extra_format_options_(extra_format_options),
      indexes_(std::move(indexes)),
      mapped_leaves_(mapped_leaves),
      vector_search_(vector_search),
      full_text_search_(full_text_search) {}

Result<RoaringBitmap32> PreciseFileIndexRead::CreateCandidates(
    const DeletionVector::Factory& dv_factory,
    const std::optional<std::vector<Range>>& row_ranges) const {
    RoaringBitmap32 candidates;
    if (row_ranges) {
        for (const auto& range : *row_ranges) {
            candidates.AddRange(static_cast<int32_t>(range.from),
                                static_cast<int32_t>(range.to + 1));
        }
    } else {
        candidates.AddRange(0, static_cast<int32_t>(file_->row_count));
    }
    if (dv_factory) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DeletionVector> dv, dv_factory(file_->file_name));
        if (auto bitmap = dynamic_cast<BitmapDeletionVector*>(dv.get())) {
            candidates -= *bitmap->GetBitmap();
        } else if (dv) {
            return Status::NotImplemented(
                "Precise file-index evaluation requires bitmap32 deletion vectors");
        }
    }
    return candidates;
}

Result<std::optional<double>> PreciseFileIndexRead::EstimateLeafSelectivity(
    const std::shared_ptr<LeafPredicate>& predicate) const {
    auto iter = mapped_leaves_.find(predicate);
    if (iter == mapped_leaves_.end()) {
        return Status::Invalid("File predicate mapping was not prepared");
    }
    if (!iter->second) {
        return std::optional<double>();
    }
    return indexes_->EstimateSelectivity(iter->second);
}

Result<PreciseFileIndexRead::Evaluation> PreciseFileIndexRead::EvaluateLeaf(
    const std::shared_ptr<LeafPredicate>& predicate, const RoaringBitmap32& candidates) const {
    auto iter = mapped_leaves_.find(predicate);
    if (iter == mapped_leaves_.end()) {
        return Status::Invalid("File predicate mapping was not prepared");
    }
    bool needs_scores = split_read_.context_->NeedsScores(
        {InternalReadContext::IndexScoreSource::Type::FULL_TEXT_MATCH, predicate->FieldName()});
    return Evaluator::EvaluateLeaf<FileIndexReader>(
        predicate, iter->second, candidates, needs_scores,
        [this](const std::shared_ptr<LeafPredicate>& leaf, const RoaringBitmap32& rows) {
            return ScanScalar(leaf, rows);
        },
        indexes_.get());
}

Result<std::unique_ptr<FileBatchReader>> PreciseFileIndexRead::CreateSelectedFileReader(
    const RoaringBitmap32& selection, const FieldMappingBuilder* field_mapping_builder) const {
    PAIMON_ASSIGN_OR_RAISE(std::string format, file_->FileFormat());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> builder,
                           split_read_.PrepareReaderBuilder(format, extra_format_options_));
    return split_read_.CreateFieldMappingReader(path_factory_->ToPath(file_), file_, partition_,
                                                std::move(builder), field_mapping_builder, nullptr,
                                                std::nullopt, path_factory_, &selection);
}

Result<RoaringBitmap32> PreciseFileIndexRead::ScanScalar(
    const std::shared_ptr<LeafPredicate>& predicate, const RoaringBitmap32& input) const {
    auto field = read_schema_->GetFieldByName(predicate->FieldName());
    if (!field) {
        return Status::Invalid(fmt::format(
            "Scalar predicate field '{}' is missing from the read schema", predicate->FieldName()));
    }
    // TODO(lisizhuo.lsz): Push the scalar predicate to use min/max statistics for pruning row
    // groups/pages.
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FieldMappingBuilder> field_mapping_builder,
        FieldMappingBuilder::Create(arrow::schema({field}),
                                    split_read_.context_->GetPartitionKeys(), nullptr));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> reader,
                           CreateSelectedFileReader(input, field_mapping_builder.get()));
    FileBatchReader& file_reader = *reader;
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<PredicateBatchReader> filtered_reader,
        PredicateBatchReader::Create(std::move(reader), predicate,
                                     split_read_.context_->GetArrowMemoryPool()));
    ScopeGuard close([&]() { filtered_reader->Close(); });
    RoaringBitmap32 matches;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch,
                               filtered_reader->NextBatchWithBitmap());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        ScopeGuard release_batch(
            [&batch]() { ReaderUtils::ReleaseReadBatch(std::move(batch.first)); });
        for (auto iter = batch.second.Begin(); iter != batch.second.End(); ++iter) {
            int32_t batch_row = *iter;
            if (batch_row < 0 || batch_row >= batch.first.first->length) {
                return Status::Invalid("Scalar scan returned an invalid batch row position");
            }
            PAIMON_ASSIGN_OR_RAISE(uint64_t row, file_reader.GetPreviousBatchFileRowId(batch_row));
            if (row >= static_cast<uint64_t>(file_->row_count)) {
                return Status::Invalid("Scalar scan returned an invalid file row position");
            }
            matches.Add(static_cast<int32_t>(row));
        }
    }
    return matches;
}

Status PreciseFileIndexRead::ApplyRootSearch(Evaluation* evaluated) const {
    // Score bindings use the original field name, not the historical file schema's name.
    const auto& context = split_read_.context_;
    std::string field;
    if (context->GetVectorSearch()) {
        field = context->GetVectorSearch()->field_name;
    } else if (context->GetFullTextSearch()) {
        field = context->GetFullTextSearch()->field_name;
    }
    return Evaluator::ApplyRootSearch<FileIndexReader>(vector_search_, full_text_search_, field,
                                                       indexes_.get(), evaluated);
}

Result<std::unique_ptr<FileBatchReader>> PreciseFileIndexRead::CreateReader(
    const DeletionVector::Factory& dv_factory,
    const std::optional<std::vector<Range>>& row_ranges) const {
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 candidates, CreateCandidates(dv_factory, row_ranges));
    Evaluator evaluator([this](const std::shared_ptr<LeafPredicate>& leaf,
                               const RoaringBitmap32& rows) { return EvaluateLeaf(leaf, rows); },
                        [this](const std::shared_ptr<LeafPredicate>& leaf) {
                            return EstimateLeafSelectivity(leaf);
                        });
    PAIMON_ASSIGN_OR_RAISE(Evaluation evaluated, evaluator.Evaluate(predicate_, candidates));
    if (evaluated.rows.IsEmpty()) {
        return std::unique_ptr<FileBatchReader>();
    }
    PAIMON_RETURN_NOT_OK(ApplyRootSearch(&evaluated));
    if (evaluated.rows.IsEmpty()) {
        return std::unique_ptr<FileBatchReader>();
    }
    std::map<std::string, std::vector<float>> output_scores;
    PAIMON_ASSIGN_OR_RAISE(
        output_scores,
        Evaluator::CreateOutputScores(evaluated, split_read_.context_->GetIndexScoreBindings(),
                                      split_read_.context_->GetReadSchema()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> reader,
                           CreateSelectedFileReader(evaluated.rows, field_mapping_builder_));
    if (!output_scores.empty()) {
        return std::unique_ptr<FileBatchReader>(std::make_unique<CompleteIndexScoreFileBatchReader>(
            std::move(reader), output_scores, split_read_.context_->GetArrowMemoryPool()));
    }
    return reader;
}

}  // namespace paimon

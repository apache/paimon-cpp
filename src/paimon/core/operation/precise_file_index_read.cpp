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
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/file_index/file_index_result.h"
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
    if (!predicate) {
        return Status::OK();
    }
    if (auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        const Function::Type type = compound->GetFunction().GetType();
        if (type != Function::Type::AND && type != Function::Type::OR) {
            return Status::Invalid(
                fmt::format("unsupported compound file predicate {}", predicate->ToString()));
        }
        for (const auto& child : compound->Children()) {
            PAIMON_RETURN_NOT_OK(PreparePredicate(child, read_by_name, data_by_id, partition_keys,
                                                  indexes, mapped_leaves));
        }
        return Status::OK();
    }
    auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    if (!leaf) {
        return Status::Invalid("File predicate must contain leaf or compound predicates");
    }
    bool full_text = leaf->GetFunction().GetType() == Function::Type::FULL_TEXT_MATCH;
    if (!full_text && ObjectUtils::Contains(partition_keys, leaf->FieldName())) {
        mapped_leaves->emplace(leaf, nullptr);
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::optional<std::shared_ptr<Predicate>> mapped,
        FieldMappingBuilder::ReconstructPredicateWithDataFields(leaf, read_by_name, data_by_id));
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
    const std::shared_ptr<DataFileMeta>& file, const FieldMappingBuilder* field_mapping_builder,
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
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<DataField> read_fields,
        DataField::ConvertArrowSchemaToDataFields(split_read.context_->GetReadSchema()));
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
    PAIMON_RETURN_NOT_OK(PreparePredicate(split_read.context_->GetPredicate(), read_by_name,
                                          data_by_id, split_read.context_->GetPartitionKeys(),
                                          indexes.get(), &mapped_leaves));
    std::shared_ptr<VectorSearch> vector_search;
    std::shared_ptr<FullTextSearch> full_text_search;
    PAIMON_RETURN_NOT_OK(PrepareSearch(split_read.context_, field_names, file->file_name,
                                       indexes.get(), &vector_search, &full_text_search));
    return std::unique_ptr<PreciseFileIndexRead>(new PreciseFileIndexRead(
        split_read, partition, file, field_mapping_builder, path_factory, extra_format_options,
        std::move(indexes), mapped_leaves, vector_search, full_text_search));
}

PreciseFileIndexRead::PreciseFileIndexRead(
    const RawFileSplitRead& split_read, const BinaryRow& partition,
    const std::shared_ptr<DataFileMeta>& file, const FieldMappingBuilder* field_mapping_builder,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::map<std::string, std::string>& extra_format_options,
    std::unique_ptr<FileIndexEvaluator> indexes,
    const std::map<std::shared_ptr<LeafPredicate>, std::shared_ptr<LeafPredicate>>& mapped_leaves,
    const std::shared_ptr<VectorSearch>& vector_search,
    const std::shared_ptr<FullTextSearch>& full_text_search)
    : split_read_(split_read),
      partition_(partition),
      file_(file),
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

Result<PreciseFileIndexRead::Evaluation> PreciseFileIndexRead::Evaluate(
    const std::shared_ptr<Predicate>& predicate, const RoaringBitmap32& candidates) const {
    if (!predicate || candidates.IsEmpty()) {
        return Evaluation{candidates, {}};
    }
    if (auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        return EvaluateLeaf(leaf, candidates);
    }
    if (auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        return EvaluateCompound(compound, candidates);
    }
    return Status::Invalid(fmt::format("unsupported file predicate {}", predicate->ToString()));
}

Result<PreciseFileIndexRead::Evaluation> PreciseFileIndexRead::EvaluateCompound(
    const std::shared_ptr<CompoundPredicate>& predicate, const RoaringBitmap32& candidates) const {
    const Function::Type type = predicate->GetFunction().GetType();
    if (type == Function::Type::OR) {
        Evaluation result;
        for (const auto& child : predicate->Children()) {
            // Every branch must see the same input, including rows matched by earlier branches.
            PAIMON_ASSIGN_OR_RAISE(Evaluation child_result, Evaluate(child, candidates));
            result.rows |= child_result.rows;
            result.scores.insert(child_result.scores.begin(), child_result.scores.end());
        }
        return result;
    }
    if (type != Function::Type::AND) {
        return Status::Invalid(
            fmt::format("unsupported compound file predicate {}", predicate->ToString()));
    }

    struct Child {
        std::shared_ptr<Predicate> predicate;
        std::optional<double> selectivity;
    };
    std::vector<Child> children;
    children.reserve(predicate->Children().size());
    for (const auto& child : predicate->Children()) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<double> selectivity, EstimateSelectivity(child));
        children.push_back({child, selectivity});
    }
    std::stable_sort(children.begin(), children.end(), [](const Child& lhs, const Child& rhs) {
        if (!lhs.selectivity) {
            return false;
        }
        return !rhs.selectivity || *lhs.selectivity < *rhs.selectivity;
    });

    Evaluation result{candidates, {}};
    for (const auto& child : children) {
        PAIMON_ASSIGN_OR_RAISE(Evaluation child_result, Evaluate(child.predicate, result.rows));
        result.rows = std::move(child_result.rows);
        result.scores.insert(child_result.scores.begin(), child_result.scores.end());
        if (result.rows.IsEmpty()) {
            break;
        }
    }
    return result;
}

Result<std::optional<double>> PreciseFileIndexRead::EstimateSelectivity(
    const std::shared_ptr<Predicate>& predicate) const {
    if (!predicate) {
        return std::optional<double>();
    }
    if (auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<double> estimate, EstimateLeafSelectivity(leaf));
        if (estimate && (!std::isfinite(*estimate) || *estimate < 0.0 || *estimate > 1.0)) {
            return std::optional<double>();
        }
        return estimate;
    }
    auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate);
    if (!compound) {
        return Status::Invalid(fmt::format("unsupported file predicate {}", predicate->ToString()));
    }
    const Function::Type type = compound->GetFunction().GetType();
    if (type != Function::Type::AND && type != Function::Type::OR) {
        return Status::Invalid(
            fmt::format("unsupported compound file predicate {}", predicate->ToString()));
    }
    std::optional<double> estimate =
        type == Function::Type::AND ? std::nullopt : std::make_optional(0.0);
    // These are scheduling hints. Continue visiting all children to propagate errors.
    for (const auto& child : compound->Children()) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<double> child_estimate, EstimateSelectivity(child));
        if (type == Function::Type::AND) {
            if (child_estimate) {
                if (estimate.has_value()) {
                    estimate = std::min(*estimate, *child_estimate);
                } else {
                    estimate = *child_estimate;
                }
            }
        } else if (estimate && child_estimate) {
            estimate = std::min(1.0, *estimate + *child_estimate);
        } else {
            estimate = std::nullopt;
        }
    }
    return estimate;
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
    bool is_full_text = predicate->GetFunction().GetType() == Function::Type::FULL_TEXT_MATCH;
    bool needs_scores =
        is_full_text &&
        split_read_.context_->NeedsScores(
            {InternalReadContext::IndexScoreSource::Type::FULL_TEXT_MATCH, predicate->FieldName()});
    std::shared_ptr<FileIndexResult> index_result = FileIndexResult::Remain();
    auto iter = mapped_leaves_.find(predicate);
    if (iter == mapped_leaves_.end()) {
        return Status::Invalid("File predicate mapping was not prepared");
    }
    if (iter->second) {
        // TODO(lisizhuo.lsz): Pass candidates through FileIndexEvaluator::EvaluateLeaf to index
        // readers so they can skip rows excluded by earlier predicates, row ranges, or deletion
        // vectors.
        PAIMON_ASSIGN_OR_RAISE(index_result, indexes_->EvaluateLeaf(iter->second, needs_scores));
    }
    if (!index_result) {
        return Status::Invalid("file predicate index returned a null result");
    }
    Evaluation result{candidates, {}};
    if (needs_scores) {
        auto scored = std::dynamic_pointer_cast<ScoredFileIndexResult>(index_result);
        if (!scored) {
            return Status::Invalid(
                fmt::format("full-text index for field '{}' did not return requested scores",
                            predicate->FieldName()));
        }
        result.rows &= scored->GetRowPositions();
        result.scores.emplace(predicate->FieldName(), std::move(scored));
        return result;
    }

    if (auto bitmap = std::dynamic_pointer_cast<BitmapIndexResult>(index_result)) {
        PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* rows, bitmap->GetBitmap());
        result.rows &= *rows;
    } else if (is_full_text) {
        return Status::Invalid(fmt::format("full-text index for field '{}' did not return a bitmap",
                                           predicate->FieldName()));
    } else if (std::dynamic_pointer_cast<Skip>(index_result)) {
        return Evaluation{};
    } else if (!std::dynamic_pointer_cast<Remain>(index_result)) {
        return Status::Invalid(
            fmt::format("invalid index result for predicate {}", predicate->ToString()));
    }
    // TODO(lisizhuo.lsz): Whether to ScanScalar depends on whether the index returns exact results.
    if (!is_full_text && !result.rows.IsEmpty()) {
        // Scalar indexes may contain false positives, even if they return a bitmap.
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 exact_rows, ScanScalar(predicate, result.rows));
        result.rows &= exact_rows;
    }
    return result;
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
    auto field = split_read_.context_->GetReadSchema()->GetFieldByName(predicate->FieldName());
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
    std::shared_ptr<ScoredFileIndexResult> root_scores;
    std::string field_name;
    if (vector_search_) {
        auto eligible = std::make_shared<RoaringBitmap32>(evaluated->rows);
        auto user_filter = vector_search_->pre_filter;
        auto search = vector_search_->ReplacePreFilter([eligible, user_filter](int64_t row) {
            return row >= 0 && row < RoaringBitmap32::MAX_VALUE &&
                   eligible->Contains(static_cast<int32_t>(row)) &&
                   (!user_filter || user_filter(row));
        });
        PAIMON_ASSIGN_OR_RAISE(root_scores, indexes_->SearchVector(search));
        field_name = split_read_.context_->GetVectorSearch()->field_name;
    } else if (full_text_search_) {
        RoaringBitmap64 eligible(evaluated->rows);
        if (full_text_search_->include_row_ids) {
            eligible &= *full_text_search_->include_row_ids;
        }
        auto search = std::make_shared<FullTextSearch>(*full_text_search_);
        search->include_row_ids = std::move(eligible);
        PAIMON_ASSIGN_OR_RAISE(root_scores, indexes_->SearchFullText(search));
        field_name = split_read_.context_->GetFullTextSearch()->field_name;
    } else {
        return Status::OK();
    }
    evaluated->rows &= root_scores->GetRowPositions();
    // Score bindings use the original field name, not the historical file schema's name.
    evaluated->scores.emplace(field_name, std::move(root_scores));
    return Status::OK();
}

Result<std::vector<float>> PreciseFileIndexRead::AlignScores(
    const std::shared_ptr<ScoredFileIndexResult>& source, const RoaringBitmap32& rows) {
    if (!source) {
        return Status::Invalid("Projected search source did not return scores");
    }
    std::vector<float> aligned;
    aligned.reserve(rows.Cardinality());
    size_t score = 0;
    for (auto iter = source->GetRowPositions().Begin(); iter != source->GetRowPositions().End();
         ++iter, ++score) {
        if (rows.Contains(*iter)) {
            aligned.push_back(source->GetScores()[score]);
        }
    }
    if (aligned.size() != static_cast<size_t>(rows.Cardinality())) {
        return Status::Invalid("Projected scores do not cover all selected file rows");
    }
    return aligned;
}

Result<std::map<std::string, std::vector<float>>> PreciseFileIndexRead::CreateOutputScores(
    const Evaluation& evaluated) const {
    std::map<std::string, std::vector<float>> output_scores;
    for (const auto& [position, source] : split_read_.context_->GetIndexScoreBindings()) {
        auto found = evaluated.scores.find(source.field_name);
        if (found == evaluated.scores.end()) {
            return Status::Invalid(fmt::format("Projected search field '{}' did not return scores",
                                               source.field_name));
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<float> aligned,
                               AlignScores(found->second, evaluated.rows));
        output_scores.emplace(split_read_.context_->GetReadSchema()->field(position)->name(),
                              std::move(aligned));
    }
    return output_scores;
}

Result<std::unique_ptr<FileBatchReader>> PreciseFileIndexRead::CreateReader(
    const DeletionVector::Factory& dv_factory,
    const std::optional<std::vector<Range>>& row_ranges) const {
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 candidates, CreateCandidates(dv_factory, row_ranges));
    PAIMON_ASSIGN_OR_RAISE(Evaluation evaluated,
                           Evaluate(split_read_.context_->GetPredicate(), candidates));
    if (evaluated.rows.IsEmpty()) {
        return std::unique_ptr<FileBatchReader>();
    }
    PAIMON_RETURN_NOT_OK(ApplyRootSearch(&evaluated));
    if (evaluated.rows.IsEmpty()) {
        return std::unique_ptr<FileBatchReader>();
    }
    std::map<std::string, std::vector<float>> output_scores;
    PAIMON_ASSIGN_OR_RAISE(output_scores, CreateOutputScores(evaluated));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> reader,
                           CreateSelectedFileReader(evaluated.rows, field_mapping_builder_));
    if (!output_scores.empty()) {
        return std::unique_ptr<FileBatchReader>(std::make_unique<CompleteIndexScoreFileBatchReader>(
            std::move(reader), output_scores, split_read_.context_->GetArrowMemoryPool()));
    }
    return reader;
}

}  // namespace paimon

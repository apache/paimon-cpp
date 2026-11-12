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

#include "paimon/core/realtime/precise_realtime_index_read.h"

#include <algorithm>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/index/index_reader_utils.h"
#include "paimon/common/reader/complete_row_kind_batch_reader.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/reader/index_score_batch_utils.h"
#include "paimon/common/reader/predicate_batch_reader.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/precise_index_evaluator.h"
#include "paimon/core/realtime/realtime_reader.h"
#include "paimon/core/realtime/realtime_schema_layout.h"
#include "paimon/core/realtime/realtime_store_read_pipeline.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/predicate/full_text_match.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/realtime/index/realtime_index_read_source.h"

namespace paimon {
namespace {

using Evaluator = PreciseIndexEvaluator<RoaringBitmap64, ScoredRealtimeIndexResult>;
using Evaluation = Evaluator::Evaluation;
// Outer map: output score column name (e.g. _embedding_INDEX_SCORE) -> scores by real-time offset.
// Inner map: real-time offset -> search score for that row.
using OutputScores = std::map<std::string, std::map<int64_t, float>>;

/// Completes virtual scores and restores projection while offsets are still available.
class RealtimeScoreBatchReader final : public BatchReader {
 public:
    RealtimeScoreBatchReader(std::unique_ptr<BatchReader> reader,
                             std::shared_ptr<arrow::Schema> output_schema, OutputScores scores,
                             const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : reader_(std::move(reader)),
          output_schema_(std::move(output_schema)),
          scores_(std::move(scores)),
          arrow_pool_(arrow_pool) {}

    Result<ReadBatch> NextBatch() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, NextBatchWithBitmap());
        return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch), arrow_pool_);
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, reader_->NextBatchWithBitmap());
        if (IsEofBatch(batch)) {
            return batch;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> imported,
            arrow::ImportArray(batch.first.first.get(), batch.first.second.get()));
        if (!imported || imported->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("Real-time score reader requires struct batches");
        }
        auto data = checked_pointer_cast<arrow::StructArray>(imported);
        auto offsets = std::dynamic_pointer_cast<arrow::Int64Array>(
            data->GetFieldByName(SpecialFields::RealtimeOffset().Name()));
        if (!offsets || offsets->null_count() != 0) {
            return Status::Invalid("Real-time score reader requires non-null offsets");
        }
        return IndexScoreBatchUtils::CompleteScores(
            std::move(batch), data, output_schema_, scores_,
            [&offsets](const std::string&, const std::map<int64_t, float>& scores, int64_t row,
                       size_t) -> Result<float> {
                auto found = scores.find(offsets->Value(row));
                if (found == scores.end()) {
                    return Status::Invalid(
                        "Search score is missing for a selected real-time offset");
                }
                return found->second;
            },
            arrow_pool_);
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        reader_->Close();
    }

 private:
    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<arrow::Schema> output_schema_;
    OutputScores scores_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

class RealtimeSourceRead {
 public:
    RealtimeSourceRead(const std::shared_ptr<InternalReadContext>& context,
                       const std::shared_ptr<RealtimeStore>& store,
                       const std::shared_ptr<RealtimeReadView>& view,
                       const std::shared_ptr<RealtimeIndexReadSource>& source,
                       const RealtimeSchemaLayout& layout)
        : context_(context), store_(store), view_(view), source_(source), layout_(layout) {}

    Status Prepare(const std::shared_ptr<Predicate>& predicate) {
        return Evaluator::Prepare(
            predicate, [this](const std::shared_ptr<LeafPredicate>& leaf) -> Status {
                PAIMON_ASSIGN_OR_RAISE(
                    [[maybe_unused]] std::vector<std::shared_ptr<RealtimeIndexReader>> readers,
                    GetReaders(leaf->FieldName()));
                if (leaf->GetFunction().GetType() == Function::Type::FULL_TEXT_MATCH) {
                    PAIMON_ASSIGN_OR_RAISE(
                        [[maybe_unused]] std::shared_ptr<RealtimeIndexReader> reader,
                        GetSearchReader(leaf->FieldName(),
                                        RealtimeIndexReader::SearchType::FULL_TEXT_MATCH));
                }
                return Status::OK();
            });
    }

    Status PrepareSearch() {
        if (context_->GetVectorSearch()) {
            PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] std::shared_ptr<RealtimeIndexReader> reader,
                                   GetSearchReader(context_->GetVectorSearch()->field_name,
                                                   RealtimeIndexReader::SearchType::VECTOR_SEARCH));
        }
        if (context_->GetFullTextSearch()) {
            PAIMON_ASSIGN_OR_RAISE(
                [[maybe_unused]] std::shared_ptr<RealtimeIndexReader> reader,
                GetSearchReader(context_->GetFullTextSearch()->field_name,
                                RealtimeIndexReader::SearchType::FULL_TEXT_SEARCH));
        }
        return Status::OK();
    }

    Result<std::unique_ptr<BatchReader>> CreateReader() {
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 candidates, source_->CreateCandidates());
        Evaluator evaluator(
            [this](const std::shared_ptr<LeafPredicate>& leaf, const RoaringBitmap64& rows) {
                return EvaluateLeaf(leaf, rows);
            },
            [this](const std::shared_ptr<LeafPredicate>& leaf) {
                return EstimateSelectivity(leaf);
            });
        PAIMON_ASSIGN_OR_RAISE(Evaluation evaluated,
                               evaluator.Evaluate(context_->GetPredicate(), candidates));
        if (evaluated.rows.IsEmpty()) {
            return std::unique_ptr<BatchReader>();
        }
        PAIMON_RETURN_NOT_OK(ApplyRootSearch(&evaluated));
        if (evaluated.rows.IsEmpty()) {
            return std::unique_ptr<BatchReader>();
        }

        using AlignedScores = std::map<std::string, std::vector<float>>;
        PAIMON_ASSIGN_OR_RAISE(
            AlignedScores aligned_scores,
            Evaluator::CreateOutputScores(evaluated, context_->GetIndexScoreBindings(),
                                          context_->GetReadSchema()));
        OutputScores scores;
        for (const auto& [field, aligned] : aligned_scores) {
            auto& output = scores[field];
            size_t index = 0;
            for (auto iter = evaluated.rows.Begin(); iter != evaluated.rows.End(); ++iter) {
                output.emplace(*iter, aligned[index++]);
            }
        }
        arrow::FieldVector fields;
        for (const auto& field : context_->GetReadSchema()->fields()) {
            if (!SpecialFields::ParseIndexScoreFieldName(field->name())) {
                fields.push_back(field);
            }
        }
        if (context_->GetReadSchema()->GetFieldIndex(OffsetField()->name()) < 0) {
            fields.push_back(OffsetField());
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                               ReadLogical(arrow::schema(fields), /*predicate=*/nullptr,
                                           std::make_shared<RoaringBitmap64>(evaluated.rows)));
        return std::unique_ptr<BatchReader>(std::make_unique<RealtimeScoreBatchReader>(
            std::move(reader), context_->GetReadSchema(), std::move(scores),
            context_->GetArrowMemoryPool()));
    }

    Result<std::vector<std::shared_ptr<RealtimeIndexReader>>> GetReaders(
        const std::string& field_name) {
        auto found = readers_.find(field_name);
        if (found == readers_.end()) {
            PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<RealtimeIndexReader>> readers,
                                   source_->CreateIndexReaders(field_name));
            for (const auto& reader : readers) {
                if (!reader) {
                    return Status::Invalid("Real-time source returned a null index reader");
                }
            }
            found = readers_.emplace(field_name, std::move(readers)).first;
        }
        return found->second;
    }

    Result<std::shared_ptr<RealtimeIndexReader>> GetSearchReader(
        const std::string& field_name, RealtimeIndexReader::SearchType type) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<RealtimeIndexReader>> readers,
                               GetReaders(field_name));
        return IndexReaderUtils::GetSearchReader(
            readers, type,
            fmt::format("Multiple real-time indexes support search on: {}", field_name),
            fmt::format("No real-time index supports requested search on: {}", field_name));
    }

 private:
    std::shared_ptr<arrow::Field> OffsetField() const {
        return layout_.StoreWriteSchema()->GetFieldByName(SpecialFields::RealtimeOffset().Name());
    }

    Result<std::optional<double>> EstimateSelectivity(const std::shared_ptr<LeafPredicate>& leaf) {
        return IndexReaderUtils::EstimateSelectivity<RealtimeIndexReader>(leaf, this);
    }

    Result<Evaluation> EvaluateLeaf(const std::shared_ptr<LeafPredicate>& leaf,
                                    const RoaringBitmap64& candidates) {
        bool needs_scores = context_->NeedsScores(
            {InternalReadContext::IndexScoreSource::Type::FULL_TEXT_MATCH, leaf->FieldName()});
        return Evaluator::EvaluateLeaf<RealtimeIndexReader>(
            leaf, leaf, candidates, needs_scores,
            [this](const std::shared_ptr<LeafPredicate>& predicate, const RoaringBitmap64& rows) {
                return ScanScalar(predicate, rows);
            },
            this);
    }

    Result<RoaringBitmap64> ScanScalar(const std::shared_ptr<LeafPredicate>& leaf,
                                       const RoaringBitmap64& candidates) const {
        auto field = context_->GetReadSchema()->GetFieldByName(leaf->FieldName());
        if (!field) {
            return Status::Invalid("Missing real-time scalar field: ", leaf->FieldName());
        }
        arrow::FieldVector fields{field};
        if (field->name() != OffsetField()->name()) {
            fields.push_back(OffsetField());
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                               ReadLogical(arrow::schema(fields), leaf,
                                           std::make_shared<RoaringBitmap64>(candidates)));
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<PredicateBatchReader> filtered,
            PredicateBatchReader::Create(std::move(reader), leaf, context_->GetArrowMemoryPool()));
        return CollectOffsets(std::move(filtered));
    }

    Status ApplyRootSearch(Evaluation* evaluated) {
        const auto& vector = context_->GetVectorSearch();
        const auto& text = context_->GetFullTextSearch();
        const std::string field = vector ? vector->field_name : text ? text->field_name : "";
        return Evaluator::ApplyRootSearch<RealtimeIndexReader>(vector, text, field, this,
                                                               evaluated);
    }

    Result<std::unique_ptr<BatchReader>> ReadLogical(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::shared_ptr<LeafPredicate>& predicate,
        const std::shared_ptr<const RoaringBitmap64>& selection) const {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<RealtimeStoreReadPipeline> pipeline,
            RealtimeStoreReadPipeline::Create(schema, layout_, context_->GetMemoryPool(),
                                              context_->GetArrowMemoryPool()));
        ArrowSchema c_schema;
        ArrowSchemaMarkReleased(&c_schema);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportSchema(*pipeline->StoreReadSchema(), &c_schema));
        ScopeGuard schema_guard([&]() { ArrowSchemaRelease(&c_schema); });
        std::shared_ptr<Predicate> store_predicate;
        if (predicate) {
            const auto& field_name = predicate->FieldName();
            auto logical_field = schema->GetFieldByName(field_name);
            auto physical_field = pipeline->StoreReadSchema()->GetFieldByName(field_name);
            auto stored_field = layout_.StoreWriteSchema()->GetFieldByName(field_name);
            // Conversion plans may change the value representation. Only push predicates on
            // fields whose logical, requested physical and stored types agree.
            if (logical_field && physical_field && stored_field &&
                logical_field->type()->Equals(physical_field->type()) &&
                logical_field->type()->Equals(stored_field->type())) {
                PAIMON_ASSIGN_OR_RAISE(
                    store_predicate,
                    PredicateUtils::CreatePickedFieldFilter(
                        predicate,
                        {{field_name, pipeline->StoreReadSchema()->GetFieldIndex(field_name)}}));
            }
        }
        RealtimeQueryContext query{&c_schema,
                                   store_predicate,
                                   source_->GetOffsetRange(),
                                   context_->GetCoreOptions().GetReadBatchSize(),
                                   selection,
                                   /*offset_deletions=*/nullptr,
                                   /*enable_index_pruning=*/false};
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> readers,
                               store_->CreateQueryReaders(view_, query));
        ScopeGuard close([&]() {
            for (const auto& reader : readers) {
                if (reader) {
                    reader->Close();
                }
            }
        });
        for (auto& reader : readers) {
            if (!reader) {
                return Status::Invalid("Real-time index source returned a null data reader");
            }
            PAIMON_ASSIGN_OR_RAISE(reader,
                                   pipeline->Wrap(std::move(reader), source_->GetOffsetRange()));
        }
        std::unique_ptr<BatchReader> result =
            std::make_unique<ConcatBatchReader>(std::move(readers), context_->GetArrowMemoryPool());
        close.Release();
        return result;
    }

    Result<RoaringBitmap64> CollectOffsets(std::unique_ptr<BatchReader> reader) const {
        ScopeGuard close([&]() { reader->Close(); });
        RoaringBitmap64 offsets;
        const OffsetRange range = source_->GetOffsetRange();
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch,
                                   reader->NextBatchWithBitmap());
            if (BatchReader::IsEofBatch(batch)) {
                break;
            }
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> imported,
                arrow::ImportArray(batch.first.first.get(), batch.first.second.get()));
            if (!imported || imported->type_id() != arrow::Type::STRUCT) {
                return Status::Invalid("Real-time offset collection requires struct batches");
            }
            auto data = checked_pointer_cast<arrow::StructArray>(imported);
            auto values = std::dynamic_pointer_cast<arrow::Int64Array>(
                data->GetFieldByName(OffsetField()->name()));
            if (!values || values->null_count() != 0) {
                return Status::Invalid("Real-time offset collection requires non-null offsets");
            }
            for (auto iter = batch.second.Begin(); iter != batch.second.End(); ++iter) {
                if (*iter < 0 || *iter >= values->length()) {
                    return Status::Invalid("Invalid real-time batch row position");
                }
                const int64_t offset = values->Value(*iter);
                if (offset < range.begin || offset >= range.end) {
                    return Status::Invalid(
                        "Real-time source returned an offset outside its bounds");
                }
                offsets.Add(offset);
            }
        }
        return offsets;
    }

    std::shared_ptr<InternalReadContext> context_;
    std::shared_ptr<RealtimeStore> store_;
    std::shared_ptr<RealtimeReadView> view_;
    std::shared_ptr<RealtimeIndexReadSource> source_;
    const RealtimeSchemaLayout& layout_;
    std::map<std::string, std::vector<std::shared_ptr<RealtimeIndexReader>>> readers_;
};

}  // namespace

Result<std::unique_ptr<BatchReader>> PreciseRealtimeIndexRead::CreateReader(
    const std::shared_ptr<InternalReadContext>& context, RealtimeStoreMode mode,
    const std::shared_ptr<RealtimeStore>& store, const std::shared_ptr<RealtimeReadView>& view,
    const OffsetRange& offset_range) {
    if (mode == RealtimeStoreMode::PRIMARY_KEY) {
        return Status::NotImplemented(
            "Precise real-time search does not support primary-key merge");
    }
    if (!store) {
        return Status::Invalid("Precise real-time read requires a store");
    }
    auto indexed = std::dynamic_pointer_cast<RealtimeIndexedReadView>(view);
    if (!indexed) {
        return Status::NotImplemented("Real-time store has no precise index read capability");
    }
    auto table_schema =
        DataField::ConvertDataFieldsToArrowSchema(context->GetTableSchema()->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RealtimeSchemaLayout> layout,
                           RealtimeSchemaLayout::Create(mode, table_schema));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<RealtimeIndexReadSource>> sources,
                           indexed->CreateIndexReadSources(offset_range));
    std::vector<std::unique_ptr<RealtimeSourceRead>> plans;
    std::vector<OffsetRange> source_ranges;
    for (const auto& source : sources) {
        if (!source) {
            return Status::Invalid("Real-time view returned a null index source");
        }
        const OffsetRange range = source->GetOffsetRange();
        if (range.begin < offset_range.begin || range.end > offset_range.end ||
            range.begin >= range.end) {
            return Status::Invalid("Real-time index source has invalid offset bounds");
        }
        source_ranges.push_back(range);
        auto plan = std::make_unique<RealtimeSourceRead>(context, store, view, source, *layout);
        PAIMON_RETURN_NOT_OK(plan->Prepare(context->GetPredicate()));
        PAIMON_RETURN_NOT_OK(plan->PrepareSearch());
        plans.push_back(std::move(plan));
    }
    std::sort(
        source_ranges.begin(), source_ranges.end(),
        [](const OffsetRange& left, const OffsetRange& right) { return left.begin < right.begin; });
    for (size_t i = 1; i < source_ranges.size(); ++i) {
        if (source_ranges[i - 1].end > source_ranges[i].begin) {
            return Status::Invalid("Real-time index sources have overlapping offset bounds");
        }
    }
    std::vector<std::unique_ptr<BatchReader>> readers;
    ScopeGuard close([&]() {
        for (const auto& reader : readers) {
            reader->Close();
        }
    });
    for (const auto& plan : plans) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, plan->CreateReader());
        if (reader) {
            readers.push_back(std::move(reader));
        }
    }
    std::unique_ptr<BatchReader> result =
        std::make_unique<ConcatBatchReader>(std::move(readers), context->GetArrowMemoryPool());
    close.Release();
    result = std::make_unique<CompleteRowKindBatchReader>(std::move(result),
                                                          context->GetArrowMemoryPool());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RealtimeReader> pinned,
                           RealtimeReader::Create(view, std::move(result)));
    return std::unique_ptr<BatchReader>(std::move(pinned));
}

}  // namespace paimon

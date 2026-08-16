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

#include "paimon/core/table/source/primary_key_sorted_index_scan.h"

#include <cassert>
#include <set>
#include <unordered_map>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/global_index_evaluator_impl.h"
#include "paimon/core/index/pk/primary_key_index_definitions.h"
#include "paimon/core/index/pksorted/pk_sorted_bucket_index_state.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/predicate/predicate_utils.h"

namespace paimon {
namespace {
using BucketKey = std::pair<BinaryRow, int32_t>;

struct QueryKey {
    Function::Type operation;
    std::vector<Literal> literals;

    bool operator==(const QueryKey& other) const {
        return operation == other.operation && literals == other.literals;
    }
};

/// Shares one group payload reader and its group-scope query results across all source
/// files of the group; localizes group ordinals to file-local physical positions using the
/// ordered source row-count prefix.
class SharedGroupReader {
 public:
    using UnderlyingReaderFactory = std::function<Result<std::shared_ptr<GlobalIndexReader>>()>;

    SharedGroupReader(const std::shared_ptr<PkSortedIndexGroup>& group,
                      UnderlyingReaderFactory reader_factory)
        : group_(group), reader_factory_(std::move(reader_factory)) {
        const std::vector<PrimaryKeyIndexSourceFile>& source_files = group->SourceFiles();
        source_offsets_.reserve(source_files.size() + 1);
        source_offsets_.push_back(0);
        for (const PrimaryKeyIndexSourceFile& source_file : source_files) {
            source_offsets_.push_back(source_offsets_.back() + source_file.row_count);
        }
    }

    const std::shared_ptr<PkSortedIndexGroup>& Group() const {
        return group_;
    }

    /// Runs one group-scope query with caching; equal queries evaluate exactly once.
    Result<std::shared_ptr<GlobalIndexResult>> Query(
        const QueryKey& key,
        const std::function<Result<std::shared_ptr<GlobalIndexResult>>(GlobalIndexReader*)>&
            query) {
        for (const auto& cached : query_cache_) {
            if (cached.first == key) {
                if (!cached.second.status.ok()) {
                    return cached.second.status;
                }
                return cached.second.result;
            }
        }
        Result<std::shared_ptr<GlobalIndexResult>> result = RunQuery(query);
        CachedQuery cached_query;
        if (result.ok()) {
            cached_query.result = result.value();
        } else {
            cached_query.status = result.status();
        }
        query_cache_.emplace_back(key, cached_query);
        return result;
    }

    /// Restricts one group-scope result to the local row positions of `source_index`.
    /// Any out-of-range group ordinal fails the localization so that every covered file
    /// of this group falls back to a normal scan; a poison marker would not survive the
    /// AND/OR combination of results from other indexes.
    Result<std::shared_ptr<GlobalIndexResult>> Localize(
        const std::shared_ptr<GlobalIndexResult>& result, size_t source_index) {
        if (result == nullptr) {
            return std::shared_ptr<GlobalIndexResult>(nullptr);
        }
        assert(source_index + 1 < source_offsets_.size());
        auto localized = localized_cache_.find(result.get());
        if (localized == localized_cache_.end()) {
            PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<GlobalIndexResult>> partitions,
                                   PartitionBySource(result));
            localized = localized_cache_.emplace(result.get(), std::move(partitions)).first;
        }
        return localized->second[source_index];
    }

 private:
    struct CachedQuery {
        Status status;
        std::shared_ptr<GlobalIndexResult> result;
    };

    Result<std::shared_ptr<GlobalIndexResult>> RunQuery(
        const std::function<Result<std::shared_ptr<GlobalIndexResult>>(GlobalIndexReader*)>&
            query) {
        if (!reader_status_.ok()) {
            return reader_status_;
        }
        if (reader_ == nullptr) {
            Result<std::shared_ptr<GlobalIndexReader>> reader_result = reader_factory_();
            if (!reader_result.ok()) {
                reader_status_ = reader_result.status();
                return reader_status_;
            }
            reader_ = reader_result.value();
            if (reader_ == nullptr) {
                // The index type has no usable reader; keep normal scan semantics.
                return std::shared_ptr<GlobalIndexResult>(nullptr);
            }
        }
        return query(reader_.get());
    }

    Result<std::vector<std::shared_ptr<GlobalIndexResult>>> PartitionBySource(
        const std::shared_ptr<GlobalIndexResult>& result) {
        size_t source_count = source_offsets_.size() - 1;
        std::vector<RoaringBitmap64> partitions(source_count);
        int64_t total_row_count = source_offsets_.back();
        size_t source_index = 0;
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexResult::Iterator> iterator,
                               result->CreateIterator());
        while (iterator->HasNext()) {
            int64_t position = iterator->Next();
            if (position < 0 || position >= total_row_count) {
                return Status::Invalid(fmt::format(
                    "Sorted index returned group ordinal {} outside the source row range "
                    "[0, {}).",
                    position, total_row_count));
            }
            while (position >= source_offsets_[source_index + 1]) {
                source_index++;
            }
            partitions[source_index].Add(position - source_offsets_[source_index]);
        }
        std::vector<std::shared_ptr<GlobalIndexResult>> localized;
        localized.reserve(source_count);
        for (RoaringBitmap64& partition : partitions) {
            auto bitmap = std::make_shared<RoaringBitmap64>(std::move(partition));
            localized.push_back(std::make_shared<BitmapGlobalIndexResult>(
                [bitmap]() -> Result<RoaringBitmap64> { return *bitmap; }));
        }
        return localized;
    }

    std::shared_ptr<PkSortedIndexGroup> group_;
    UnderlyingReaderFactory reader_factory_;
    std::vector<int64_t> source_offsets_;
    std::vector<std::pair<QueryKey, CachedQuery>> query_cache_;
    std::unordered_map<const GlobalIndexResult*, std::vector<std::shared_ptr<GlobalIndexResult>>>
        localized_cache_;
    std::shared_ptr<GlobalIndexReader> reader_;
    Status reader_status_;
};

/// Restricts merged source-group ordinals to one source file's local row positions.
class FileLocalGroupReader : public GlobalIndexReader {
 public:
    FileLocalGroupReader(std::shared_ptr<SharedGroupReader> shared_reader, size_t source_index)
        : shared_reader_(std::move(shared_reader)), source_index_(source_index) {}

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override {
        return Query({Function::Type::IS_NOT_NULL, {}},
                     [](GlobalIndexReader* reader) { return reader->VisitIsNotNull(); });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override {
        return Query({Function::Type::IS_NULL, {}},
                     [](GlobalIndexReader* reader) { return reader->VisitIsNull(); });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override {
        return Query({Function::Type::EQUAL, {literal}},
                     [&literal](GlobalIndexReader* reader) { return reader->VisitEqual(literal); });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override {
        return Query({Function::Type::NOT_EQUAL, {literal}}, [&literal](GlobalIndexReader* reader) {
            return reader->VisitNotEqual(literal);
        });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override {
        return Query({Function::Type::LESS_THAN, {literal}}, [&literal](GlobalIndexReader* reader) {
            return reader->VisitLessThan(literal);
        });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override {
        return Query(
            {Function::Type::LESS_OR_EQUAL, {literal}},
            [&literal](GlobalIndexReader* reader) { return reader->VisitLessOrEqual(literal); });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override {
        return Query(
            {Function::Type::GREATER_THAN, {literal}},
            [&literal](GlobalIndexReader* reader) { return reader->VisitGreaterThan(literal); });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(
        const Literal& literal) override {
        return Query(
            {Function::Type::GREATER_OR_EQUAL, {literal}},
            [&literal](GlobalIndexReader* reader) { return reader->VisitGreaterOrEqual(literal); });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override {
        return Query({Function::Type::IN, literals},
                     [&literals](GlobalIndexReader* reader) { return reader->VisitIn(literals); });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override {
        return Query({Function::Type::NOT_IN, literals}, [&literals](GlobalIndexReader* reader) {
            return reader->VisitNotIn(literals);
        });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override {
        return Query({Function::Type::STARTS_WITH, {prefix}}, [&prefix](GlobalIndexReader* reader) {
            return reader->VisitStartsWith(prefix);
        });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override {
        return Query({Function::Type::ENDS_WITH, {suffix}}, [&suffix](GlobalIndexReader* reader) {
            return reader->VisitEndsWith(suffix);
        });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override {
        return Query({Function::Type::CONTAINS, {literal}}, [&literal](GlobalIndexReader* reader) {
            return reader->VisitContains(literal);
        });
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override {
        return Query({Function::Type::LIKE, {literal}},
                     [&literal](GlobalIndexReader* reader) { return reader->VisitLike(literal); });
    }

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override {
        return Status::Invalid("Primary-key sorted index does not support vector search.");
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override {
        return Status::Invalid("Primary-key sorted index does not support full text search.");
    }

    bool IsThreadSafe() const override {
        return false;
    }

    std::string GetIndexType() const override {
        return shared_reader_->Group()->Payload()->IndexType();
    }

 private:
    Result<std::shared_ptr<GlobalIndexResult>> Query(
        QueryKey key,
        const std::function<Result<std::shared_ptr<GlobalIndexResult>>(GlobalIndexReader*)>&
            query) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> group_result,
                               shared_reader_->Query(key, query));
        return shared_reader_->Localize(group_result, source_index_);
    }

    std::shared_ptr<SharedGroupReader> shared_reader_;
    size_t source_index_;
};

Result<size_t> FindSourceIndex(const PkSortedIndexGroup& group, const DataFileMeta& data_file) {
    const std::vector<PrimaryKeyIndexSourceFile>& source_files = group.SourceFiles();
    for (size_t i = 0; i < source_files.size(); i++) {
        if (source_files[i].file_name == data_file.file_name &&
            source_files[i].row_count == data_file.row_count) {
            return i;
        }
    }
    return Status::Invalid(fmt::format(
        "Data file {} is not covered by its sorted-index source group.", data_file.file_name));
}
}  // namespace

Result<PrimaryKeySortedIndexScan::Plan> PrimaryKeySortedIndexScan::CreatePlan(
    int64_t snapshot_id, const std::vector<std::shared_ptr<DataSplitImpl>>& data_splits,
    const std::vector<PrimaryKeyIndexDefinition>& definitions,
    const std::vector<IndexManifestEntry>& index_entries) {
    std::unordered_map<BucketKey, std::vector<std::shared_ptr<IndexFileMeta>>> payloads_by_bucket;
    for (const IndexManifestEntry& entry : index_entries) {
        const std::shared_ptr<IndexFileMeta>& payload = entry.index_file;
        if (payload == nullptr || !(entry.kind == FileKind::Add())) {
            continue;
        }
        const std::optional<GlobalIndexMeta>& meta = payload->GetGlobalIndexMeta();
        if (meta == std::nullopt || meta.value().source_meta == nullptr) {
            continue;
        }
        payloads_by_bucket[BucketKey(entry.partition, entry.bucket)].push_back(payload);
    }

    std::vector<PrimaryKeyIndexDefinition> scalar_definitions =
        PrimaryKeyIndexDefinitions::ScalarDefinitions(definitions);

    std::unordered_map<BucketKey, std::vector<std::shared_ptr<DataFileMeta>>> data_files_by_bucket;
    for (const std::shared_ptr<DataSplitImpl>& split : data_splits) {
        if (split == nullptr) {
            return Status::Invalid("Primary-key sorted-index scan received a null data split.");
        }
        if (split->SnapshotId() != snapshot_id) {
            return Status::Invalid(
                fmt::format("Data split snapshot {} does not match sorted-index scan snapshot {}.",
                            split->SnapshotId(), snapshot_id));
        }
        if (split->IsStreaming()) {
            return Status::Invalid("Primary-key sorted-index scan requires batch splits.");
        }
        if (!split->DeletionFiles().empty() &&
            split->DeletionFiles().size() != split->DataFiles().size()) {
            return Status::Invalid(
                "Deletion files must align with data files in a sorted-index split.");
        }
        std::vector<std::shared_ptr<DataFileMeta>>& data_files =
            data_files_by_bucket[BucketKey(split->Partition(), split->Bucket())];
        data_files.insert(data_files.end(), split->DataFiles().begin(), split->DataFiles().end());
    }

    // file name -> field id -> validated group, per bucket.
    std::unordered_map<
        BucketKey, std::map<std::string, std::map<int32_t, std::shared_ptr<PkSortedIndexGroup>>>>
        groups_by_bucket;
    for (const auto& bucket_entry : data_files_by_bucket) {
        const BucketKey& bucket = bucket_entry.first;
        std::vector<std::shared_ptr<IndexFileMeta>> bucket_payloads;
        auto payloads_iter = payloads_by_bucket.find(bucket);
        if (payloads_iter != payloads_by_bucket.end()) {
            bucket_payloads = payloads_iter->second;
        }
        std::set<std::pair<std::string, int64_t>> active_source_files;
        for (const std::shared_ptr<DataFileMeta>& data_file : bucket_entry.second) {
            active_source_files.emplace(data_file->file_name, data_file->row_count);
        }
        std::map<std::string, std::map<int32_t, std::shared_ptr<PkSortedIndexGroup>>>
            groups_by_source;
        for (const PrimaryKeyIndexDefinition& definition : scalar_definitions) {
            std::vector<std::shared_ptr<IndexFileMeta>> definition_payloads;
            for (const std::shared_ptr<IndexFileMeta>& payload : bucket_payloads) {
                const std::optional<GlobalIndexMeta>& meta = payload->GetGlobalIndexMeta();
                if (meta != std::nullopt && definition.IndexType() == payload->IndexType() &&
                    definition.FieldId() == meta.value().index_field_id) {
                    definition_payloads.push_back(payload);
                }
            }
            PkSortedBucketIndexState state = PkSortedBucketIndexState::FromActiveDataFiles(
                definition.FieldId(), definition.IndexType(), bucket_entry.second,
                definition_payloads);
            for (const std::shared_ptr<PkSortedIndexGroup>& group : state.Groups()) {
                for (const PrimaryKeyIndexSourceFile& source_file : group->SourceFiles()) {
                    if (active_source_files.count({source_file.file_name, source_file.row_count}) ==
                        0) {
                        continue;
                    }
                    groups_by_source[source_file.file_name][definition.FieldId()] = group;
                }
            }
        }
        groups_by_bucket[bucket] = std::move(groups_by_source);
    }

    std::vector<FilePlan> files;
    for (const std::shared_ptr<DataSplitImpl>& split : data_splits) {
        auto bucket_groups = groups_by_bucket.find(BucketKey(split->Partition(), split->Bucket()));
        for (size_t file_index = 0; file_index < split->DataFiles().size(); file_index++) {
            const std::shared_ptr<DataFileMeta>& data_file = split->DataFiles()[file_index];
            std::map<int32_t, std::shared_ptr<PkSortedIndexGroup>> groups;
            if (bucket_groups != groups_by_bucket.end() && data_file != nullptr) {
                auto source_groups = bucket_groups->second.find(data_file->file_name);
                if (source_groups != bucket_groups->second.end()) {
                    groups = source_groups->second;
                }
            }
            files.emplace_back(split, static_cast<int32_t>(file_index), std::move(groups));
        }
    }
    return Plan(snapshot_id, std::move(files));
}

Result<PrimaryKeySortedIndexScan::EvaluatedPlan> PrimaryKeySortedIndexScan::Evaluate(
    const Plan& plan, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<Predicate>& predicate,
    const std::vector<PrimaryKeyIndexDefinition>& definitions,
    const ReaderFactory& reader_factory) {
    std::map<int32_t, PrimaryKeyIndexDefinition> definitions_by_field;
    for (const PrimaryKeyIndexDefinition& definition :
         PrimaryKeyIndexDefinitions::ScalarDefinitions(definitions)) {
        definitions_by_field.emplace(definition.FieldId(), definition);
    }

    std::unordered_map<const PkSortedIndexGroup*, std::shared_ptr<SharedGroupReader>>
        shared_readers;
    std::vector<EvaluatedFile> files;
    files.reserve(plan.Files().size());
    for (const FilePlan& file : plan.Files()) {
        GlobalIndexEvaluatorImpl::IndexReadersCreator create_readers =
            [&file, &definitions_by_field, &shared_readers, &reader_factory](
                int32_t field_id) -> Result<std::vector<std::shared_ptr<GlobalIndexReader>>> {
            auto definition_iter = definitions_by_field.find(field_id);
            std::shared_ptr<PkSortedIndexGroup> group = file.Group(field_id);
            if (definition_iter == definitions_by_field.end() || group == nullptr) {
                return std::vector<std::shared_ptr<GlobalIndexReader>>();
            }
            auto shared_iter = shared_readers.find(group.get());
            if (shared_iter == shared_readers.end()) {
                const PrimaryKeyIndexDefinition& definition = definition_iter->second;
                // The shared reader outlives this file plan, so the factory owns a copy of
                // the file plan instead of referencing the loop variable.
                SharedGroupReader::UnderlyingReaderFactory underlying_factory =
                    [file_copy = file, definition, group,
                     &reader_factory]() -> Result<std::shared_ptr<GlobalIndexReader>> {
                    return reader_factory(file_copy, definition, *group);
                };
                shared_iter = shared_readers
                                  .emplace(group.get(), std::make_shared<SharedGroupReader>(
                                                            group, std::move(underlying_factory)))
                                  .first;
            }
            PAIMON_ASSIGN_OR_RAISE(size_t source_index, FindSourceIndex(*group, *file.DataFile()));
            std::vector<std::shared_ptr<GlobalIndexReader>> readers;
            readers.push_back(
                std::make_shared<FileLocalGroupReader>(shared_iter->second, source_index));
            return readers;
        };
        GlobalIndexEvaluatorImpl evaluator(table_schema, create_readers);
        Result<std::shared_ptr<GlobalIndexResult>> result = evaluator.Evaluate(predicate);
        if (result.ok()) {
            files.emplace_back(file, result.value());
        } else {
            // Evaluation failures degrade to a normal scan for this file only.
            files.emplace_back(file, nullptr);
        }
    }
    return EvaluatedPlan(plan.SnapshotId(), std::move(files));
}

namespace {
class FsGlobalIndexFileReader : public GlobalIndexFileReader {
 public:
    explicit FsGlobalIndexFileReader(std::shared_ptr<FileSystem> file_system)
        : file_system_(std::move(file_system)) {}

    Result<std::unique_ptr<InputStream>> GetInputStream(
        const std::string& file_path) const override {
        return file_system_->Open(file_path);
    }

 private:
    std::shared_ptr<FileSystem> file_system_;
};
}  // namespace

PrimaryKeySortedIndexScan::ReaderFactory PrimaryKeySortedIndexScan::MakeReaderFactory(
    const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<IndexFilePathFactories>& path_factories,
    const std::shared_ptr<TableSchema>& table_schema, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<Executor>& executor) {
    auto file_reader = std::make_shared<FsGlobalIndexFileReader>(file_system);
    return [path_factories, table_schema, pool, file_reader, executor](
               const FilePlan& file, const PrimaryKeyIndexDefinition& definition,
               const PkSortedIndexGroup& group) -> Result<std::shared_ptr<GlobalIndexReader>> {
        if (definition.GetFamily() != PrimaryKeyIndexDefinition::Family::BTREE) {
            // Only the BTree payload reader is wired up; other families keep normal scan
            // semantics until their dedicated readers are supported.
            return std::shared_ptr<GlobalIndexReader>(nullptr);
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<GlobalIndexer> indexer,
            GlobalIndexerFactory::Get(definition.IndexType(), definition.Options()));
        if (indexer == nullptr) {
            return std::shared_ptr<GlobalIndexReader>(nullptr);
        }
        const std::shared_ptr<DataSplitImpl>& split = file.SourceSplit();
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexPathFactory> path_factory,
                               path_factories->Get(split->Partition(), split->Bucket()));
        const std::shared_ptr<IndexFileMeta>& payload = group.Payload();
        const std::optional<GlobalIndexMeta>& payload_meta = payload->GetGlobalIndexMeta();
        if (payload_meta == std::nullopt) {
            // Group validation guarantees the metadata; degrade to a normal scan of the
            // covered files if it is ever violated, like the Java reader factory.
            return Status::Invalid(fmt::format(
                "Sorted index payload {} has no global index metadata.", payload->FileName()));
        }
        std::vector<GlobalIndexIOMeta> io_metas;
        io_metas.emplace_back(path_factory->ToPath(payload), payload->FileSize(),
                              payload_meta.value().index_meta);
        PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(definition.FieldId()));
        auto arrow_field = DataField::ConvertDataFieldToArrowField(field);
        auto arrow_schema = arrow::schema({arrow_field});
        ArrowSchema c_arrow_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
        ScopeGuard guard([&]() { ArrowSchemaRelease(&c_arrow_schema); });
        return indexer->CreateReader(&c_arrow_schema, file_reader, io_metas, pool, executor);
    };
}

}  // namespace paimon

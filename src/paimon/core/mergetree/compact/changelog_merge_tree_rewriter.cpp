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

#include "paimon/core/mergetree/compact/changelog_merge_tree_rewriter.h"

#include <utility>

#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/key_value_meta_projection_consumer.h"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/format/file_format.h"
namespace paimon {

namespace {

using SortMergeReaderFactory =
    std::function<Result<std::unique_ptr<SortMergeReader>>(const std::vector<SortedRun>& section)>;
using BoundChangelogMergeFunctionWrapperFactory =
    std::function<Result<std::shared_ptr<MergeFunctionWrapper<ChangelogResult>>>()>;
using KeyComparator = std::function<int32_t(const InternalRow&, const InternalRow&)>;
using CancellationChecker = std::function<bool()>;

class ChangelogCompactionBatchProducer : public AsyncKeyValueBatchProducer {
 public:
    ChangelogCompactionBatchProducer(
        const std::vector<std::vector<SortedRun>>& sections,
        std::vector<std::unique_ptr<SortMergeReader>>& reader_holders, int32_t write_batch_size,
        SortMergeReaderFactory reader_factory,
        BoundChangelogMergeFunctionWrapperFactory merge_function_wrapper_factory,
        KeyComparator key_comparator, CancellationChecker cancellation_checker, bool drop_delete,
        bool produce_data, bool produce_changelog)
        : sections_(sections),
          reader_holders_(reader_holders),
          write_batch_size_(NormalizeProjectionBatchSize(write_batch_size)),
          reader_factory_(std::move(reader_factory)),
          merge_function_wrapper_factory_(std::move(merge_function_wrapper_factory)),
          key_comparator_(std::move(key_comparator)),
          cancellation_checker_(std::move(cancellation_checker)),
          drop_delete_(drop_delete),
          produce_data_(produce_data),
          produce_changelog_(produce_changelog) {}

    Status Produce(AsyncKeyValueBatchSink* sink) override {
        std::vector<KeyValue> compact_buffer;
        std::vector<KeyValue> changelog_buffer;
        compact_buffer.reserve(write_batch_size_);
        changelog_buffer.reserve(write_batch_size_);

        auto flush = [&](AsyncKeyValueBatchType type, std::vector<KeyValue>* buffer) -> Status {
            if (buffer->empty()) {
                return Status::OK();
            }
            std::vector<KeyValue> rows = std::move(*buffer);
            buffer->clear();
            buffer->reserve(write_batch_size_);
            return sink->Write(type, std::move(rows));
        };

        auto emit_result = [&](ChangelogResult&& result) -> Status {
            if (produce_data_ && result.result &&
                (!drop_delete_ || result.result->value_kind->IsAdd())) {
                compact_buffer.emplace_back(std::move(result.result).value());
                if (static_cast<int32_t>(compact_buffer.size()) >= write_batch_size_) {
                    PAIMON_RETURN_NOT_OK(flush(AsyncKeyValueBatchType::DATA, &compact_buffer));
                }
            }
            if (produce_changelog_) {
                for (auto& changelog : result.changelogs) {
                    changelog_buffer.emplace_back(std::move(changelog));
                    if (static_cast<int32_t>(changelog_buffer.size()) >= write_batch_size_) {
                        PAIMON_RETURN_NOT_OK(
                            flush(AsyncKeyValueBatchType::CHANGELOG, &changelog_buffer));
                    }
                }
            }
            return Status::OK();
        };

        for (const auto& section : sections_) {
            if (cancellation_checker_()) {
                return Status::Cancelled("Compaction is cancelled");
            }
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortMergeReader> sort_merge_reader,
                                   reader_factory_(section));
            reader_holders_.emplace_back(std::move(sort_merge_reader));
            SortMergeReader* reader = reader_holders_.back().get();
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MergeFunctionWrapper<ChangelogResult>> wrapper,
                                   merge_function_wrapper_factory_());

            std::shared_ptr<InternalRow> current_key;
            auto finish_group = [&]() -> Status {
                if (!current_key) {
                    return Status::OK();
                }
                PAIMON_ASSIGN_OR_RAISE(std::optional<ChangelogResult> result, wrapper->GetResult());
                current_key.reset();
                if (result) {
                    PAIMON_RETURN_NOT_OK(emit_result(std::move(result).value()));
                }
                return Status::OK();
            };

            while (true) {
                if (cancellation_checker_()) {
                    return Status::Cancelled("Compaction is cancelled");
                }
                PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortMergeReader::Iterator> iterator,
                                       reader->NextBatch());
                if (!iterator) {
                    break;
                }
                while (true) {
                    PAIMON_ASSIGN_OR_RAISE(bool has_next, iterator->HasNext());
                    if (!has_next) {
                        break;
                    }
                    KeyValue key_value = iterator->Next();
                    if (current_key && key_comparator_(*current_key, *key_value.key) != 0) {
                        PAIMON_RETURN_NOT_OK(finish_group());
                    }
                    if (!current_key) {
                        wrapper->Reset();
                        current_key = key_value.key;
                    }
                    PAIMON_RETURN_NOT_OK(wrapper->Add(std::move(key_value)));
                }
            }
            PAIMON_RETURN_NOT_OK(finish_group());
            reader->Close();
        }
        PAIMON_RETURN_NOT_OK(flush(AsyncKeyValueBatchType::DATA, &compact_buffer));
        PAIMON_RETURN_NOT_OK(flush(AsyncKeyValueBatchType::CHANGELOG, &changelog_buffer));
        return Status::OK();
    }

 private:
    const std::vector<std::vector<SortedRun>>& sections_;
    std::vector<std::unique_ptr<SortMergeReader>>& reader_holders_;
    int32_t write_batch_size_;
    SortMergeReaderFactory reader_factory_;
    BoundChangelogMergeFunctionWrapperFactory merge_function_wrapper_factory_;
    KeyComparator key_comparator_;
    CancellationChecker cancellation_checker_;
    bool drop_delete_;
    bool produce_data_;
    bool produce_changelog_;
};

}  // namespace

ChangelogMergeTreeRewriter::ChangelogMergeTreeRewriter(
    int32_t max_level, bool force_drop_delete, const BinaryRow& partition, int32_t bucket,
    int64_t schema_id, const std::vector<std::string>& trimmed_primary_keys,
    const CoreOptions& options, const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<arrow::Schema>& write_schema, DeletionVector::Factory dv_factory,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    std::unique_ptr<MergeFileSplitRead>&& merge_file_split_read,
    MergeFunctionWrapperFactory merge_function_wrapper_factory,
    ChangelogMergeFunctionWrapperFactory changelog_merge_function_wrapper_factory,
    bool produce_changelog, const std::shared_ptr<CancellationController>& cancellation_controller,
    const std::shared_ptr<MemoryPool>& pool)
    : MergeTreeCompactRewriter(
          partition, bucket, schema_id, trimmed_primary_keys, options, data_schema, write_schema,
          std::move(dv_factory), path_factory_cache, std::move(merge_file_split_read),
          std::move(merge_function_wrapper_factory), cancellation_controller, pool),
      max_level_(max_level),
      force_drop_delete_(force_drop_delete),
      changelog_merge_function_wrapper_factory_(
          std::move(changelog_merge_function_wrapper_factory)),
      produce_changelog_(produce_changelog) {}

Result<CompactResult> ChangelogMergeTreeRewriter::Rewrite(
    int32_t output_level, bool drop_delete, const std::vector<std::vector<SortedRun>>& sections) {
    if (RewriteChangelog(output_level, drop_delete, sections)) {
        return RewriteOrProduceChangelog(output_level, sections, drop_delete,
                                         /*rewrite_compact_file=*/true);
    } else {
        return RewriteCompaction(output_level, drop_delete, sections);
    }
}

Result<CompactResult> ChangelogMergeTreeRewriter::Upgrade(
    int32_t output_level, const std::shared_ptr<DataFileMeta>& file) {
    UpgradeStrategy upgrade_strategy = GenerateUpgradeStrategy(output_level, file);
    if (upgrade_strategy.changelog) {
        return RewriteOrProduceChangelog(output_level, {{SortedRun::FromSingle(file)}},
                                         force_drop_delete_, upgrade_strategy.rewrite);
    } else {
        return MergeTreeCompactRewriter::Upgrade(output_level, file);
    }
}

bool ChangelogMergeTreeRewriter::RewriteLookupChangelog(
    int32_t output_level, const std::vector<std::vector<SortedRun>>& sections) const {
    if (output_level == 0) {
        return false;
    }
    for (const auto& runs : sections) {
        for (const auto& run : runs) {
            for (const auto& file : run.Files()) {
                if (file->level == 0) {
                    return true;
                }
            }
        }
    }
    return false;
}

Result<CompactResult> ChangelogMergeTreeRewriter::RewriteOrProduceChangelog(
    int32_t output_level, const std::vector<std::vector<SortedRun>>& sections, bool drop_delete,
    bool rewrite_compact_file) {
    PAIMON_ASSIGN_OR_RAISE(MergeTreeCompactRewriter::KeyValueConsumerCreator create_consumer,
                           GenerateKeyValueConsumer());
    auto before = ExtractFilesFromSections(sections);
    std::unique_ptr<MergeTreeCompactRewriter::KeyValueRollingFileWriter> compact_file_writer;
    if (rewrite_compact_file) {
        PAIMON_ASSIGN_OR_RAISE(compact_file_writer, CreateRollingRowWriter(output_level));
    }
    std::unique_ptr<MergeTreeCompactRewriter::KeyValueRollingFileWriter> changelog_file_writer;
    if (produce_changelog_) {
        PAIMON_ASSIGN_OR_RAISE(changelog_file_writer, CreateRollingChangelogWriter(output_level));
    }

    std::vector<std::unique_ptr<SortMergeReader>> reader_holders;
    ScopeGuard write_guard([&]() -> void {
        if (compact_file_writer) {
            compact_file_writer->Abort();
            compact_file_writer.reset();
        }
        if (changelog_file_writer) {
            changelog_file_writer->Abort();
            changelog_file_writer.reset();
        }
        for (const auto& reader : reader_holders) {
            reader->Close();
        }
        merge_file_split_read_.reset();
    });

    bool produce_data = compact_file_writer != nullptr;
    bool produce_changelog = changelog_file_writer != nullptr;
    SortMergeReaderFactory reader_factory = [this](const std::vector<SortedRun>& section) {
        return CreateRawSortMergeReaderForSection(section);
    };
    BoundChangelogMergeFunctionWrapperFactory merge_function_wrapper_factory =
        [factory = changelog_merge_function_wrapper_factory_, output_level]() {
            return factory(output_level);
        };
    KeyComparator key_comparator = [this](const InternalRow& lhs, const InternalRow& rhs) {
        return merge_file_split_read_->GetKeyComparator()->CompareTo(lhs, rhs);
    };
    CancellationChecker cancellation_checker = [this]() { return IsCancelled(); };
    std::unique_ptr<AsyncKeyValueBatchProducer> producer =
        std::make_unique<ChangelogCompactionBatchProducer>(
            sections, reader_holders, options_.GetWriteBatchSize(), std::move(reader_factory),
            std::move(merge_function_wrapper_factory), std::move(key_comparator),
            std::move(cancellation_checker), drop_delete, produce_data, produce_changelog);

    auto producer_and_consumer =
        std::make_unique<AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch>>(
            std::move(producer), std::move(create_consumer), /*consumer_thread_num=*/1);
    while (true) {
        if (IsCancelled()) {
            return Status::Cancelled("Compaction is cancelled");
        }
        PAIMON_ASSIGN_OR_RAISE(AsyncKeyValueResultBatch<KeyValueBatch> output,
                               producer_and_consumer->NextBatchWithType());
        if (output.result.batch == nullptr) {
            break;
        }
        if (output.type == AsyncKeyValueBatchType::DATA) {
            PAIMON_RETURN_NOT_OK(compact_file_writer->Write(std::move(output.result)));
        } else {
            PAIMON_RETURN_NOT_OK(changelog_file_writer->Write(std::move(output.result)));
        }
    }
    producer_and_consumer->Close();
    if (compact_file_writer) {
        PAIMON_RETURN_NOT_OK(compact_file_writer->Close());
    }
    if (changelog_file_writer) {
        PAIMON_RETURN_NOT_OK(changelog_file_writer->Close());
    }
    std::vector<std::shared_ptr<DataFileMeta>> after;
    if (compact_file_writer) {
        PAIMON_ASSIGN_OR_RAISE(after, compact_file_writer->GetResult());
    } else {
        after.reserve(before.size());
        for (const auto& file : before) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> new_file,
                                   file->Upgrade(output_level));
            after.emplace_back(std::move(new_file));
        }
    }
    if (rewrite_compact_file) {
        NotifyRewriteCompactBefore(before);
    }
    PAIMON_ASSIGN_OR_RAISE(after, NotifyRewriteCompactAfter(after));
    std::vector<std::shared_ptr<DataFileMeta>> changelog_files;
    if (changelog_file_writer) {
        PAIMON_ASSIGN_OR_RAISE(changelog_files, changelog_file_writer->GetResult());
    }
    write_guard.Release();
    return CompactResult(before, after, changelog_files);
}

}  // namespace paimon

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

#include "paimon/core/io/shredding_key_value_data_file_writer_factory.h"

#include <utility>

#include "arrow/c/helpers.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_index_writer.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/infer_shredding_file_writer.h"
#include "paimon/core/io/key_value_data_file_writer.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/file_system.h"

namespace paimon {

ShreddingKeyValueDataFileWriterFactory::ShreddingKeyValueDataFileWriterFactory(
    const CoreOptions& options, int64_t schema_id,
    const std::shared_ptr<arrow::Schema>& write_schema, int32_t level, FileSource file_source,
    const std::vector<std::string>& primary_keys,
    const std::shared_ptr<DataFilePathFactory>& path_factory, bool create_stats_extractor,
    const std::shared_ptr<ShreddingWritePlanFactory>& plan_factory, bool is_changelog,
    const std::shared_ptr<MemoryPool>& pool)
    : KeyValueDataFileWriterFactory(options, schema_id, write_schema, level, file_source,
                                    primary_keys, path_factory, create_stats_extractor,
                                    is_changelog, pool),
      plan_factory_(plan_factory) {}

Result<std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>>
ShreddingKeyValueDataFileWriterFactory::CreateWriter() const {
    if (!plan_factory_) {
        return Status::Invalid("Shredding key-value writer requires a write-plan factory.");
    }
    const std::string format_identifier = GetFileFormat()->Identifier();
    if (plan_factory_->ShouldInferWritePlan()) {
        auto create_inner = [this](const std::shared_ptr<ShreddingBatchConverter>& converter) {
            return CreateShreddedWriter(converter);
        };
        return std::make_unique<
            InferShreddingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>(
            write_schema_, plan_factory_, format_identifier, std::move(create_inner));
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ShreddingBatchConverter> converter,
        plan_factory_->CreateConverter(format_identifier, /*sample_batches=*/{}));
    return CreateShreddedWriter(converter);
}

Result<std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>>
ShreddingKeyValueDataFileWriterFactory::CreateShreddedWriter(
    const std::shared_ptr<ShreddingBatchConverter>& converter) const {
    if (converter == nullptr) {
        // No conversion is useful for this file; fall back to the plain writer.
        std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>> writer;
        PAIMON_ASSIGN_OR_RAISE(writer, KeyValueDataFileWriterFactory::CreateWriter());
        writer->SetCompletionCallback(
            [factory = plan_factory_, converter]() { return factory->OnFileCompleted(converter); });
        return writer;
    }
    std::shared_ptr<FileFormat> format = GetFileFormat();
    std::string compression = GetFileCompression();
    std::shared_ptr<arrow::Schema> file_schema = converter->GetPhysicalSchema();
    std::function<Status(KeyValueBatch&&, ::ArrowArray*)> batch_converter =
        [converter](KeyValueBatch key_value_batch, ::ArrowArray* array) -> Status {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowArray> physical,
                               converter->Convert(key_value_batch.batch.get()));
        ArrowArrayMove(physical.get(), array);
        return Status::OK();
    };
    PAIMON_ASSIGN_OR_RAISE(WriterResources resources,
                           CreateWriterResources(*format, file_schema, create_stats_extractor_));
    auto writer = std::make_unique<KeyValueDataFileWriter>(
        compression, std::move(batch_converter), schema_id_, level_, file_source_, primary_keys_,
        resources.stats_extractor, file_schema, path_factory_->IsExternalPath(), pool_);
    // Changelog files are consumed sequentially and intentionally do not produce file indexes.
    if (!is_changelog_) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<DataFileIndexWriter> file_index_writer,
                               CreateFileIndexWriter(write_schema_, path_factory_));
        if (file_index_writer) {
            writer->SetFileIndexWriter(std::move(file_index_writer), write_schema_);
        }
    }
    PAIMON_RETURN_NOT_OK(writer->Init(options_.GetFileSystem(), NewFilePath(format->Identifier()),
                                      resources.writer_builder));
    ShreddingWritePlanFactory::MetadataFinalizer finalizer =
        plan_factory_->CreateMetadataFinalizer(converter, compression);
    if (finalizer) {
        writer->SetMetadataFinalizer(std::move(finalizer));
    }
    writer->SetCompletionCallback(
        [factory = plan_factory_, converter]() { return factory->OnFileCompleted(converter); });
    return std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>(
        std::move(writer));
}

}  // namespace paimon

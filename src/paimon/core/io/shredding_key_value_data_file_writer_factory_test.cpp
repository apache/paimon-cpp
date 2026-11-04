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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

// A valid plan may decide that a particular file does not benefit from conversion.
class PlainFallbackWritePlanFactory : public ShreddingWritePlanFactory {
 public:
    explicit PlainFallbackWritePlanFactory(int32_t buffer_rows) : buffer_rows_(buffer_rows) {}

    bool ShouldCreateWritePlan() const override {
        return true;
    }
    bool ShouldInferWritePlan() const override {
        return buffer_rows_ > 0;
    }
    int32_t InferBufferRowCount() const override {
        return buffer_rows_;
    }
    Result<std::shared_ptr<ShreddingBatchConverter>> CreateConverter(
        const std::string&, const std::vector<std::shared_ptr<arrow::Array>>&) override {
        return std::shared_ptr<ShreddingBatchConverter>();
    }
    MetadataFinalizer CreateMetadataFinalizer(const std::shared_ptr<ShreddingBatchConverter>&,
                                              const std::string&) const override {
        return nullptr;
    }
    Status OnFileCompleted(const std::shared_ptr<ShreddingBatchConverter>&) override {
        ++completed_files;
        return Status::OK();
    }

    int32_t completed_files = 0;

 private:
    int32_t buffer_rows_;
};

}  // namespace

TEST(ShreddingKeyValueDataFileWriterFactoryTest, PlainFallbackPreservesBatchesAndMetadata) {
    using Writer = SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>;
    auto pool = GetDefaultPool();
    auto schema = SpecialFields::CompleteSequenceAndValueKindField(
        arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::utf8())}));
    auto data = arrow::ipc::internal::json::ArrayFromJSON(
                    arrow::struct_(schema->fields()),
                    R"([[10, 0, 1, "a"], [11, 0, 2, null], [12, 0, 3, "c"]])")
                    .ValueOrDie();
    auto record_batch = arrow::RecordBatch::FromStructArray(data).ValueOrDie();
    // Cover immediate planning, inference with subsequent writes, inference on the last batch,
    // and inference at Close().
    for (int32_t buffer_rows : {0, 2, 3, 10}) {
        SCOPED_TRACE(buffer_rows);
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::FILE_FORMAT, "parquet"}}));
        auto path_factory = std::make_shared<DataFilePathFactory>();
        ASSERT_OK(path_factory->Init(dir->Str(), "parquet", "data-", nullptr));
        auto plan_factory = std::make_shared<PlainFallbackWritePlanFactory>(buffer_rows);
        ShreddingKeyValueDataFileWriterFactory factory(
            options, /*schema_id=*/7, schema, /*level=*/0, FileSource::Append(),
            /*primary_keys=*/{"k"}, path_factory, /*create_stats_extractor=*/true, plan_factory,
            /*is_changelog=*/false, pool);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<Writer> writer, factory.CreateWriter());
        for (int32_t offset : {0, 2}) {
            auto slice = record_batch->Slice(offset, 2);
            KeyValueBatch batch;
            batch.min_sequence_number = 10 + offset;
            batch.max_sequence_number = 10 + offset + slice->num_rows() - 1;
            batch.min_key = BinaryRowGenerator::GenerateRowPtr({offset + 1}, pool.get());
            batch.max_key = BinaryRowGenerator::GenerateRowPtr(
                {offset + static_cast<int32_t>(slice->num_rows())}, pool.get());
            batch.batch = std::make_unique<ArrowArray>();
            auto export_status = arrow::ExportRecordBatch(*slice, batch.batch.get());
            ASSERT_TRUE(export_status.ok()) << export_status.ToString();
            ASSERT_OK(writer->Write(std::move(batch)));
        }
        ASSERT_EQ(plan_factory->completed_files, 0);
        ASSERT_OK(writer->Close());
        ASSERT_EQ(plan_factory->completed_files, 1);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> meta, writer->GetResult());
        ASSERT_EQ(meta->row_count, 3);
        ASSERT_EQ(meta->schema_id, 7);
        ASSERT_EQ(meta->min_sequence_number, 10);
        ASSERT_EQ(meta->max_sequence_number, 12);
        ASSERT_EQ(meta->min_key.GetInt(0), 1);
        ASSERT_EQ(meta->max_key.GetInt(0), 3);

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReaderBuilder> reader_builder,
                             options.GetFileFormat()->CreateReaderBuilder(/*batch_size=*/2));
        reader_builder->WithMemoryPool(pool);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input,
                             options.GetFileSystem()->Open(writer->GetPath()));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, reader_builder->Build(input));
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                             ReadResultCollector::CollectResult(std::move(reader)));
        ASSERT_TRUE(result);
        ASSERT_TRUE(result->Equals(arrow::ChunkedArray(data))) << result->ToString();
    }
}

}  // namespace paimon::test

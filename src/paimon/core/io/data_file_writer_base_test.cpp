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

#include "paimon/core/io/data_file_writer_base.h"

#include <memory>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_index_writer.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/file_index_options.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class IndexedDataFileWriter : public DataFileWriterBase<::ArrowArray*> {
 public:
    IndexedDataFileWriter() : DataFileWriterBase(/*compression=*/"zstd", /*converter=*/nullptr) {}

    Status Write(::ArrowArray* batch) override {
        return WriteRecordWithFileIndex(batch);
    }

    Result<std::shared_ptr<DataFileMeta>> GetResult() override {
        return std::shared_ptr<DataFileMeta>();
    }
};

TEST(DataFileWriterBaseTest, WriteAfterCloseDoesNotConsumeIndexBatch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::shared_ptr<arrow::Schema> schema = arrow::schema({arrow::field("col", arrow::int32())});
    std::shared_ptr<arrow::DataType> data_type = arrow::struct_(schema->fields());
    ASSERT_OK_AND_ASSIGN(
        CoreOptions options,
        CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"},
                              {"file-index.bitmap.columns", "col"},
                              {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1MB"}}));
    std::shared_ptr<FileSystem> file_system = options.GetFileSystem();
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", "data-", nullptr));
    ASSERT_OK_AND_ASSIGN(FileIndexOptions file_index_options,
                         FileIndexOptions::FromCoreOptions(options));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DataFileIndexWriter> file_index_writer,
        DataFileIndexWriter::Create(schema, file_index_options, file_system, path_factory, pool));

    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<WriterBuilder> writer_builder,
                         options.GetFileFormat()->CreateWriterBuilder(&c_schema,
                                                                      /*batch_size=*/100));
    IndexedDataFileWriter writer;
    writer.SetFileIndexWriter(std::move(file_index_writer), schema);
    ASSERT_OK(writer.Init(file_system, path_factory->NewPath(), writer_builder));

    std::shared_ptr<arrow::Array> first_array =
        arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([[1]])").ValueOrDie();
    ::ArrowArray first_batch;
    ASSERT_TRUE(arrow::ExportArray(*first_array, &first_batch).ok());
    ASSERT_OK(writer.Write(&first_batch));
    ASSERT_OK(writer.Close());

    std::shared_ptr<arrow::Array> second_array =
        arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([[2]])").ValueOrDie();
    ::ArrowArray second_batch;
    ASSERT_TRUE(arrow::ExportArray(*second_array, &second_batch).ok());
    ASSERT_NOK_WITH_MSG(writer.Write(&second_batch), "Writer has already closed");
    ASSERT_NE(nullptr, second_batch.release);
    ArrowArrayRelease(&second_batch);
}

}  // namespace paimon::test

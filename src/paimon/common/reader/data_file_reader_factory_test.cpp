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
#include "paimon/common/reader/data_file_reader_factory.h"

#include <memory>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/reader/delegating_prefetch_reader.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/mock/mock_format_reader_builder.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/prefetch_cache_config.h"

namespace paimon::test {

/// `Open()` is the one place a production read builds the prefetching reader, and a level that is
/// not forwarded here fails nothing: the reader warms up however its own default says, and the only
/// symptom is latency and memory a query never asked for.
TEST(DataFileReaderFactoryTest, OpenForwardsWarmupLevelToPrefetchReader) {
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    auto mock_fs = std::make_shared<MockFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor,
                         CreateDefaultExecutor(/*thread_count=*/1));

    std::shared_ptr<arrow::DataType> data_type =
        arrow::struct_({arrow::field("f1", arrow::int32())});
    std::shared_ptr<arrow::Array> data =
        arrow::StructArray::Make(
            {arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 3]").ValueOrDie()},
            data_type->fields())
            .ValueOrDie();

    for (WarmupLevel level : {WarmupLevel::NONE, WarmupLevel::RAW, WarmupLevel::DECODED}) {
        MockFormatReaderBuilder reader_builder(data, data_type, /*read_batch_size=*/10);
        DataFileReadOptions read_options;
        read_options.read_batch_size = 10;
        read_options.prefetch_enabled = true;
        read_options.prefetch_max_parallel_num = 1;
        read_options.prefetch_batch_count = 2;
        // No file backs the mock file system, so keep the read-ahead cache out of the read.
        read_options.read_ahead_cache_enabled = false;
        read_options.warmup_level = level;

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader,
                             DataFileReaderFactory::Open(
                                 "parquet", /*file_path=*/"DUMMY", /*file_size=*/0, &reader_builder,
                                 read_options, mock_fs, executor, pool, GetArrowPool(pool)));

        // `Open()` hands back the delegating wrapper. The level has to have arrived on the
        // prefetching reader underneath it, because that reader's Warmup() is what it governs.
        auto* delegating_reader = dynamic_cast<DelegatingPrefetchReader*>(reader.get());
        ASSERT_NE(delegating_reader, nullptr);
        EXPECT_EQ(level, delegating_reader->prefetch_reader_->warmup_level_);
    }
}

}  // namespace paimon::test

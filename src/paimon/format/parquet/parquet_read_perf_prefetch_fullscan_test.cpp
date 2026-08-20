/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/common/reader/prefetch_file_batch_reader_impl.h"

#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/read_ahead_cache.h"

namespace paimon::parquet::test {

// Command-line usage:
//   ./parquet_read_perf_prefetch_fullscan_test [batch_size] [enable_pre_buffer]
//   [parquet_executor_thread_count] [prefetch_max_parallel_num]
//   [prefetch_executor_thread_count]
//
// Example:
//   ./parquet_read_perf_prefetch_fullscan_test 4096 false 0 3 3
//
// Defaults: batch_size=4096, enable_pre_buffer="false",
// parquet_executor_thread_count="0", prefetch_max_parallel_num=3,
// prefetch_executor_thread_count=3

static int32_t g_batch_size = 4096;
static std::string g_enable_pre_buffer = "false";
static std::string g_parquet_executor_thread_count = "0";
static uint32_t g_prefetch_max_parallel_num = 3;
static uint32_t g_prefetch_executor_thread_count = 3;

class ParquetReadPerfPrefetchFullScanTest : public ::testing::Test {
 public:
    void SetUp() override {
        fs_ = std::make_shared<LocalFileSystem>();
        pool_ = GetDefaultPool();
        batch_size_ = g_batch_size;
        enable_pre_buffer_ = g_enable_pre_buffer;
        parquet_executor_thread_count_ = g_parquet_executor_thread_count;
        prefetch_max_parallel_num_ = g_prefetch_max_parallel_num;
        prefetch_executor_thread_count_ = g_prefetch_executor_thread_count;
    }

    void TearDown() override {}

    Result<std::unique_ptr<PrefetchFileBatchReaderImpl>> OpenPrefetchReader(
        const std::string& file_path) {
        std::map<std::string, std::string> options{
            {PARQUET_READ_ENABLE_PRE_BUFFER, enable_pre_buffer_},
            {PARQUET_READ_EXECUTOR_THREAD_COUNT, parquet_executor_thread_count_}};
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileFormat> file_format,
                               FileFormatFactory::Get("parquet", options));
        PAIMON_ASSIGN_OR_RAISE(auto reader_builder,
                               file_format->CreateReaderBuilder(batch_size_));
        reader_builder->WithMemoryPool(pool_);

        PAIMON_ASSIGN_OR_RAISE(auto file_status, fs_->GetFileStatus(file_path));
        int64_t data_file_size = file_status->GetLen();

        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Executor> executor,
                               CreateDefaultExecutor(prefetch_executor_thread_count_));

        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<PrefetchFileBatchReaderImpl> reader,
            PrefetchFileBatchReaderImpl::Create(
                file_path, data_file_size, reader_builder.get(), fs_,
                prefetch_max_parallel_num_, batch_size_,
                prefetch_max_parallel_num_ * 2,
                /*enable_adaptive_prefetch_strategy=*/false, executor,
                /*initialize_read_ranges=*/false,
                /*prefetch_cache_mode=*/PrefetchCacheMode::NEVER, CacheConfig(), pool_));
        return std::move(reader);
    }

    Result<std::shared_ptr<arrow::Schema>> GetFileSchema(
        const std::unique_ptr<PrefetchFileBatchReaderImpl>& reader) const {
        auto schema_result = reader->GetFileSchema();
        EXPECT_TRUE(schema_result.ok());
        auto arrow_schema = arrow::ImportSchema(schema_result.value().get()).ValueOrDie();
        return arrow_schema;
    }

    Status SetReadSchema(const std::unique_ptr<PrefetchFileBatchReaderImpl>& reader,
                         const std::shared_ptr<arrow::Schema>& read_schema) const {
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, c_schema.get()));
        PAIMON_RETURN_NOT_OK(reader->SetReadSchema(c_schema.get(), nullptr, std::nullopt));
        return Status::OK();
    }

    Result<int64_t> DrainAllBatches(const std::unique_ptr<PrefetchFileBatchReaderImpl>& reader) {
        int64_t total_rows = 0;
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(auto batch_with_bitmap, reader->NextBatchWithBitmap());
            if (BatchReader::IsEofBatch(batch_with_bitmap)) {
                break;
            }
            total_rows += batch_with_bitmap.first.first->length;
            ReaderUtils::ReleaseReadBatch(std::move(batch_with_bitmap.first));
        }
        return total_rows;
    }

    void RunPrefetchFullScan(const std::string& file_path, const std::string& test_label) {
        ASSERT_OK_AND_ASSIGN(auto reader, OpenPrefetchReader(file_path));
        ASSERT_OK_AND_ASSIGN(auto file_schema, GetFileSchema(reader));
        ASSERT_OK(SetReadSchema(reader, file_schema));

        bool need_prefetch = reader->NeedPrefetch();

        auto start = std::chrono::steady_clock::now();
        ASSERT_OK_AND_ASSIGN(auto total_rows, DrainAllBatches(reader));
        auto end = std::chrono::steady_clock::now();
        auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        reader->Close();
        std::cout << "[" << test_label << "] batch_size=" << batch_size_
                  << ", pre_buffer=" << enable_pre_buffer_
                  << ", parquet_threads=" << parquet_executor_thread_count_
                  << ", prefetch_parallel=" << prefetch_max_parallel_num_
                  << ", prefetch_threads=" << prefetch_executor_thread_count_
                  << " | need_prefetch=" << (need_prefetch ? "true" : "false")
                  << ", total_rows=" << total_rows << ", elapsed=" << elapsed_ms
                  << " ms" << std::endl;
    }

 protected:
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
    int32_t batch_size_;
    std::string enable_pre_buffer_;
    std::string parquet_executor_thread_count_;
    uint32_t prefetch_max_parallel_num_;
    uint32_t prefetch_executor_thread_count_;
};

// ---------- mainse_sorted_by_nid.parquet ----------
// Schema: all string fields, sorted by nid, ~6.5GB
TEST_F(ParquetReadPerfPrefetchFullScanTest, MainseFullScan) {
    std::string file_path = paimon::test::GetDataDir() + "/mainse_sorted_by_nid.parquet";
    RunPrefetchFullScan(file_path, "MainsePrefetchFullScan");
}

// ---------- fmm_sorted_by_user_id.parquet ----------
// Schema: user_id(string), spm_cnt(int64), args(string), ds(string)
// Sorted by user_id, ~11M rows
TEST_F(ParquetReadPerfPrefetchFullScanTest, FmmFullScan) {
    std::string file_path = paimon::test::GetDataDir() + "/fmm_sorted_by_user_id.parquet";
    RunPrefetchFullScan(file_path, "FmmPrefetchFullScan");
}

}  // namespace paimon::parquet::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    // Parse custom arguments after gtest consumes its own flags.
    // Usage: ./parquet_read_perf_prefetch_fullscan_test [batch_size] [enable_pre_buffer]
    //        [parquet_executor_thread_count] [prefetch_max_parallel_num]
    //        [prefetch_executor_thread_count]
    if (argc > 1) {
        paimon::parquet::test::g_batch_size = std::stoi(argv[1]);
    }
    if (argc > 2) {
        paimon::parquet::test::g_enable_pre_buffer = argv[2];
    }
    if (argc > 3) {
        paimon::parquet::test::g_parquet_executor_thread_count = argv[3];
    }
    if (argc > 4) {
        paimon::parquet::test::g_prefetch_max_parallel_num =
            static_cast<uint32_t>(std::stoul(argv[4]));
    }
    if (argc > 5) {
        paimon::parquet::test::g_prefetch_executor_thread_count =
            static_cast<uint32_t>(std::stoul(argv[5]));
    }

    std::cout << "Config: batch_size=" << paimon::parquet::test::g_batch_size
              << ", enable_pre_buffer=" << paimon::parquet::test::g_enable_pre_buffer
              << ", parquet_executor_thread_count="
              << paimon::parquet::test::g_parquet_executor_thread_count
              << ", prefetch_max_parallel_num="
              << paimon::parquet::test::g_prefetch_max_parallel_num
              << ", prefetch_executor_thread_count="
              << paimon::parquet::test::g_prefetch_executor_thread_count << std::endl;

    return RUN_ALL_TESTS();
}

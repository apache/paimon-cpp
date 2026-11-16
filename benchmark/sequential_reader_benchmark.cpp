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

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/c/abi.h"
#include "benchmark/benchmark.h"
#include "fmt/format.h"
#include "paimon/common/reader/complete_row_kind_batch_reader.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/format/file_format.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/counting_reader_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

bool benchmark_failed = false;

struct Fixture {
    std::unique_ptr<UniqueTestDirectory> directory;
    std::string table_path;
    std::shared_ptr<TableSchema> table_schema;
    std::vector<std::shared_ptr<DataFileMeta>> files;
};

Result<Fixture> PrepareFiles(int64_t count) {
    Fixture fixture;
    fixture.directory = UniqueTestDirectory::Create();
    if (!fixture.directory) {
        return Status::IOError("cannot create benchmark directory");
    }
    fixture.table_path =
        GetDataDir() + "/orc/multi_partition_append_table.db/multi_partition_append_table";
    SchemaManager schemas(fixture.directory->GetFileSystem(), fixture.table_path);
    PAIMON_ASSIGN_OR_RAISE(fixture.table_schema, schemas.ReadSchema(0));
    const std::string source =
        fixture.table_path + "/f1=10/f2=0/bucket-0/data-01b6a930-6564-409b-b8f4-ed1307790d72-0.orc";
    PAIMON_ASSIGN_OR_RAISE(FileStatus source_status,
                           fixture.directory->GetFileSystem()->GetFileStatus(source));
    for (int64_t i = 0; i < count; i++) {
        const std::string name = fmt::format("data-{}.orc", i);
        const std::string path = fixture.directory->Str() + "/" + name;
        std::error_code error;
        std::filesystem::copy_file(source, path, error);
        if (error) {
            return Status::IOError(error.message());
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<DataFileMeta> file,
            DataFileMeta::ForAppend(name, source_status.GetLen(), /*row_count=*/3,
                                    SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
                                    /*max_sequence_number=*/2, /*schema_id=*/0,
                                    FileSource::Append(), /*value_stats_cols=*/std::nullopt, path,
                                    /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt));
        fixture.files.push_back(std::move(file));
    }
    return fixture;
}

struct ScanStats {
    double seconds = 0;
    double first_batch_us = 0;
    uint64_t initial_footer_reads = 0;
    uint64_t first_batch_opens = 0;
    uint64_t opens = 0;
    uint64_t peak_streams = 0;
    uint64_t pool_peak_bytes = 0;
    uint64_t read_bytes = 0;
    int64_t rows = 0;
};

Result<ScanStats> Scan(const Fixture& fixture, bool lazy, bool first_only, int64_t open_delay_us) {
    std::shared_ptr<MemoryPool> pool = GetMemoryPool();
    auto fs = std::make_shared<CountingReaderFileSystem>(".orc");
    if (open_delay_us > 0) {
        fs->before_open = [open_delay_us](const std::string&) {
            std::this_thread::sleep_for(std::chrono::microseconds(open_delay_us));
            return Status::OK();
        };
    }
    ReadContextBuilder builder(fixture.table_path);
    builder.WithFileSystem(fs).WithMemoryPool(pool).EnablePrefetch(false);
    builder.AddOption(Options::FILE_INDEX_READ_ENABLED, "false");
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context, builder.Finish());
    auto read_options = fixture.table_schema->Options();
    for (const auto& [key, value] : read_context->GetOptions()) {
        read_options.insert_or_assign(key, value);
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> context,
        InternalReadContext::Create(read_context, fixture.table_schema, read_options));
    const auto& options = context->GetCoreOptions();
    auto schema = DataField::ConvertDataFieldsToArrowSchema(fixture.table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> paths,
        FileStorePathFactory::Create(
            fixture.table_path, schema, fixture.table_schema->PartitionKeys(),
            options.GetPartitionDefaultName(), options.GetFileFormat()->Identifier(),
            options.DataFilePrefix(), options.LegacyPartitionNameEnabled(), {}, {},
            options.IndexFileInDataFileDir(), {}, pool));
    auto partition = BinaryRowGenerator::GenerateRow({10, 0}, pool.get());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_paths,
                           paths->CreateDataFilePathFactory(partition, 0));
    RawFileSplitRead split_read(paths, context, pool, context->GetExecutor());
    auto arrow_pool = GetArrowPool(pool);
    const auto start = std::chrono::steady_clock::now();
    std::unique_ptr<BatchReader> reader;
    if (lazy) {
        PAIMON_ASSIGN_OR_RAISE(
            reader, split_read.CreateReader(partition, 0, fixture.files, DeletionVector::Factory(),
                                            std::nullopt));
    } else {
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::unique_ptr<FileBatchReader>> children,
            split_read.CreateRawFileReaders(partition, fixture.files, context->GetReadSchema(),
                                            nullptr, {}, std::nullopt, data_paths, {}));
        auto concat = std::make_unique<ConcatBatchReader>(
            ObjectUtils::MoveVector<std::unique_ptr<BatchReader>>(std::move(children)), arrow_pool);
        reader = std::make_unique<CompleteRowKindBatchReader>(std::move(concat), arrow_pool);
    }
    ScanStats stats;
    // With prefetch disabled and before NextBatch, these are the ORC footer/setup reads.
    stats.initial_footer_reads = fs->counts->reads.load();
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        if (stats.rows == 0) {
            stats.first_batch_us =
                std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start)
                    .count();
            stats.first_batch_opens = fs->counts->opens.load();
        }
        stats.rows += batch.first->length;
        ReaderUtils::ReleaseReadBatch(std::move(batch));
        if (first_only) {
            break;
        }
    }
    reader->Close();
    stats.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    stats.opens = fs->counts->opens.load();
    stats.peak_streams = fs->counts->peak.load();
    stats.read_bytes = fs->counts->bytes.load();
    stats.pool_peak_bytes = pool->MaxMemoryUsage();
    const int64_t expected = first_only ? 3 : 3 * static_cast<int64_t>(fixture.files.size());
    if (stats.rows != expected || fs->counts->live.load() != 0) {
        return Status::Invalid("unexpected row count or retained streams after Close");
    }
    return stats;
}

void BM_SequentialReaders(::benchmark::State& state) {
    auto fixture = PrepareFiles(state.range(0));
    if (!fixture.ok()) {
        benchmark_failed = true;
        state.SkipWithError(fixture.status().ToString().c_str());
        return;
    }
    int64_t rows = 0;
    for (auto _ : state) {
        auto scan = Scan(fixture.value(), state.range(1) != 0, state.range(2) != 0, state.range(3));
        if (!scan.ok()) {
            benchmark_failed = true;
            state.SkipWithError(scan.status().ToString().c_str());
            return;
        }
        const auto& stats = scan.value();
        state.SetIterationTime(stats.seconds);
        rows += stats.rows;
        state.counters["first_batch_us"] += stats.first_batch_us;
        state.counters["initial_footer_reads"] += stats.initial_footer_reads;
        state.counters["first_batch_opens"] += stats.first_batch_opens;
        state.counters["opens"] += stats.opens;
        state.counters["peak_streams"] += stats.peak_streams;
        state.counters["pool_peak_bytes"] += stats.pool_peak_bytes;
        state.counters["read_bytes"] += stats.read_bytes;
    }
    for (auto& [name, counter] : state.counters) {
        counter.flags = ::benchmark::Counter::kAvgIterations;
    }
    state.SetItemsProcessed(rows);
}

BENCHMARK(BM_SequentialReaders)
    ->ArgsProduct({{128, 1024}, {0, 1}, {0, 1}, {0, 1000}})
    ->ArgNames({"files", "lazy", "first_only", "open_delay_us"})
    ->UseManualTime();

}  // namespace
}  // namespace paimon::test

int32_t main(int32_t argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return paimon::test::benchmark_failed ? 1 : 0;
}

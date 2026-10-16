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

#include "paimon/core/realtime/realtime_deduplicate_state.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/realtime/arrow_deduplicate_realtime_store.h"
#include "paimon/core/realtime/arrow_realtime_store.h"
#include "paimon/core/realtime/realtime_key_offset_index.h"
#include "paimon/core/realtime/realtime_offset_file_index_lookup.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(RealtimeDeduplicateStateTest, TestAttachAndInstallSnapshot) {
    const std::shared_ptr<arrow::Schema> schema = arrow::schema({
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()),
        arrow::field("id", arrow::int64()),
        arrow::field("value", arrow::utf8()),
    });
    const std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    const std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(pool);
    auto delegate = std::make_shared<ArrowRealtimeStore>(
        schema, RealtimeStoreMode::APPEND_ONLY, StatisticsMode::NONE,
        /*temp_directory=*/"", /*spill_file_system=*/nullptr,
        /*spill_compression=*/"zstd", /*spill_compression_level=*/1, pool, arrow_pool);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ArrowDeduplicateRealtimeStore> store,
        ArrowDeduplicateRealtimeStore::Create(schema, {"id"}, delegate, pool, arrow_pool));

    std::unique_ptr<UniqueTestDirectory> directory = UniqueTestDirectory::Create();
    ASSERT_NE(nullptr, directory);
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(directory->Str(), /*format_identifier=*/"parquet",
                                 /*data_file_prefix=*/"data-",
                                 /*external_path_provider=*/nullptr));
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{"file-index.bitmap.columns",
                                                SpecialFields::RealtimeOffset().Name()}}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<RealtimeOffsetFileIndexLookup> initial_lookup,
        RealtimeOffsetFileIndexLookup::Create(
            schema, /*data_schema_id=*/0, schema->GetFieldByName("id"),
            /*data_files=*/{}, path_factory, directory->GetFileSystem(), pool, options));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<RealtimeOffsetFileIndexLookup> ignored_lookup,
        RealtimeOffsetFileIndexLookup::Create(
            schema, /*data_schema_id=*/0, schema->GetFieldByName("id"),
            /*data_files=*/{}, path_factory, directory->GetFileSystem(), pool, options));

    RealtimeDeduplicateState state;
    ASSERT_EQ(nullptr, state.committed_file_lookup_);
    ASSERT_NOK_WITH_MSG(state.AcquireFileLookup(), "lookup is not attached");
    ASSERT_NOK_WITH_MSG(state.AttachFileIndexLookup(nullptr, store), "cannot attach a null");
    ASSERT_NOK_WITH_MSG(state.AttachFileIndexLookup(initial_lookup, delegate),
                        "requires ArrowDeduplicateRealtimeStore");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> attached,
                         state.AttachFileIndexLookup(initial_lookup, store));
    ASSERT_EQ(initial_lookup, attached);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> existing,
                         state.AttachFileIndexLookup(ignored_lookup, store));
    ASSERT_EQ(initial_lookup, existing);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileMeta> first_file,
        DataFileMeta::ForAppend("prepared-0.parquet", /*file_size=*/0, /*row_count=*/1,
                                SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
                                /*max_sequence_number=*/0, /*schema_id=*/0,
                                /*file_source=*/std::nullopt, /*value_stats_cols=*/std::nullopt,
                                /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
                                /*write_cols=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileMeta> second_file,
        DataFileMeta::ForAppend("prepared-1.parquet", /*file_size=*/0, /*row_count=*/1,
                                SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
                                /*max_sequence_number=*/0, /*schema_id=*/0,
                                /*file_source=*/std::nullopt, /*value_stats_cols=*/std::nullopt,
                                /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
                                /*write_cols=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DataFileKeyOffsetIndexWriter> sidecar_writer,
        DataFileKeyOffsetIndexWriter::Create(schema->field(1), path_factory,
                                             directory->GetFileSystem(), pool, options.ToMap()));
    const std::shared_ptr<arrow::StructArray> keys = checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({schema->field(1)}), R"([[1]])")
            .ValueOrDie());
    const std::shared_ptr<arrow::Int64Array> offsets = checked_pointer_cast<arrow::Int64Array>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[0]").ValueOrDie());
    ASSERT_OK(sidecar_writer->AddBatch(keys, offsets));
    ASSERT_OK_AND_ASSIGN(std::string sidecar, sidecar_writer->Finish(first_file));
    first_file = first_file->CopyWithExtraFiles({sidecar});

    ASSERT_NOK_WITH_MSG(state.RegisterPreparedDataFile(nullptr, OffsetRange(0, 10)),
                        "invalid prepared data file or offset range");
    ASSERT_NOK_WITH_MSG(state.RegisterPreparedDataFile(first_file, OffsetRange(10, 10)),
                        "invalid prepared data file or offset range");
    ASSERT_OK(state.RegisterPreparedDataFile(first_file, OffsetRange(0, 10)));
    ASSERT_OK(state.RegisterPreparedDataFile(first_file, OffsetRange(0, 10)));
    ASSERT_OK(state.RegisterPreparedDataFile(second_file, OffsetRange(10, 20)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> prepared,
                         state.AcquireFileLookup());
    ASSERT_EQ((std::vector<std::shared_ptr<DataFileMeta>>{first_file, second_file}),
              prepared->DataFiles());

    ASSERT_NOK_WITH_MSG(state.InstallCommittedSnapshot(-1, {}, store),
                        "invalid deduplicate committed advancement");
    ASSERT_NOK_WITH_MSG(state.InstallCommittedSnapshot(1, {}, delegate),
                        "requires ArrowDeduplicateRealtimeStore");
    ASSERT_OK(state.InstallCommittedSnapshot(/*committed_end_offset=*/1,
                                             /*active_data_files=*/{}, store));
    const std::shared_ptr<RealtimeOffsetFileIndexLookup> installed = state.committed_file_lookup_;
    ASSERT_NE(nullptr, installed);
    ASSERT_NE(initial_lookup, installed);
    ASSERT_TRUE(installed->DataFiles().empty());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> crossing,
                         state.AcquireFileLookup());
    ASSERT_EQ(prepared->DataFiles(), crossing->DataFiles());
    ASSERT_OK(state.InstallCommittedSnapshot(/*committed_end_offset=*/5, {first_file}, store));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> deduplicated,
                         state.AcquireFileLookup());
    ASSERT_EQ(prepared->DataFiles(), deduplicated->DataFiles());

    // A committed file may already have been retired from the active snapshot. Its prepared
    // metadata must still be reclaimed by offset rather than waiting to see the file name again.
    ASSERT_OK(state.InstallCommittedSnapshot(/*committed_end_offset=*/10, {}, store));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> retained,
                         state.AcquireFileLookup());
    ASSERT_EQ((std::vector<std::shared_ptr<DataFileMeta>>{second_file}), retained->DataFiles());
    ASSERT_OK(state.RegisterPreparedDataFile(first_file, OffsetRange(0, 10)));
    ASSERT_OK(state.InstallCommittedSnapshot(/*committed_end_offset=*/15, {}, store));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> still_crossing,
                         state.AcquireFileLookup());
    ASSERT_EQ(retained->DataFiles(), still_crossing->DataFiles());
    ASSERT_OK(state.InstallCommittedSnapshot(/*committed_end_offset=*/20, {}, store));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> reclaimed,
                         state.AcquireFileLookup());
    ASSERT_TRUE(reclaimed->DataFiles().empty());
    ASSERT_EQ((std::vector<std::shared_ptr<DataFileMeta>>{first_file, second_file}),
              prepared->DataFiles());
}

}  // namespace paimon::test

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
#include "paimon/core/io/data_file_key_offset_index_writer.h"
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
    ASSERT_NOK_WITH_MSG(state.AttachFileIndexLookup(nullptr, store), "cannot attach a null");
    ASSERT_NOK_WITH_MSG(state.AttachFileIndexLookup(initial_lookup, delegate),
                        "requires ArrowDeduplicateRealtimeStore");
    ASSERT_OK(state.AttachFileIndexLookup(initial_lookup, store));
    ASSERT_EQ(initial_lookup, state.committed_file_lookup_);
    ASSERT_OK(state.AttachFileIndexLookup(ignored_lookup, store));
    ASSERT_EQ(initial_lookup, state.committed_file_lookup_);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileMeta> first_file,
        DataFileMeta::ForAppend("prepared-0.parquet", /*file_size=*/0, /*row_count=*/1,
                                SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
                                /*max_sequence_number=*/0, /*schema_id=*/0,
                                /*file_source=*/std::nullopt, /*value_stats_cols=*/std::nullopt,
                                /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
                                /*write_cols=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DataFileKeyOffsetIndexWriter> sidecar_writer,
        DataFileKeyOffsetIndexWriter::Create(schema->field(1), directory->GetFileSystem(), pool));
    const std::shared_ptr<arrow::StructArray> keys = checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({schema->field(1)}), R"([[1]])")
            .ValueOrDie());
    const std::shared_ptr<arrow::Int64Array> offsets = checked_pointer_cast<arrow::Int64Array>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[0]").ValueOrDie());
    const std::shared_ptr<arrow::StructArray> index_batch =
        arrow::StructArray::Make({keys->field(0), offsets}, {schema->field(1), schema->field(0)})
            .ValueOrDie();
    ASSERT_OK(sidecar_writer->AddBatch(index_batch));
    ASSERT_OK_AND_ASSIGN(
        std::string sidecar,
        sidecar_writer->Finish(path_factory->ToPath(first_file), first_file->row_count));
    first_file = first_file->CopyWithExtraFiles({sidecar});

    ASSERT_NOK_WITH_MSG(state.InstallCommittedSnapshot(-1, {}, store),
                        "invalid deduplicate committed advancement");
    ASSERT_NOK_WITH_MSG(state.InstallCommittedSnapshot(1, {}, delegate),
                        "requires ArrowDeduplicateRealtimeStore");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const KeyOffsetLookup> old_keys,
                         store->AcquireKeyOffsetLookup());
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 old_offsets, old_keys->LookupOffsets(keys));
    ASSERT_TRUE(old_offsets.IsEmpty());

    ASSERT_OK(state.InstallCommittedSnapshot(/*committed_end_offset=*/1, {first_file}, store));
    const std::shared_ptr<RealtimeOffsetFileIndexLookup> installed = state.committed_file_lookup_;
    ASSERT_NE(initial_lookup, installed);
    ASSERT_EQ(initial_lookup->cache_manager_, installed->cache_manager_);
    ASSERT_EQ(options.GetCachePageSize(), installed->cache_page_size_);
    ASSERT_EQ(std::vector<std::shared_ptr<DataFileMeta>>{first_file}, installed->DataFiles());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const KeyOffsetLookup> new_keys,
                         store->AcquireKeyOffsetLookup());
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 new_offsets, new_keys->LookupOffsets(keys));
    ASSERT_EQ("{0}", new_offsets.ToString());
    ASSERT_OK_AND_ASSIGN(old_offsets, old_keys->LookupOffsets(keys));
    ASSERT_TRUE(old_offsets.IsEmpty());

    // A new snapshot may replace its active file set without advancing the durable offset.
    ASSERT_OK(state.InstallCommittedSnapshot(/*committed_end_offset=*/1, {}, store));
    ASSERT_TRUE(state.committed_file_lookup_->DataFiles().empty());
    ASSERT_EQ(installed->cache_manager_, state.committed_file_lookup_->cache_manager_);
    ASSERT_EQ(std::vector<std::shared_ptr<DataFileMeta>>{first_file}, installed->DataFiles());
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 pinned_offsets, new_keys->LookupOffsets(keys));
    ASSERT_EQ("{0}", pinned_offsets.ToString());
}

}  // namespace paimon::test

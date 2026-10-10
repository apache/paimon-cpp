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
#include <vector>

#include "arrow/api.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/realtime/arrow_deduplicate_realtime_store.h"
#include "paimon/core/realtime/arrow_realtime_store.h"
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
    ASSERT_NOK_WITH_MSG(state.AcquireCommittedFileLookup(), "lookup is not attached");
    ASSERT_NOK_WITH_MSG(state.AttachFileIndexLookup(nullptr, store), "cannot attach a null");
    ASSERT_NOK_WITH_MSG(state.AttachFileIndexLookup(initial_lookup, delegate),
                        "requires ArrowDeduplicateRealtimeStore");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> attached,
                         state.AttachFileIndexLookup(initial_lookup, store));
    ASSERT_EQ(initial_lookup, attached);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> existing,
                         state.AttachFileIndexLookup(ignored_lookup, store));
    ASSERT_EQ(initial_lookup, existing);

    ASSERT_NOK_WITH_MSG(state.InstallCommittedSnapshot(-1, {}, store),
                        "invalid deduplicate committed advancement");
    ASSERT_NOK_WITH_MSG(state.InstallCommittedSnapshot(1, {}, delegate),
                        "requires ArrowDeduplicateRealtimeStore");
    ASSERT_OK(state.InstallCommittedSnapshot(/*committed_end_offset=*/1,
                                             /*active_data_files=*/{}, store));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> installed,
                         state.AcquireCommittedFileLookup());
    ASSERT_NE(initial_lookup, installed);
    ASSERT_TRUE(installed->DataFiles().empty());
}

}  // namespace paimon::test

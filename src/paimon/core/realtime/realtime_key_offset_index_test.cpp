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

#include "paimon/core/realtime/realtime_key_offset_index.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

std::shared_ptr<arrow::StructArray> MakeKeys(const std::shared_ptr<arrow::Field>& key_field,
                                             const std::string& json) {
    return checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({key_field}), json).ValueOrDie());
}

std::shared_ptr<arrow::Int64Array> MakeOffsets(const std::string& json) {
    return checked_pointer_cast<arrow::Int64Array>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), json).ValueOrDie());
}

std::shared_ptr<DataFileMeta> MakeDataFile(
    const std::string& file_name, int64_t row_count,
    const std::vector<std::optional<std::string>>& extra_files = {}) {
    return std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/0, row_count, DataFileMeta::EmptyMinKey(),
        DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        DataFileMeta::DUMMY_LEVEL, extra_files, Timestamp(0, 0),
        /*delete_row_count=*/std::nullopt, /*embedded_index=*/nullptr,
        /*file_source=*/std::nullopt, /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt, /*column_max_sequence_numbers=*/std::nullopt);
}

void AssertOffsets(const RoaringBitmap64& offsets, const std::vector<int64_t>& expected) {
    ASSERT_EQ(expected.size(), offsets.Cardinality());
    for (int64_t offset : expected) {
        ASSERT_TRUE(offsets.Contains(offset));
    }
}

}  // namespace

TEST(RealtimeKeyOffsetIndexTest, TestMutableIndexSnapshotIsCopyOnWrite) {
    const std::shared_ptr<arrow::Field> key_field = arrow::field("id", arrow::int64());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MutableKeyOffsetIndex> index,
                         MutableKeyOffsetIndex::Create(key_field, GetDefaultPool()));
    ASSERT_OK(index->Add(MakeKeys(key_field, R"([[1], [2]])"), MakeOffsets("[10, 20]")));
    const std::shared_ptr<const KeyOffsetLookup> snapshot = index->Snapshot();

    ASSERT_OK(index->Add(MakeKeys(key_field, R"([[1]])"), MakeOffsets("[30]")));
    ASSERT_OK(index->Erase(MakeKeys(key_field, R"([[2]])")));

    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 current,
                         index->LookupOffsets(MakeKeys(key_field, R"([[1], [2], [3]])")));
    AssertOffsets(current, {30});
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 pinned,
                         snapshot->LookupOffsets(MakeKeys(key_field, R"([[1], [2], [3]])")));
    AssertOffsets(pinned, {10, 20});
}

TEST(RealtimeKeyOffsetIndexTest, TestMutableIndexValidatesInput) {
    const std::shared_ptr<arrow::Field> key_field = arrow::field("id", arrow::int64());
    ASSERT_NOK_WITH_MSG(MutableKeyOffsetIndex::Create(nullptr, GetDefaultPool()),
                        "requires a key field and memory pool");
    ASSERT_NOK_WITH_MSG(MutableKeyOffsetIndex::Create(key_field, nullptr),
                        "requires a key field and memory pool");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MutableKeyOffsetIndex> index,
                         MutableKeyOffsetIndex::Create(key_field, GetDefaultPool()));

    ASSERT_NOK_WITH_MSG(index->Add(MakeKeys(key_field, R"([[1], [2]])"), MakeOffsets("[10]")),
                        "input columns are not aligned");
    ASSERT_NOK_WITH_MSG(index->Add(MakeKeys(key_field, R"([[1]])"), MakeOffsets("[-1]")),
                        "offset must not be negative");
    ASSERT_NOK_WITH_MSG(
        index->LookupOffsets(MakeKeys(arrow::field("id", arrow::utf8()), R"([["1"]])")),
        "exactly the configured key field");
}

TEST(RealtimeKeyOffsetIndexTest, TestCompositeLookupUnionsChildren) {
    const std::shared_ptr<arrow::Field> key_field = arrow::field("id", arrow::int64());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MutableKeyOffsetIndex> first,
                         MutableKeyOffsetIndex::Create(key_field, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MutableKeyOffsetIndex> second,
                         MutableKeyOffsetIndex::Create(key_field, GetDefaultPool()));
    ASSERT_OK(first->Add(MakeKeys(key_field, R"([[1]])"), MakeOffsets("[10]")));
    ASSERT_OK(second->Add(MakeKeys(key_field, R"([[1], [2]])"), MakeOffsets("[30, 20]")));
    std::vector<std::shared_ptr<const KeyOffsetLookup>> children = {first->Snapshot(), nullptr,
                                                                    second->Snapshot()};
    CompositeKeyOffsetLookup composite(std::move(children));

    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 result,
                         composite.LookupOffsets(MakeKeys(key_field, R"([[1], [2], [3]])")));
    AssertOffsets(result, {10, 20, 30});
}

TEST(RealtimeKeyOffsetIndexTest, TestSidecarRoundTripAndAbort) {
    std::unique_ptr<UniqueTestDirectory> directory = UniqueTestDirectory::Create();
    ASSERT_NE(nullptr, directory);
    const std::shared_ptr<arrow::Field> key_field = arrow::field("id", arrow::int64());
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(directory->Str(), /*format_identifier=*/"parquet",
                                 /*data_file_prefix=*/"data-",
                                 /*external_path_provider=*/nullptr));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DataFileKeyOffsetIndexWriter> writer,
        DataFileKeyOffsetIndexWriter::Create(key_field, path_factory, directory->GetFileSystem(),
                                             GetDefaultPool(), /*options=*/{}));
    ASSERT_OK(writer->AddBatch(MakeKeys(key_field, R"([[1], [2]])"), MakeOffsets("[7, 9]")));
    ASSERT_NOK_WITH_MSG(writer->AddBatch(MakeKeys(key_field, R"([[2]])"), MakeOffsets("[11]")),
                        "more than one offset for a user-defined key");

    std::shared_ptr<DataFileMeta> data_file = MakeDataFile("data-0.parquet", /*row_count=*/2);
    ASSERT_OK_AND_ASSIGN(std::string sidecar, writer->Finish(data_file));
    ASSERT_EQ("data-0.parquet.offset", sidecar);
    data_file->extra_files.emplace_back(sidecar);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileKeyOffsetIndexReader> reader,
        DataFileKeyOffsetIndexReader::Create(key_field, data_file, path_factory,
                                             directory->GetFileSystem(), GetDefaultPool(), {}));
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 result,
                         reader->LookupOffsets(MakeKeys(key_field, R"([[2], [3], [1]])")));
    AssertOffsets(result, {7, 9});

    const std::string sidecar_path = path_factory->ToAlignedPath(sidecar, data_file);
    ASSERT_OK_AND_ASSIGN(bool exists, directory->GetFileSystem()->Exists(sidecar_path));
    ASSERT_TRUE(exists);
    writer->Abort();
    ASSERT_OK_AND_ASSIGN(exists, directory->GetFileSystem()->Exists(sidecar_path));
    ASSERT_FALSE(exists);
    ASSERT_OK_AND_ASSIGN(result, reader->LookupOffsets(MakeKeys(key_field, R"([[1], [2]])")));
    AssertOffsets(result, {7, 9});
}

TEST(RealtimeKeyOffsetIndexTest, TestSidecarValidation) {
    std::unique_ptr<UniqueTestDirectory> directory = UniqueTestDirectory::Create();
    ASSERT_NE(nullptr, directory);
    const std::shared_ptr<arrow::Field> key_field = arrow::field("id", arrow::int64());
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(directory->Str(), /*format_identifier=*/"parquet",
                                 /*data_file_prefix=*/"data-",
                                 /*external_path_provider=*/nullptr));

    ASSERT_NOK_WITH_MSG(DataFileKeyOffsetIndexWriter::Create(
                            key_field, nullptr, directory->GetFileSystem(), GetDefaultPool(), {}),
                        "missing a required dependency");
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DataFileKeyOffsetIndexWriter> writer,
        DataFileKeyOffsetIndexWriter::Create(key_field, path_factory, directory->GetFileSystem(),
                                             GetDefaultPool(), /*options=*/{}));
    ASSERT_NOK_WITH_MSG(writer->Finish(MakeDataFile("empty.parquet", /*row_count=*/0)),
                        "cannot finish an empty key-offset index");

    ASSERT_NOK_WITH_MSG(DataFileKeyOffsetIndexReader::Create(
                            key_field, MakeDataFile("missing.parquet", /*row_count=*/1),
                            path_factory, directory->GetFileSystem(), GetDefaultPool(), {}),
                        "does not contain the required .offset sidecar");
    ASSERT_NOK_WITH_MSG(
        DataFileKeyOffsetIndexReader::Create(
            key_field,
            MakeDataFile("duplicate.parquet", /*row_count=*/1, {"first.offset", "second.offset"}),
            path_factory, directory->GetFileSystem(), GetDefaultPool(), {}),
        "contains multiple .offset sidecars");
}

}  // namespace paimon::test

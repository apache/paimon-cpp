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
#include "paimon/common/global_index/key_serializer.h"
#include "paimon/common/io/cache/cache_manager.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/io/data_file_key_offset_index_writer.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/key_offset_index_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"
#include "xxhash.h"  // NOLINT(build/include_subdir)

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

std::shared_ptr<arrow::StructArray> MakeIndexBatch(const std::shared_ptr<arrow::Field>& key_field,
                                                   const std::string& json) {
    return checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(
                {key_field, arrow::field(SpecialFields::RealtimeOffset().Name(), arrow::int64())}),
            json)
            .ValueOrDie());
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

    const std::shared_ptr<const KeyOffsetLookup> deleted_snapshot = index->Snapshot();
    ASSERT_OK(index->Add(MakeKeys(key_field, R"([[2]])"), MakeOffsets("[40]")));
    ASSERT_OK(index->Erase(MakeKeys(key_field, R"([[99]])")));
    ASSERT_OK_AND_ASSIGN(current, index->LookupOffsets(MakeKeys(key_field, R"([[1], [2]])")));
    AssertOffsets(current, {30, 40});
    ASSERT_OK_AND_ASSIGN(pinned, deleted_snapshot->LookupOffsets(MakeKeys(key_field, R"([[2]])")));
    ASSERT_TRUE(pinned.IsEmpty());
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
    ASSERT_NOK_WITH_MSG(index->Add(MakeKeys(key_field, R"([[null]])"), MakeOffsets("[10]")),
                        "deduplicate key must not contain null");
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
    auto cache_manager = std::make_shared<CacheManager>(128, /*high_priority_pool_ratio=*/0.0);
    std::unique_ptr<UniqueTestDirectory> directory = UniqueTestDirectory::Create();
    ASSERT_NE(nullptr, directory);
    const std::shared_ptr<arrow::Field> key_field = arrow::field("id", arrow::int64());
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(directory->Str(), /*format_identifier=*/"parquet",
                                 /*data_file_prefix=*/"data-",
                                 /*external_path_provider=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<DataFileKeyOffsetIndexWriter> writer,
                         DataFileKeyOffsetIndexWriter::Create(key_field, directory->GetFileSystem(),
                                                              GetDefaultPool()));
    ASSERT_OK(writer->AddBatch(MakeIndexBatch(key_field, R"([[1, 7], [2, 9]])")));
    ASSERT_NOK_WITH_MSG(writer->AddBatch(MakeIndexBatch(key_field, R"([[2, 11]])")),
                        "more than one offset for a deduplicate key");

    std::shared_ptr<DataFileMeta> data_file = MakeDataFile("data-0.parquet", /*row_count=*/2);
    ASSERT_OK_AND_ASSIGN(std::string sidecar,
                         writer->Finish(path_factory->ToPath(data_file), data_file->row_count));
    ASSERT_EQ("data-0.parquet.offset", sidecar);
    data_file->extra_files.emplace_back(sidecar);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileKeyOffsetIndexReader> reader,
                         DataFileKeyOffsetIndexReader::Create(
                             /*page_size=*/32, cache_manager, key_field, data_file, path_factory,
                             directory->GetFileSystem(), GetDefaultPool()));
    const std::shared_ptr<LruCache> cache =
        checked_pointer_cast<LruCache>(cache_manager->IndexCache());
    ASSERT_EQ(0, cache->GetCurrentWeight());
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 result,
                         reader->LookupOffsets(MakeKeys(key_field, R"([[2], [3], [1]])")));
    AssertOffsets(result, {7, 9});
    ASSERT_GT(cache->GetCurrentWeight(), 0);
    ASSERT_LE(cache->GetCurrentWeight(), 128);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileKeyOffsetIndexReader> reopened,
                         DataFileKeyOffsetIndexReader::Create(
                             /*page_size=*/32, cache_manager, key_field, data_file, path_factory,
                             directory->GetFileSystem(), GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(result, reopened->LookupOffsets(MakeKeys(key_field, R"([[1], [2]])")));
    AssertOffsets(result, {7, 9});

    const std::string sidecar_path = path_factory->ToAlignedPath(sidecar, data_file);
    ASSERT_OK_AND_ASSIGN(bool exists, directory->GetFileSystem()->Exists(sidecar_path));
    ASSERT_TRUE(exists);
    writer->Abort();
    ASSERT_OK_AND_ASSIGN(exists, directory->GetFileSystem()->Exists(sidecar_path));
    ASSERT_FALSE(exists);
}

TEST(RealtimeKeyOffsetIndexTest, TestSidecarRoundTripWithEmptyStringKey) {
    auto cache_manager = std::make_shared<CacheManager>(128, /*high_priority_pool_ratio=*/0.0);
    std::unique_ptr<UniqueTestDirectory> directory = UniqueTestDirectory::Create();
    ASSERT_NE(nullptr, directory);
    const std::shared_ptr<arrow::Field> key_field = arrow::field("id", arrow::utf8());
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(directory->Str(), /*format_identifier=*/"parquet",
                                 /*data_file_prefix=*/"data-",
                                 /*external_path_provider=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<DataFileKeyOffsetIndexWriter> writer,
                         DataFileKeyOffsetIndexWriter::Create(key_field, directory->GetFileSystem(),
                                                              GetDefaultPool()));
    ASSERT_OK(writer->AddBatch(MakeIndexBatch(key_field, R"([["", 7], ["alpha", 9]])")));

    std::shared_ptr<DataFileMeta> data_file = MakeDataFile("data-0.parquet", /*row_count=*/2);
    ASSERT_OK_AND_ASSIGN(std::string sidecar,
                         writer->Finish(path_factory->ToPath(data_file), data_file->row_count));
    data_file->extra_files.emplace_back(sidecar);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileKeyOffsetIndexReader> reader,
                         DataFileKeyOffsetIndexReader::Create(
                             /*page_size=*/32, cache_manager, key_field, data_file, path_factory,
                             directory->GetFileSystem(), GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 empty_key_offset,
                         reader->LookupOffsets(MakeKeys(key_field, R"([[""]])")));
    AssertOffsets(empty_key_offset, {7});
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 nonempty_key_offset,
                         reader->LookupOffsets(MakeKeys(key_field, R"([["alpha"]])")));
    AssertOffsets(nonempty_key_offset, {9});
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 missing_key_offset,
                         reader->LookupOffsets(MakeKeys(key_field, R"([["missing"]])")));
    ASSERT_TRUE(missing_key_offset.IsEmpty());
}

TEST(RealtimeKeyOffsetIndexTest, TestSidecarAcceptsDictionaryEncodedLogicalBatch) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    const std::shared_ptr<arrow::Field> key_field = arrow::field("id", arrow::utf8());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<DataFileKeyOffsetIndexWriter> writer,
                         DataFileKeyOffsetIndexWriter::Create(key_field, directory->GetFileSystem(),
                                                              GetDefaultPool()));
    const std::shared_ptr<arrow::Array> indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 0]").ValueOrDie();
    const std::shared_ptr<arrow::Array> dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b"])").ValueOrDie();
    const std::shared_ptr<arrow::Array> encoded =
        arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::utf8()),
                                           indices, dictionary)
            .ValueOrDie();
    const std::shared_ptr<arrow::StructArray> logical_batch =
        arrow::StructArray::Make(
            {encoded, MakeOffsets("[100, 200]")},
            std::vector<std::string>{"id", SpecialFields::RealtimeOffset().Name()})
            .ValueOrDie();
    ASSERT_OK(writer->AddBatch(logical_batch));
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(directory->Str(), "parquet", "data-", nullptr));
    std::shared_ptr<DataFileMeta> data_file = MakeDataFile("dictionary.parquet", 2);
    ASSERT_OK_AND_ASSIGN(std::string sidecar,
                         writer->Finish(path_factory->ToPath(data_file), data_file->row_count));
    data_file = data_file->CopyWithExtraFiles({sidecar});
    auto cache_manager = std::make_shared<CacheManager>(128, 0.0);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileKeyOffsetIndexReader> reader,
        DataFileKeyOffsetIndexReader::Create(32, cache_manager, key_field, data_file, path_factory,
                                             directory->GetFileSystem(), GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 offsets,
                         reader->LookupOffsets(MakeKeys(key_field, R"([["a"]])")));
    AssertOffsets(offsets, {200});
    ASSERT_OK_AND_ASSIGN(offsets, reader->LookupOffsets(MakeKeys(key_field, R"([["b"]])")));
    AssertOffsets(offsets, {100});
}

TEST(RealtimeKeyOffsetIndexTest, TestSidecarValidation) {
    auto cache_manager = std::make_shared<CacheManager>(128, /*high_priority_pool_ratio=*/0.0);
    std::unique_ptr<UniqueTestDirectory> directory = UniqueTestDirectory::Create();
    ASSERT_NE(nullptr, directory);
    const std::shared_ptr<arrow::Field> key_field = arrow::field("id", arrow::int64());
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(directory->Str(), /*format_identifier=*/"parquet",
                                 /*data_file_prefix=*/"data-",
                                 /*external_path_provider=*/nullptr));

    ASSERT_NOK_WITH_MSG(DataFileKeyOffsetIndexWriter::Create(key_field, nullptr, GetDefaultPool()),
                        "missing a required dependency");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<DataFileKeyOffsetIndexWriter> writer,
                         DataFileKeyOffsetIndexWriter::Create(key_field, directory->GetFileSystem(),
                                                              GetDefaultPool()));
    ASSERT_NOK_WITH_MSG(writer->Finish(path_factory->ToPath("empty.parquet"), /*row_count=*/0),
                        "cannot finish an empty key-offset index");

    ASSERT_NOK_WITH_MSG(DataFileKeyOffsetIndexReader::Create(
                            /*page_size=*/32, cache_manager, key_field,
                            MakeDataFile("missing.parquet", /*row_count=*/1), path_factory,
                            directory->GetFileSystem(), GetDefaultPool()),
                        "does not contain the required .offset sidecar");
    ASSERT_NOK_WITH_MSG(
        DataFileKeyOffsetIndexReader::Create(
            /*page_size=*/32, cache_manager, key_field,
            MakeDataFile("duplicate.parquet", /*row_count=*/1, {"first.offset", "second.offset"}),
            path_factory, directory->GetFileSystem(), GetDefaultPool()),
        "contains multiple .offset sidecars");

    ASSERT_OK(writer->AddBatch(MakeIndexBatch(key_field, R"([[1, 0]])")));
    std::shared_ptr<DataFileMeta> data_file = MakeDataFile("one.parquet", /*row_count=*/1);
    ASSERT_OK_AND_ASSIGN(std::string sidecar,
                         writer->Finish(path_factory->ToPath(data_file), data_file->row_count));
    data_file = MakeDataFile("one.parquet", /*row_count=*/2, {sidecar});
    ASSERT_NOK_WITH_MSG(
        DataFileKeyOffsetIndexReader::Create(32, cache_manager, key_field, data_file, path_factory,
                                             directory->GetFileSystem(), GetDefaultPool()),
        "entry count does not match data file");
}

TEST(RealtimeKeyOffsetIndexTest, TestSidecarRejectsNegativeOffsetOnLookup) {
    std::unique_ptr<UniqueTestDirectory> directory = UniqueTestDirectory::Create();
    ASSERT_NE(nullptr, directory);
    const std::shared_ptr<arrow::Field> key_field = arrow::field("id", arrow::int64());
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(directory->Str(), /*format_identifier=*/"parquet",
                                 /*data_file_prefix=*/"data-", /*external_path_provider=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<KeySerializer> serializer,
                         KeySerializer::Create(key_field->type(), GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> serialized,
                         serializer->Serialize(Literal(int64_t{1})));
    using HashTable = DenseHashTable<uint64_t, int64_t>;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashTable> hash_table,
                         HashTable::Create(1, /*occupancy_pct=*/80, GetDefaultPool()));
    ASSERT_OK(hash_table->Insert(XXH64(serialized->data(), serialized->size(), /*seed=*/0), -1));
    const std::shared_ptr<DataFileMeta> data_file =
        MakeDataFile("negative.parquet", /*row_count=*/1, {"negative.parquet.offset"});
    const std::string path = path_factory->ToAlignedPath("negative.parquet.offset", data_file);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> output,
                         directory->GetFileSystem()->Create(path, /*overwrite=*/false));
    DataOutputStream data_output(output);
    ASSERT_OK(data_output.WriteValue<int32_t>(KeyOffsetIndexUtils::kCurrentVersion));
    ASSERT_OK(hash_table->WriteTo(output.get()));
    ASSERT_OK(output->Flush());
    ASSERT_OK(output->Close());

    auto cache_manager = std::make_shared<CacheManager>(128, /*high_priority_pool_ratio=*/0.0);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileKeyOffsetIndexReader> reader,
        DataFileKeyOffsetIndexReader::Create(32, cache_manager, key_field, data_file, path_factory,
                                             directory->GetFileSystem(), GetDefaultPool()));
    // Opening checks the header only. Invalid values are rejected when their buckets are read.
    ASSERT_NOK_WITH_MSG(reader->LookupOffsets(MakeKeys(key_field, R"([[1]])")),
                        "negative offset in realtime .offset index");
}

}  // namespace paimon::test

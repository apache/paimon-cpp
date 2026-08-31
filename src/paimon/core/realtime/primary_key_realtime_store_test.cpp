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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/realtime/primary_key_realtime_store.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/realtime/realtime_primary_key_reader.h"
#include "paimon/macros.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/realtime/arrow_realtime_store_factory.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

std::shared_ptr<arrow::Field> FieldWithId(const std::string& name,
                                          const std::shared_ptr<arrow::DataType>& type,
                                          int32_t field_id, bool nullable = true) {
    return DataField::ConvertDataFieldToArrowField(
               DataField(field_id, arrow::field(name, type, nullable)))
        ->WithNullable(nullable);
}

std::shared_ptr<arrow::Schema> TransportSchema() {
    return RealtimePrimaryKeyLayout::CreateSchema(
        {FieldWithId("id", arrow::int64(), 0), FieldWithId("value", arrow::utf8(), 1)});
}

std::shared_ptr<arrow::Schema> NestedTransportSchema() {
    return RealtimePrimaryKeyLayout::CreateSchema(
        {FieldWithId("id", arrow::int64(), 0),
         FieldWithId("value",
                     arrow::struct_({arrow::field("name", arrow::utf8()),
                                     arrow::field("items", arrow::list(arrow::int32()))}),
                     1)});
}

std::unique_ptr<RecordBatch> MakeBatch(const std::string& json) {
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(TransportSchema()->fields()), json)
            .ValueOrDie();
    auto c_array = std::make_unique<ArrowArray>();
    EXPECT_TRUE(arrow::ExportArray(*array, c_array.get()).ok());
    return RecordBatchBuilder(c_array.get()).Finish().value();
}

std::unique_ptr<RecordBatch> MakeSlicedBatch(const std::shared_ptr<arrow::Schema>& schema,
                                             const std::string& json, int64_t offset,
                                             int64_t length) {
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), json)
            .ValueOrDie()
            ->Slice(offset, length);
    auto c_array = std::make_unique<ArrowArray>();
    EXPECT_TRUE(arrow::ExportArray(*array, c_array.get()).ok());
    return RecordBatchBuilder(c_array.get()).Finish().value();
}

void AssertOffsetsZero(const ArrowArray* array) {
    ASSERT_NE(nullptr, array);
    ASSERT_EQ(0, array->offset);
    for (int64_t child = 0; child < array->n_children; ++child) {
        AssertOffsetsZero(array->children[child]);
    }
    if (array->dictionary) {
        AssertOffsetsZero(array->dictionary);
    }
}

Result<std::string> ReadJson(std::vector<std::unique_ptr<BatchReader>> readers) {
    std::vector<std::shared_ptr<arrow::Array>> batches;
    for (std::unique_ptr<BatchReader>& reader : readers) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> result,
                               ReadResultCollector::CollectResult(std::move(reader)));
        if (result) {
            batches.insert(batches.end(), result->chunks().begin(), result->chunks().end());
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> result,
                                      arrow::Concatenate(batches));
    return result->ToString();
}

TEST(PrimaryKeyRealtimeStoreTest, TestWriteAndSealValidation) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(TransportSchema(), GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_FALSE(segment.has_value());
    ASSERT_NOK_WITH_MSG(store->Write(RealtimeWriteBatch{nullptr, OffsetRange(0, 0)}),
                        "write batch is null");
    ASSERT_NOK_WITH_MSG(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 1, 0, 1, "one"]])"), OffsetRange(0, 0)}),
        "offset range does not match batch row count");

    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeBatch(R"([[0, 1, 0, 1, "one"], [0, 2, 1, 2, "two"]])"), OffsetRange(0, 2)}));
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 3, 2, 3, "three"]])"), OffsetRange(2, 3)}));

    ASSERT_OK_AND_ASSIGN(segment, store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(OffsetRange(0, 3), segment.value()->GetOffsetRange());
    ASSERT_GT(store->GetMemoryUsage(), 0);
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 4, 3, 4, "four"]])"), OffsetRange(3, 4)}));
}

TEST(PrimaryKeyRealtimeStoreTest, TestCommitReaderPerStoredBatch) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(TransportSchema(), GetDefaultPool()));
    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeBatch(R"([[1, 6, 1, 1, "before"], [0, 5, 0, 3, "three"]])"), OffsetRange(0, 2)}));
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[2, 7, 2, 2, "after"]])"), OffsetRange(2, 3)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateCommitReaders(segment.value()));
    ASSERT_EQ(2, readers.size());
    ASSERT_OK_AND_ASSIGN(std::string actual, ReadJson(std::move(readers)));
    ASSERT_EQ(
        "-- is_valid: all not null\n-- child 0 type: int8\n  [\n    1,\n    0,\n    2\n  ]\n-- "
        "child 1 type: int64\n  [\n    6,\n    5,\n    7\n  ]\n-- child 2 type: int64\n  [\n    "
        "1,\n    0,\n    2\n  ]\n-- child 3 type: int64\n  [\n    1,\n    3,\n    2\n  ]\n-- child "
        "4 type: string\n  [\n    \"before\",\n    \"three\",\n    \"after\"\n  ]",
        actual);
}

void AssertSlicedBatch(BatchReader* reader) {
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    ASSERT_EQ(2, batch.first->length);
    AssertOffsetsZero(batch.first.get());
    arrow::Result<std::shared_ptr<arrow::Array>> import_result =
        arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
    std::shared_ptr<arrow::Array> array = std::move(import_result).ValueOrDie();
    std::shared_ptr<arrow::StructArray> values = checked_pointer_cast<arrow::StructArray>(array);
    ASSERT_EQ(2, checked_pointer_cast<arrow::Int64Array>(values->field(3))->Value(0));
    ASSERT_EQ(3, checked_pointer_cast<arrow::Int64Array>(values->field(3))->Value(1));
    std::shared_ptr<arrow::StructArray> nested =
        checked_pointer_cast<arrow::StructArray>(values->field(4));
    ASSERT_EQ("two", checked_pointer_cast<arrow::StringArray>(nested->field(0))->GetString(0));
    ASSERT_EQ("three", checked_pointer_cast<arrow::StringArray>(nested->field(0))->GetString(1));
    std::shared_ptr<arrow::ListArray> items =
        checked_pointer_cast<arrow::ListArray>(nested->field(1));
    std::shared_ptr<arrow::Int32Array> first_items =
        checked_pointer_cast<arrow::Int32Array>(items->value_slice(0));
    ASSERT_EQ(3, first_items->Value(0));
    ASSERT_EQ(4, first_items->Value(1));
    std::shared_ptr<arrow::Int32Array> second_items =
        checked_pointer_cast<arrow::Int32Array>(items->value_slice(1));
    ASSERT_EQ(5, second_items->Value(0));
    ASSERT_EQ(6, second_items->Value(1));
    ASSERT_OK_AND_ASSIGN(batch, reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
}

TEST(PrimaryKeyRealtimeStoreTest, TestSlicedReadersExportZeroOffsets) {
    std::shared_ptr<arrow::Schema> schema = NestedTransportSchema();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(schema, GetDefaultPool()));
    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeSlicedBatch(
            schema,
            R"([[0, 1, 0, 1, ["one", [1, 2]]], [0, 2, 1, 2, ["two", [3, 4]]], [0, 3, 2, 3, ["three", [5, 6]]], [0, 4, 3, 4, ["four", [7, 8]]]])",
            1, 2),
        OffsetRange(0, 2)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateCommitReaders(segment.value()));
    ASSERT_EQ(1, readers.size());
    AssertSlicedBatch(readers[0].get());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
    RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr};
    ASSERT_OK_AND_ASSIGN(readers, store->CreateQueryReaders(view, context));
    ASSERT_EQ(1, readers.size());
    AssertSlicedBatch(readers[0].get());
}

TEST(PrimaryKeyRealtimeStoreTest, TestReclaimKeepsReadView) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(TransportSchema(), GetDefaultPool()));
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 0, 4, 1, "one"]])"), OffsetRange(4, 5)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 1, 5, 2, "two"]])"), OffsetRange(5, 6)}));
    ASSERT_OK_AND_ASSIGN(segment, store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 2, 6, 3, "three"]])"), OffsetRange(6, 7)}));
    ASSERT_OK_AND_ASSIGN(segment, store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> retained_view, store->AcquireReadView());
    ASSERT_EQ(OffsetRange(4, 7), retained_view->GetOffsetRange());

    const uint64_t initial_memory_usage = store->GetMemoryUsage();
    ASSERT_GT(initial_memory_usage, 0);
    ASSERT_OK(store->AdvanceCommittedOffset(4));
    ASSERT_EQ(initial_memory_usage, store->GetMemoryUsage());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> current_view, store->AcquireReadView());
    ASSERT_EQ(OffsetRange(4, 7), current_view->GetOffsetRange());
    ASSERT_OK(store->AdvanceCommittedOffset(5));
    ASSERT_LT(store->GetMemoryUsage(), initial_memory_usage);
    ASSERT_OK_AND_ASSIGN(current_view, store->AcquireReadView());
    ASSERT_EQ(OffsetRange(5, 7), current_view->GetOffsetRange());
    ASSERT_OK(store->AdvanceCommittedOffset(6));
    ASSERT_OK_AND_ASSIGN(current_view, store->AcquireReadView());
    ASSERT_EQ(OffsetRange(6, 7), current_view->GetOffsetRange());
    ASSERT_OK(store->AdvanceCommittedOffset(7));
    ASSERT_EQ(0, store->GetMemoryUsage());
    ASSERT_OK_AND_ASSIGN(current_view, store->AcquireReadView());
    ASSERT_FALSE(current_view->GetOffsetRange().has_value());

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*TransportSchema(), c_schema.get()).ok());
    RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(retained_view, context));
    ASSERT_OK_AND_ASSIGN(std::string actual, ReadJson(std::move(readers)));
    ASSERT_NE(std::string::npos, actual.find("\"one\""));
    ASSERT_NE(std::string::npos, actual.find("\"two\""));
    ASSERT_NE(std::string::npos, actual.find("\"three\""));
}

TEST(PrimaryKeyRealtimeStoreTest, TestQueryReaderPerStoredBatch) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(TransportSchema(), GetDefaultPool()));
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 1, 0, 2, "two"]])"), OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 2, 1, 1, "one"]])"), OffsetRange(1, 2)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*TransportSchema(), c_schema.get()).ok());
    RealtimeQueryContext context{/*read_schema=*/c_schema.get(), /*predicate=*/nullptr};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, context));
    ASSERT_EQ(2, readers.size());
    ASSERT_OK_AND_ASSIGN(std::string actual, ReadJson(std::move(readers)));
    ASSERT_NE(std::string::npos, actual.find("\"one\""));
    ASSERT_NE(std::string::npos, actual.find("\"two\""));
}

TEST(PrimaryKeyRealtimeStoreTest, TestQueryBatchOutlivesStoreAndReader) {
    const std::shared_ptr<arrow::Schema> stored_schema = TransportSchema();
    std::shared_ptr<MemoryPool> pool = GetMemoryPool();
    std::weak_ptr<MemoryPool> pool_lifetime = pool;
    auto write_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*stored_schema, write_schema.get()).ok());
    ArrowRealtimeStoreFactory factory;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeStore> store,
                         factory.Create(RealtimeStoreCreateRequest{
                             std::move(write_schema),
                             /*options=*/{}, pool, RealtimeStoreMode::PRIMARY_KEY}));
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 1, 0, 7, "seven"]])"), OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());

    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*stored_schema, c_schema.get()).ok());
    RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, context));
    ASSERT_EQ(1, readers.size());
    view.reset();
    store.reset();
    pool.reset();
    ASSERT_FALSE(pool_lifetime.expired());

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    readers.clear();
    ASSERT_FALSE(pool_lifetime.expired());

    arrow::Result<std::shared_ptr<arrow::Array>> import_result =
        arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
    std::shared_ptr<arrow::Array> imported = std::move(import_result).ValueOrDie();
    ASSERT_FALSE(pool_lifetime.expired());
    imported.reset();
    ASSERT_TRUE(pool_lifetime.expired());
}

TEST(PrimaryKeyRealtimeStoreTest, TestQueryReaderProjectsNestedFields) {
    const std::shared_ptr<arrow::Field> stored_profile_a =
        FieldWithId("profile_a", arrow::int32(), 30);
    const std::shared_ptr<arrow::Field> stored_a = FieldWithId("a", arrow::int32(), 10);
    const std::shared_ptr<arrow::Field> stored_b = FieldWithId("b", arrow::int32(), 11);
    const std::shared_ptr<arrow::Field> stored_x = FieldWithId("x", arrow::int32(), 20);
    const std::shared_ptr<arrow::Field> stored_y = FieldWithId("y", arrow::int32(), 21);
    arrow::FieldVector stored_value_fields = {
        FieldWithId("id", arrow::int64(), 0),
        FieldWithId("profile", arrow::struct_({stored_profile_a}), 1),
        FieldWithId("items", arrow::list(arrow::struct_({stored_a, stored_b})), 2),
        FieldWithId("attrs", arrow::map(arrow::utf8(), arrow::struct_({stored_x, stored_y})), 3)};
    std::shared_ptr<arrow::Schema> stored_schema =
        RealtimePrimaryKeyLayout::CreateSchema(stored_value_fields);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(stored_schema, GetDefaultPool()));
    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeSlicedBatch(
            stored_schema,
            R"([[0, 1, 0, 6, [5], [[1, 2]], [["before", [3, 4]]]], [0, 2, 1, 7, [50], [[100, 200], null], [["k1", [7, 8]], ["k2", null]]], [0, 3, 2, 8, [500], [[9, 10]], [["after", [11, 12]]]]])",
            1, 1),
        OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());

    arrow::FieldVector requested_value_fields;
    requested_value_fields.push_back(FieldWithId("profile", arrow::struct_({stored_profile_a}), 1));
    requested_value_fields.push_back(
        FieldWithId("items", arrow::list(arrow::struct_({stored_b, stored_a})), 2));
    requested_value_fields.push_back(
        FieldWithId("attrs", arrow::map(arrow::utf8(), arrow::struct_({stored_y, stored_x})), 3));
    std::shared_ptr<arrow::Schema> requested_schema =
        RealtimePrimaryKeyLayout::CreateSchema(requested_value_fields);
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*requested_schema, c_schema.get()).ok());
    RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, context));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    readers.clear();
    arrow::Result<std::shared_ptr<arrow::Array>> import_result =
        arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
    std::shared_ptr<arrow::Array> array = std::move(import_result).ValueOrDie();
    ASSERT_TRUE(array->type()->Equals(arrow::struct_(requested_schema->fields())));
    std::shared_ptr<arrow::StructArray> projected = checked_pointer_cast<arrow::StructArray>(array);
    const std::shared_ptr<arrow::StructArray> profile =
        checked_pointer_cast<arrow::StructArray>(projected->field(3));
    ASSERT_EQ(50, checked_pointer_cast<arrow::Int32Array>(profile->field(0))->Value(0));
    const std::shared_ptr<arrow::ListArray> items =
        checked_pointer_cast<arrow::ListArray>(projected->field(4));
    const std::shared_ptr<arrow::StructArray> item_values =
        checked_pointer_cast<arrow::StructArray>(items->value_slice(0));
    ASSERT_EQ(200, checked_pointer_cast<arrow::Int32Array>(item_values->field(0))->Value(0));
    ASSERT_EQ(100, checked_pointer_cast<arrow::Int32Array>(item_values->field(1))->Value(0));
    ASSERT_TRUE(item_values->IsNull(1));

    const std::shared_ptr<arrow::MapArray> attrs =
        checked_pointer_cast<arrow::MapArray>(projected->field(5));
    const int64_t attr_offset = attrs->value_offset(0);
    const int64_t attr_length = attrs->value_length(0);
    const std::shared_ptr<arrow::StringArray> attr_keys =
        checked_pointer_cast<arrow::StringArray>(attrs->keys()->Slice(attr_offset, attr_length));
    ASSERT_EQ("k1", attr_keys->GetString(0));
    const std::shared_ptr<arrow::StructArray> attr_values =
        checked_pointer_cast<arrow::StructArray>(attrs->items()->Slice(attr_offset, attr_length));
    ASSERT_EQ(8, checked_pointer_cast<arrow::Int32Array>(attr_values->field(0))->Value(0));
    ASSERT_EQ(7, checked_pointer_cast<arrow::Int32Array>(attr_values->field(1))->Value(0));
    ASSERT_TRUE(attr_values->IsNull(1));
}

}  // namespace
}  // namespace paimon::test

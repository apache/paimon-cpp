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

#include "paimon/core/realtime/realtime_primary_key_reader.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/realtime/realtime_store_read_pipeline.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/realtime/offset_range.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

std::shared_ptr<arrow::Field> MakeField(const std::string& name,
                                        const std::shared_ptr<arrow::DataType>& type,
                                        int32_t field_id, bool nullable = true) {
    return DataField::ConvertDataFieldToArrowField(
        DataField(field_id, arrow::field(name, type, nullable)));
}

std::shared_ptr<arrow::Schema> MakeTransportSchema(const arrow::FieldVector& value_fields) {
    return RealtimePrimaryKeyLayout::CreateWriteSchema(value_fields);
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>>
CreateRealtimePrimaryKeyQueryReadersForTest(std::vector<std::unique_ptr<BatchReader>>&& readers,
                                            const OffsetRange& visible_offsets,
                                            const std::shared_ptr<arrow::Schema>& key_schema,
                                            const std::shared_ptr<arrow::Schema>& value_schema,
                                            const std::shared_ptr<MemoryPool>& memory_pool) {
    std::shared_ptr<arrow::Schema> write_schema = MakeTransportSchema(value_schema->fields());
    std::shared_ptr<arrow::Schema> logical_schema =
        RealtimePrimaryKeyLayout::CreateLogicalSchema(value_schema->fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<RealtimeStoreReadPipeline> pipeline,
        RealtimeStoreReadPipeline::Create(logical_schema, write_schema, memory_pool));
    return RealtimePrimaryKeyReaderFactory::CreateForQuery(
        std::move(readers), visible_offsets, key_schema, value_schema, memory_pool, *pipeline);
}

Result<std::unique_ptr<KeyValueRecordReader>> CreateRealtimePrimaryKeyQueryReaderForTest(
    std::unique_ptr<BatchReader>&& reader, const OffsetRange& visible_offsets,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.push_back(std::move(reader));
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::unique_ptr<KeyValueRecordReader>> adapted_readers,
        CreateRealtimePrimaryKeyQueryReadersForTest(std::move(readers), visible_offsets, key_schema,
                                                    value_schema, memory_pool));
    return std::move(adapted_readers[0]);
}

Result<std::unique_ptr<KeyValueRecordReader>> CreateRealtimePrimaryKeyCommitReaderForTest(
    std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.push_back(std::move(reader));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> adapted_readers,
                           RealtimePrimaryKeyReaderFactory::CreateForCommit(
                               std::move(readers), key_schema, value_schema, memory_pool));
    return std::move(adapted_readers[0]);
}

class MalformedBitmapBatchReader : public BatchReader {
 public:
    MalformedBitmapBatchReader(std::unique_ptr<BatchReader>&& delegate, int32_t row_id)
        : delegate_(std::move(delegate)), row_id_(row_id) {}

    Result<ReadBatch> NextBatch() override {
        return delegate_->NextBatch();
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, delegate_->NextBatchWithBitmap());
        if (!IsEofBatch(batch)) {
            batch.second.Add(row_id_);
        }
        return batch;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return delegate_->GetReaderMetrics();
    }

    void Close() override {
        delegate_->Close();
    }

 private:
    std::unique_ptr<BatchReader> delegate_;
    int32_t row_id_;
};

}  // namespace

class RealtimePrimaryKeyReaderTest : public testing::Test {
 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(RealtimePrimaryKeyReaderTest, TestPrimaryKeySchemaLayouts) {
    arrow::FieldVector value_fields = {arrow::field("key", arrow::int64(), false),
                                       arrow::field("value", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = MakeTransportSchema(value_fields);

    ASSERT_EQ(schema->field(0)->name(), "_VALUE_KIND");
    ASSERT_EQ(schema->field(1)->name(), "_SEQUENCE_NUMBER");
    ASSERT_EQ(schema->field(2)->name(), "_REALTIME_OFFSET");
    ASSERT_EQ(schema->field(3)->name(), "key");
    ASSERT_EQ(schema->field(4)->name(), "value");
    ASSERT_FALSE(schema->field(0)->nullable());
    ASSERT_FALSE(schema->field(1)->nullable());
    ASSERT_EQ(schema->field(2)->nullable(), SpecialFields::RealtimeOffset().Nullable());
    ASSERT_FALSE(schema->field(3)->nullable());
    ASSERT_TRUE(schema->field(4)->nullable());

    std::shared_ptr<arrow::Schema> logical_schema =
        RealtimePrimaryKeyLayout::CreateLogicalSchema(value_fields);
    ASSERT_EQ(logical_schema->field(0)->name(), "_VALUE_KIND");
    ASSERT_EQ(logical_schema->field(1)->name(), "_SEQUENCE_NUMBER");
    ASSERT_EQ(logical_schema->field(2)->name(), "key");
    ASSERT_EQ(logical_schema->field(3)->name(), "value");
}

TEST_F(RealtimePrimaryKeyReaderTest, TestQueryAllowsCommittedPrefix) {
    std::vector<DataField> value_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                           DataField(1, arrow::field("v0", arrow::int32()))};
    std::shared_ptr<arrow::Schema> value_schema =
        DataField::ConvertDataFieldsToArrowSchema(value_fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema({value_schema->field(0)});
    std::shared_ptr<arrow::Schema> transport_schema = MakeTransportSchema(value_schema->fields());
    std::shared_ptr<arrow::DataType> transport_type = arrow::struct_(transport_schema->fields());
    auto transport_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(transport_type, R"([
        [0, 100, 0, 1, 10],
        [0, 101, 1, 2, 20],
        [0, 102, 2, 4, 40],
        [0, 103, 3, 6, 60]
    ])")
            .ValueOrDie());

    std::vector<std::unique_ptr<BatchReader>> batch_readers;
    batch_readers.push_back(
        std::make_unique<MockFileBatchReader>(transport_array, transport_type, 2));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
        CreateRealtimePrimaryKeyQueryReadersForTest(std::move(batch_readers), OffsetRange(2, 4),
                                                    key_schema, value_schema, pool_));
    ASSERT_EQ(1, readers.size());
    ASSERT_OK_AND_ASSIGN(
        std::vector<KeyValue> results,
        (ReadResultCollector::CollectKeyValueResult<
            KeyValueRecordReader, KeyValueRecordReader::Iterator>(readers[0].get())));

    std::vector<RowKind*> row_kinds = {const_cast<RowKind*>(RowKind::Insert()),
                                       const_cast<RowKind*>(RowKind::Insert())};
    std::vector<int64_t> levels = {KeyValue::UNKNOWN_LEVEL, KeyValue::UNKNOWN_LEVEL};
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        row_kinds, {102, 103}, levels, {{4}, {6}}, {{4, 40}, {6, 60}}, pool_);
    KeyValueChecker::CheckResult(expected, results, 1, 2);
}

TEST_F(RealtimePrimaryKeyReaderTest, TestQueryOffsetCoverageAcrossReadersAndBatches) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::shared_ptr<arrow::Schema> transport_schema = MakeTransportSchema({key});
    std::shared_ptr<arrow::DataType> transport_type = arrow::struct_(transport_schema->fields());
    std::shared_ptr<arrow::Array> first_array =
        arrow::ipc::internal::json::ArrayFromJSON(transport_type,
                                                  R"([[0, 10, 2, 1], [0, 11, 0, 2]])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> second_array =
        arrow::ipc::internal::json::ArrayFromJSON(transport_type,
                                                  R"([[0, 12, 3, 3], [0, 13, 1, 4]])")
            .ValueOrDie();
    std::vector<std::unique_ptr<BatchReader>> batch_readers;
    batch_readers.push_back(
        std::make_unique<MockFileBatchReader>(first_array, transport_type, /*read_batch_size=*/1));
    batch_readers.push_back(
        std::make_unique<MockFileBatchReader>(second_array, transport_type, /*read_batch_size=*/1));

    ASSERT_OK_AND_ASSIGN(
        std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
        CreateRealtimePrimaryKeyQueryReadersForTest(std::move(batch_readers), OffsetRange(0, 4),
                                                    value_schema, value_schema, pool_));
    int64_t row_count = 0;
    for (const std::unique_ptr<KeyValueRecordReader>& reader : readers) {
        ASSERT_OK_AND_ASSIGN(
            std::vector<KeyValue> rows,
            (ReadResultCollector::CollectKeyValueResult<
                KeyValueRecordReader, KeyValueRecordReader::Iterator>(reader.get())));
        row_count += static_cast<int64_t>(rows.size());
    }
    ASSERT_EQ(4, row_count);
}

TEST_F(RealtimePrimaryKeyReaderTest, TestQueryAllowsEmptyReadersForEmptyVisibleRange) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::vector<std::unique_ptr<BatchReader>> batch_readers;

    ASSERT_OK_AND_ASSIGN(
        std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
        CreateRealtimePrimaryKeyQueryReadersForTest(std::move(batch_readers), OffsetRange(1, 1),
                                                    value_schema, value_schema, pool_));
    ASSERT_TRUE(readers.empty());
}

TEST_F(RealtimePrimaryKeyReaderTest, TestQueryBitmapBounds) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::shared_ptr<arrow::Schema> transport_schema = MakeTransportSchema({key});
    std::shared_ptr<arrow::DataType> transport_type = arrow::struct_(transport_schema->fields());
    std::shared_ptr<arrow::Array> transport_array =
        arrow::ipc::internal::json::ArrayFromJSON(transport_type, R"([[0, 10, 0, 1]])")
            .ValueOrDie();
    auto batch_reader = std::make_unique<MalformedBitmapBatchReader>(
        std::make_unique<MockFileBatchReader>(transport_array, transport_type, /*batch_size=*/1),
        /*row_id=*/1);

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> reader,
        CreateRealtimePrimaryKeyQueryReaderForTest(std::move(batch_reader), OffsetRange(0, 1),
                                                   value_schema, value_schema, pool_));
    Result<std::vector<KeyValue>> result =
        ReadResultCollector::CollectKeyValueResult<KeyValueRecordReader,
                                                   KeyValueRecordReader::Iterator>(reader.get());
    ASSERT_TRUE(result.status().IsInvalid());
    ASSERT_NOK_WITH_MSG(result,
                        "selected row id 1 is out of bounds for realtime query batch length 1");
}

TEST_F(RealtimePrimaryKeyReaderTest, TestQueryProjectionWithReorderedTransportFields) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Field> extra = MakeField("extra", arrow::int32(), 1);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::shared_ptr<arrow::Schema> transport_schema = arrow::schema({
        key,
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()),
        DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())->WithNullable(false),
        extra,
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())
            ->WithNullable(false),
    });
    std::shared_ptr<arrow::DataType> transport_type = arrow::struct_(transport_schema->fields());
    auto transport_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(transport_type, R"([[1, 0, 0, 2, 10]])")
            .ValueOrDie());

    auto query_batch_reader =
        std::make_unique<MockFileBatchReader>(transport_array, transport_type, 1);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> query_reader,
        CreateRealtimePrimaryKeyQueryReaderForTest(std::move(query_batch_reader), OffsetRange(0, 1),
                                                   value_schema, value_schema, pool_));
    ASSERT_OK_AND_ASSIGN(
        std::vector<KeyValue> query_results,
        (ReadResultCollector::CollectKeyValueResult<
            KeyValueRecordReader, KeyValueRecordReader::Iterator>(query_reader.get())));
    ASSERT_EQ(query_results.size(), 1);
    ASSERT_EQ(query_results[0].value->GetFieldCount(), 1);
    ASSERT_EQ(query_results[0].value->GetInt(0), 1);
}

TEST_F(RealtimePrimaryKeyReaderTest, TestCommitCoverageAcrossReadersAndBatches) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::shared_ptr<arrow::Schema> transport_schema = MakeTransportSchema({key});
    std::shared_ptr<arrow::DataType> transport_type = arrow::struct_(transport_schema->fields());
    std::shared_ptr<arrow::Array> first_array =
        arrow::ipc::internal::json::ArrayFromJSON(transport_type,
                                                  R"([[0, 10, 2, 1], [0, 11, 0, 3]])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> second_array =
        arrow::ipc::internal::json::ArrayFromJSON(transport_type,
                                                  R"([[0, 12, 1, 2], [0, 13, 3, 4]])")
            .ValueOrDie();
    std::vector<std::unique_ptr<BatchReader>> batch_readers;
    batch_readers.push_back(
        std::make_unique<MockFileBatchReader>(first_array, transport_type, /*read_batch_size=*/1));
    batch_readers.push_back(
        std::make_unique<MockFileBatchReader>(second_array, transport_type, /*read_batch_size=*/1));

    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
                         RealtimePrimaryKeyReaderFactory::CreateForCommit(
                             std::move(batch_readers), value_schema, value_schema, pool_));
    int64_t row_count = 0;
    for (const std::unique_ptr<KeyValueRecordReader>& reader : readers) {
        ASSERT_OK_AND_ASSIGN(
            std::vector<KeyValue> rows,
            (ReadResultCollector::CollectKeyValueResult<
                KeyValueRecordReader, KeyValueRecordReader::Iterator>(reader.get())));
        row_count += static_cast<int64_t>(rows.size());
    }
    ASSERT_EQ(4, row_count);
}

TEST_F(RealtimePrimaryKeyReaderTest, TestBadCommitBatch) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Field> value = MakeField("value", arrow::int32(), 1);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key, value});
    std::shared_ptr<arrow::Schema> actual_schema = MakeTransportSchema({key});
    std::shared_ptr<arrow::DataType> actual_type = arrow::struct_(actual_schema->fields());
    std::shared_ptr<arrow::Array> actual =
        arrow::ipc::internal::json::ArrayFromJSON(actual_type, R"([[0, 10, 0, 1]])").ValueOrDie();

    auto batch_reader = std::make_unique<MockFileBatchReader>(actual, actual_type, 1);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyValueRecordReader> reader,
                         CreateRealtimePrimaryKeyCommitReaderForTest(
                             std::move(batch_reader), arrow::schema({key}), value_schema, pool_));
    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "cannot find field value in data batch");
}

TEST_F(RealtimePrimaryKeyReaderTest, TestSafeDecode) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::shared_ptr<arrow::Schema> transport_schema = MakeTransportSchema({key});

    arrow::FieldVector invalid_fields = transport_schema->fields();
    invalid_fields[0] = invalid_fields[0]->WithName("wrong_value_kind");
    invalid_fields[3] = MakeField("wrong_key", arrow::int32(), 99);
    std::shared_ptr<arrow::DataType> invalid_type = arrow::struct_(invalid_fields);
    auto invalid_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(invalid_type, R"([[0, 10, 0, 1]])").ValueOrDie());

    auto batch_reader = std::make_unique<MockFileBatchReader>(invalid_array, invalid_type, 1);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> reader,
        CreateRealtimePrimaryKeyQueryReaderForTest(std::move(batch_reader), OffsetRange(0, 1),
                                                   value_schema, value_schema, pool_));
    ASSERT_NOK_WITH_MSG(
        (ReadResultCollector::CollectKeyValueResult<KeyValueRecordReader,
                                                    KeyValueRecordReader::Iterator>(reader.get())),
        "cannot cast VALUE_KIND column");
}

TEST_F(RealtimePrimaryKeyReaderTest, TestNestedValues) {
    std::shared_ptr<arrow::Field> id = MakeField("id", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema({id});
    std::shared_ptr<arrow::Field> query_item_b = MakeField("renamed_b", arrow::int32(), 11);
    std::shared_ptr<arrow::Field> query_item_a = MakeField("renamed_a", arrow::int32(), 10);
    std::shared_ptr<arrow::Field> query_items = MakeField(
        "items_renamed",
        arrow::list(arrow::field("element", arrow::struct_({query_item_b, query_item_a}))), 2);
    std::shared_ptr<arrow::Field> query_attr_y = MakeField("renamed_y", arrow::int32(), 21);
    std::shared_ptr<arrow::Field> query_attr_x = MakeField("renamed_x", arrow::int32(), 20);
    std::shared_ptr<arrow::Field> query_attrs =
        MakeField("attrs_renamed",
                  arrow::map(arrow::utf8(), arrow::struct_({query_attr_y, query_attr_x})), 3);
    std::shared_ptr<arrow::Field> query_key_right = MakeField("renamed_right", arrow::int32(), 31);
    std::shared_ptr<arrow::Field> query_key_left = MakeField("renamed_left", arrow::int32(), 30);
    std::shared_ptr<arrow::Field> query_keyed_values =
        MakeField("keyed_values_renamed",
                  arrow::map(arrow::struct_({query_key_right, query_key_left}), arrow::int32()), 4);
    std::shared_ptr<arrow::Schema> query_value_schema =
        arrow::schema({id, query_items, query_attrs, query_keyed_values});
    std::shared_ptr<arrow::Schema> transport_schema =
        MakeTransportSchema(query_value_schema->fields());
    std::shared_ptr<arrow::DataType> transport_type = arrow::struct_(transport_schema->fields());
    std::shared_ptr<arrow::Array> transport_array =
        arrow::ipc::internal::json::ArrayFromJSON(
            transport_type,
            R"([[0, 10, 0, 1, [[200, 100], [400, 300]], [["k1", [8, 7]], ["k2", [10, 9]]], [[[12, 11], 13], [[22, 21], 23]]]])")
            .ValueOrDie();

    auto batch_reader = std::make_unique<MockFileBatchReader>(transport_array, transport_type, 1);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> reader,
        CreateRealtimePrimaryKeyQueryReaderForTest(std::move(batch_reader), OffsetRange(0, 1),
                                                   key_schema, query_value_schema, pool_));
    ASSERT_OK_AND_ASSIGN(
        std::vector<KeyValue> results,
        (ReadResultCollector::CollectKeyValueResult<KeyValueRecordReader,
                                                    KeyValueRecordReader::Iterator>(reader.get())));

    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].key->GetInt(0), 1);
    ASSERT_EQ(results[0].value->GetFieldCount(), 4);
    ASSERT_EQ(results[0].value->GetInt(0), 1);

    std::shared_ptr<InternalArray> item_array = results[0].value->GetArray(1);
    ASSERT_EQ(item_array->Size(), 2);
    std::shared_ptr<InternalRow> first_item = item_array->GetRow(0, 2);
    ASSERT_EQ(first_item->GetInt(0), 200);
    ASSERT_EQ(first_item->GetInt(1), 100);
    std::shared_ptr<InternalRow> second_item = item_array->GetRow(1, 2);
    ASSERT_EQ(second_item->GetInt(0), 400);
    ASSERT_EQ(second_item->GetInt(1), 300);

    std::shared_ptr<InternalMap> attr_map = results[0].value->GetMap(2);
    ASSERT_EQ(attr_map->Size(), 2);
    std::shared_ptr<InternalArray> key_array = attr_map->KeyArray();
    ASSERT_EQ(std::string(key_array->GetStringView(0)), "k1");
    ASSERT_EQ(std::string(key_array->GetStringView(1)), "k2");
    std::shared_ptr<InternalArray> value_array = attr_map->ValueArray();
    std::shared_ptr<InternalRow> first_attr = value_array->GetRow(0, 2);
    ASSERT_EQ(first_attr->GetInt(0), 8);
    ASSERT_EQ(first_attr->GetInt(1), 7);
    std::shared_ptr<InternalRow> second_attr = value_array->GetRow(1, 2);
    ASSERT_EQ(second_attr->GetInt(0), 10);
    ASSERT_EQ(second_attr->GetInt(1), 9);

    std::shared_ptr<InternalMap> keyed_value_map = results[0].value->GetMap(3);
    ASSERT_EQ(keyed_value_map->Size(), 2);
    std::shared_ptr<InternalArray> struct_keys = keyed_value_map->KeyArray();
    std::shared_ptr<InternalRow> first_key = struct_keys->GetRow(0, 2);
    ASSERT_EQ(first_key->GetInt(0), 12);
    ASSERT_EQ(first_key->GetInt(1), 11);
    std::shared_ptr<InternalRow> second_key = struct_keys->GetRow(1, 2);
    ASSERT_EQ(second_key->GetInt(0), 22);
    ASSERT_EQ(second_key->GetInt(1), 21);
    ASSERT_EQ(keyed_value_map->ValueArray()->GetInt(0), 13);
    ASSERT_EQ(keyed_value_map->ValueArray()->GetInt(1), 23);
}

TEST_F(RealtimePrimaryKeyReaderTest, TestFactoryRejectsNullReader) {
    std::vector<DataField> value_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                           DataField(1, arrow::field("v0", arrow::int32()))};
    std::shared_ptr<arrow::Schema> value_schema =
        DataField::ConvertDataFieldsToArrowSchema(value_fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema({value_schema->field(0)});
    std::shared_ptr<arrow::Schema> transport_schema = MakeTransportSchema(value_schema->fields());
    std::shared_ptr<arrow::DataType> transport_type = arrow::struct_(transport_schema->fields());
    auto transport_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(transport_type, R"([
        [0, 10, 0, 1, 100]
    ])")
            .ValueOrDie());

    std::vector<std::unique_ptr<BatchReader>> batch_readers;
    batch_readers.push_back(
        std::make_unique<MockFileBatchReader>(transport_array, transport_type, 1));
    batch_readers.push_back(nullptr);
    ASSERT_NOK_WITH_MSG(RealtimePrimaryKeyReaderFactory::CreateForCommit(
                            std::move(batch_readers), key_schema, value_schema, pool_),
                        "real-time store returned a null reader");
}

}  // namespace paimon::test

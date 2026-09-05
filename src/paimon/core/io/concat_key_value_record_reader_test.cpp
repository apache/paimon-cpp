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

#include "paimon/core/io/concat_key_value_record_reader.h"

#include <cstdint>
#include <string>
#include <variant>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/core/key_value.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/mock/mock_key_value_data_file_record_reader.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class ConcatKeyValueRecordReaderTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void CheckResult(const std::vector<std::shared_ptr<arrow::StructArray>>& src_array_vec,
                     const std::vector<KeyValue>& expected,
                     const std::shared_ptr<arrow::Schema>& key_schema,
                     const std::shared_ptr<arrow::Schema>& value_schema) const {
        for (auto batch_size : {1, 2, 3, 4, 100}) {
            std::vector<std::unique_ptr<KeyValueRecordReader>> record_readers;
            for (const auto& src_array : src_array_vec) {
                auto src_type = src_array->type();
                auto file_batch_reader = std::make_unique<MockFileBatchReader>(
                    src_array, src_type, /*batch_size=*/batch_size);
                auto record_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
                    std::move(file_batch_reader), key_schema, value_schema, /*level=*/0, pool_);
                record_readers.push_back(std::move(record_reader));
            }

            auto concat_record_reader =
                std::make_unique<ConcatKeyValueRecordReader>(std::move(record_readers));
            ASSERT_OK_AND_ASSIGN(
                auto results,
                (ReadResultCollector::CollectKeyValueResult<ConcatKeyValueRecordReader,
                                                            KeyValueRecordReader::Iterator>(
                    concat_record_reader.get())));
            KeyValueChecker::CheckResult(expected, results, key_schema->num_fields(),
                                         value_schema->num_fields());
        }
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(ConcatKeyValueRecordReaderTest, TestSimple) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};
    std::shared_ptr<arrow::Schema> key_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 10, 20, 30],
        [0, 0, 1, 2, 11, 21, 31],
        [1, 0, 2, 2, 12, 22, 32],
        [2, 0, 2, 3, 13, 23, 33]
    ])")
            .ValueOrDie());
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 2, 4, 14, 24, 34],
        [0, 0, 3, 1, 15, 25, 35],
        [1, 0, 3, 2, 16, 26, 36],
        [2, 0, 3, 3, 17, 27, 37]
    ])")
            .ValueOrDie());

    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        /*seq_vec=*/{0, 0, 1, 2, 0, 0, 1, 2},
        /*key_vec=*/{{1, 1}, {1, 2}, {2, 2}, {2, 3}, {2, 4}, {3, 1}, {3, 2}, {3, 3}},
        /*value_vec=*/
        {{1, 1, 10, 20, 30},
         {1, 2, 11, 21, 31},
         {2, 2, 12, 22, 32},
         {2, 3, 13, 23, 33},
         {2, 4, 14, 24, 34},
         {3, 1, 15, 25, 35},
         {3, 2, 16, 26, 36},
         {3, 3, 17, 27, 37}},
        pool_);
    CheckResult({src_array1, src_array2}, expected, key_schema, value_schema);
}

TEST_F(ConcatKeyValueRecordReaderTest, TestSingleReaderInConcat) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};
    std::shared_ptr<arrow::Schema> key_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 10, 20, 30],
        [0, 0, 1, 2, 11, 21, 31],
        [1, 0, 2, 2, 12, 22, 32],
        [2, 0, 2, 3, 13, 23, 33]
    ])")
            .ValueOrDie());

    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        /*seq_vec=*/{0, 0, 1, 2}, /*key_vec=*/{{1, 1}, {1, 2}, {2, 2}, {2, 3}},
        /*value_vec=*/
        {{1, 1, 10, 20, 30}, {1, 2, 11, 21, 31}, {2, 2, 12, 22, 32}, {2, 3, 13, 23, 33}}, pool_);
    CheckResult({src_array1}, expected, key_schema, value_schema);
}

TEST_F(ConcatKeyValueRecordReaderTest, TestEmptyResult) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};
    std::shared_ptr<arrow::Schema> key_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
    ])")
            .ValueOrDie());

    CheckResult({src_array1}, {}, key_schema, value_schema);
}

TEST_F(ConcatKeyValueRecordReaderTest, TestEmptyReader) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};
    std::shared_ptr<arrow::Schema> key_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 10, 20, 30],
        [0, 0, 1, 2, 11, 21, 31],
        [1, 0, 2, 2, 12, 22, 32],
        [2, 0, 2, 3, 13, 23, 33]
    ])")
            .ValueOrDie());

    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        /*seq_vec=*/{0, 0, 1, 2}, /*key_vec=*/{{1, 1}, {1, 2}, {2, 2}, {2, 3}},
        /*value_vec=*/
        {{1, 1, 10, 20, 30}, {1, 2, 11, 21, 31}, {2, 2, 12, 22, 32}, {2, 3, 13, 23, 33}}, pool_);

    CheckResult({src_array1, src_array2}, expected, key_schema, value_schema);
    CheckResult({src_array2, src_array1}, expected, key_schema, value_schema);
}

namespace {
/// Counts Warmup() calls and forwards everything else, so a test can assert how far ahead the
/// lookahead reaches.
class CountingWarmupReader : public KeyValueRecordReader {
 public:
    explicit CountingWarmupReader(std::unique_ptr<KeyValueRecordReader>&& inner)
        : inner_(std::move(inner)) {}

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override {
        return inner_->NextBatch();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return inner_->GetReaderMetrics();
    }

    Status Warmup() override {
        warmups_++;
        return inner_->Warmup();
    }

    void Close() override {
        inner_->Close();
    }

    int warmups() const {
        return warmups_;
    }

 private:
    std::unique_ptr<KeyValueRecordReader> inner_;
    int warmups_ = 0;
};
}  // namespace

TEST_F(ConcatKeyValueRecordReaderTest, TestWarmupLooksOneReaderAhead) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};
    std::shared_ptr<arrow::Schema> key_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);
    auto make_array = [&src_type](const std::string& json) {
        return std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(src_type, json).ValueOrDie());
    };
    const std::vector<std::shared_ptr<arrow::StructArray>> src_array_vec = {
        make_array(R"([[0, 0, 1, 1, 10, 20, 30], [0, 0, 1, 2, 11, 21, 31]])"),
        make_array(R"([[0, 0, 2, 1, 12, 22, 32], [0, 0, 2, 2, 13, 23, 33]])"),
        make_array(R"([[0, 0, 3, 1, 14, 24, 34], [0, 0, 3, 2, 15, 25, 35]])")};

    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::vector<CountingWarmupReader*> counters;
    // The counters point into what the returned Concat owns, so they are only valid while it is.
    auto build = [&](std::vector<CountingWarmupReader*>* out) {
        out->clear();
        std::vector<std::unique_ptr<KeyValueRecordReader>> record_readers;
        for (const auto& src_array : src_array_vec) {
            auto file_batch_reader = std::make_unique<MockFileBatchReader>(
                src_array, src_array->type(), /*batch_size=*/100);
            auto record_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
                std::move(file_batch_reader), key_schema, value_schema, /*level=*/0, pool);
            auto counting = std::make_unique<CountingWarmupReader>(std::move(record_reader));
            out->push_back(counting.get());
            record_readers.push_back(std::move(counting));
        }
        return std::make_unique<ConcatKeyValueRecordReader>(std::move(record_readers));
    };

    {
        std::unique_ptr<ConcatKeyValueRecordReader> concat = build(&counters);
        // Nothing is started before a read asks for it.
        EXPECT_EQ(counters[0]->warmups(), 0);
        EXPECT_EQ(counters[1]->warmups(), 0);
        EXPECT_EQ(counters[2]->warmups(), 0);

        // The first batch warms the file it reads and the one after it, and no further: a deeper
        // lookahead would hold more files open at once than the prefetch executor has threads for.
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyValueRecordReader::Iterator> iterator,
                             concat->NextBatch());
        ASSERT_NE(iterator, nullptr);
        EXPECT_EQ(counters[0]->warmups(), 1);
        EXPECT_EQ(counters[1]->warmups(), 1);
        EXPECT_EQ(counters[2]->warmups(), 0);

        // Rows may reference buffers the child allocated, so drop them before closing it.
        iterator.reset();
        // Idempotent: warming an already-warm reader is not an error.
        EXPECT_TRUE(concat->Warmup().ok());
        EXPECT_TRUE(concat->Warmup().ok());
        concat->Close();
    }

    {
        std::unique_ptr<ConcatKeyValueRecordReader> concat = build(&counters);
        ASSERT_OK_AND_ASSIGN(
            auto results,
            (ReadResultCollector::CollectKeyValueResult<ConcatKeyValueRecordReader,
                                                        KeyValueRecordReader::Iterator>(
                concat.get())));
        std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
            /*seq_vec=*/{0, 0, 0, 0, 0, 0},
            /*key_vec=*/{{1, 1}, {1, 2}, {2, 1}, {2, 2}, {3, 1}, {3, 2}},
            /*value_vec=*/
            {{1, 1, 10, 20, 30},
             {1, 2, 11, 21, 31},
             {2, 1, 12, 22, 32},
             {2, 2, 13, 23, 33},
             {3, 1, 14, 24, 34},
             {3, 2, 15, 25, 35}},
            pool);
        // Warming up changes when a file is opened, never what it reads.
        KeyValueChecker::CheckResult(expected, results, key_schema->num_fields(),
                                     value_schema->num_fields());
        for (const CountingWarmupReader* counter : counters) {
            EXPECT_GE(counter->warmups(), 1);
        }
    }
}

}  // namespace paimon::test

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

#include "paimon/common/file_index/bloomfilter/bloom_filter_file_index.h"

#include <cstring>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class BloomFilterIndexReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }
    void TearDown() override {
        pool_.reset();
    }

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::DataType>& data_type) const {
        auto schema = arrow::schema({arrow::field("f0", data_type)});
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return c_schema;
    }

    Result<PAIMON_UNIQUE_PTR<Bytes>> WriteIndex(
        const std::shared_ptr<arrow::DataType>& data_type, const std::string& json,
        const std::map<std::string, std::string>& options) const {
        const std::shared_ptr<arrow::Schema> schema =
            arrow::schema({arrow::field("f0", data_type)});
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), json)
                .ValueOrDie();
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &c_schema));
        BloomFilterFileIndex file_index(options);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexWriter> writer,
                               file_index.CreateWriter(&c_schema, pool_));
        ::ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        PAIMON_RETURN_NOT_OK(writer->AddBatch(&c_array));
        return writer->SerializedBytes();
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(BloomFilterIndexReaderTest, TestWriterRejectsInvalidOptionsAndType) {
    auto create_writer = [&](const std::shared_ptr<arrow::DataType>& type,
                             const std::map<std::string, std::string>& options) {
        BloomFilterFileIndex file_index(options);
        return file_index.CreateWriter(CreateArrowSchema(type).get(), pool_);
    };
    ASSERT_NOK_WITH_MSG(create_writer(arrow::int32(), {{"items", "0"}}),
                        "items must be greater than 0");
    ASSERT_NOK_WITH_MSG(create_writer(arrow::int32(), {{"fpp", "1"}}),
                        "fpp must be greater than 0 and less than 1");
    ASSERT_NOK_WITH_MSG(create_writer(arrow::boolean(), {}),
                        "bloom filter index does not support BOOLEAN");
}

TEST_F(BloomFilterIndexReaderTest, TestStringType) {
    // Java writer output for data: "a", "b", "" with items=10 and fpp=0.02.
    const std::vector<uint8_t> expected = {0, 0, 0, 6, 0, 32, 32, 3, 208, 32, 0, 64, 73, 16, 201};
    ASSERT_OK_AND_ASSIGN(
        PAIMON_UNIQUE_PTR<Bytes> bytes,
        WriteIndex(arrow::utf8(), R"([["a"], ["b"], [""]])", {{"items", "10"}, {"fpp", "0.02"}}));
    ASSERT_EQ(expected.size(), bytes->size());
    ASSERT_EQ(0, std::memcmp(expected.data(), bytes->data(), expected.size()));

    std::shared_ptr<ByteArrayInputStream> input_stream =
        std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());

    BloomFilterFileIndex file_index({});
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        file_index.CreateReader(CreateArrowSchema(arrow::utf8()).get(),
                                /*start=*/0, /*length=*/bytes->size(), input_stream, pool_));
    ASSERT_TRUE(reader);
    ASSERT_TRUE(reader->VisitEqual(Literal(FieldType::STRING, "a", 1)).value()->IsRemain().value());
    ASSERT_TRUE(reader->VisitEqual(Literal(FieldType::STRING, "b", 1)).value()->IsRemain().value());
    ASSERT_TRUE(reader->VisitEqual(Literal(FieldType::STRING, "", 0)).value()->IsRemain().value());
    ASSERT_TRUE(reader->VisitEqual(Literal(FieldType::STRING)).value()->IsRemain().value());
}

TEST_F(BloomFilterIndexReaderTest, TestIntegerTypes) {
    // data: 1, 2, -1, 123
    std::vector<uint8_t> index_bytes = {0, 0, 0, 6, 24, 4, 79, 0, 35, 128, 1, 136, 26, 64, 129};
    auto input_stream = std::make_shared<ByteArrayInputStream>(
        reinterpret_cast<char*>(index_bytes.data()), index_bytes.size());

    BloomFilterFileIndex file_index({});

    auto check_result = [&](const std::shared_ptr<arrow::DataType>& type,
                            const std::vector<Literal>& literals) {
        ASSERT_OK_AND_ASSIGN(auto reader,
                             file_index.CreateReader(CreateArrowSchema(type).get(),
                                                     /*start=*/0, /*length=*/index_bytes.size(),
                                                     input_stream, pool_));
        ASSERT_TRUE(reader);
        for (const Literal& literal : literals) {
            ASSERT_TRUE(reader->VisitEqual(literal).value()->IsRemain().value());
        }
        ASSERT_OK_AND_ASSIGN(auto field_type, FieldTypeUtils::ConvertToFieldType(type->id()));
        ASSERT_TRUE(reader->VisitEqual(Literal(field_type)).value()->IsRemain().value());
    };

    check_result(arrow::int8(),
                 {Literal(static_cast<int8_t>(1)), Literal(static_cast<int8_t>(2)),
                  Literal(static_cast<int8_t>(-1)), Literal(static_cast<int8_t>(123))});
    check_result(arrow::int16(),
                 {Literal(static_cast<int16_t>(1)), Literal(static_cast<int16_t>(2)),
                  Literal(static_cast<int16_t>(-1)), Literal(static_cast<int16_t>(123))});
    check_result(arrow::int32(),
                 {Literal(static_cast<int32_t>(1)), Literal(static_cast<int32_t>(2)),
                  Literal(static_cast<int32_t>(-1)), Literal(static_cast<int32_t>(123))});
    check_result(arrow::int64(),
                 {Literal(static_cast<int64_t>(1)), Literal(static_cast<int64_t>(2)),
                  Literal(static_cast<int64_t>(-1)), Literal(static_cast<int64_t>(123))});
    check_result(arrow::date32(), {Literal(FieldType::DATE, 1), Literal(FieldType::DATE, 2),
                                   Literal(FieldType::DATE, -1), Literal(FieldType::DATE, 123)});
}

TEST_F(BloomFilterIndexReaderTest, TestFloatTypes) {
    // data: -0.3, -5.2, 0, 0.1, 123.12
    auto check_result = [&](const std::vector<uint8_t>& index_bytes,
                            const std::shared_ptr<arrow::DataType>& type,
                            const std::vector<Literal>& literals) {
        auto input_stream = std::make_shared<ByteArrayInputStream>(
            reinterpret_cast<const char*>(index_bytes.data()), index_bytes.size());

        BloomFilterFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(auto reader,
                             file_index.CreateReader(CreateArrowSchema(type).get(),
                                                     /*start=*/0, /*length=*/index_bytes.size(),
                                                     input_stream, pool_));
        ASSERT_TRUE(reader);
        for (const Literal& literal : literals) {
            ASSERT_TRUE(reader->VisitEqual(literal).value()->IsRemain().value());
        }
        ASSERT_OK_AND_ASSIGN(auto field_type, FieldTypeUtils::ConvertToFieldType(type->id()));
        ASSERT_TRUE(reader->VisitEqual(Literal(field_type)).value()->IsRemain().value());
    };

    {
        std::vector<uint8_t> index_bytes = {0,   0, 0, 6,  21,  130, 128, 2,
                                            106, 0, 8, 25, 165, 32,  10};
        check_result(
            index_bytes, arrow::float32(),
            {Literal(-0.3f), Literal(-5.2f), Literal(0.0f), Literal(0.1f), Literal(123.12f)});
    }
    {
        std::vector<uint8_t> index_bytes = {0, 0, 0, 6, 9, 6, 1, 48, 33, 6, 1, 17, 105, 0, 21};
        check_result(index_bytes, arrow::float64(),
                     {Literal(-0.3), Literal(-5.2), Literal(0.0), Literal(0.1), Literal(123.12)});
    }
}

TEST_F(BloomFilterIndexReaderTest, TestTimestampType) {
    // data:
    // 1745542802000lms, 123000ns
    // 1745542902000lms, 123000ns
    // 1745542602000lms, 123000ns
    // -1745lms, 123000ns
    // -1765lms, 123000ns
    // 1745542802000lms, 123001ns
    // -1725lms, 123000ns
    std::vector<uint8_t> index_bytes = {0, 0, 0, 6, 37, 47, 72, 16, 193, 132, 38, 199, 13, 46, 4};
    auto input_stream = std::make_shared<ByteArrayInputStream>(
        reinterpret_cast<char*>(index_bytes.data()), index_bytes.size());

    BloomFilterFileIndex file_index({});
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        file_index.CreateReader(CreateArrowSchema(arrow::timestamp(arrow::TimeUnit::NANO)).get(),
                                /*start=*/0, /*length=*/index_bytes.size(), input_stream, pool_));
    ASSERT_TRUE(reader);
    ASSERT_TRUE(
        reader->VisitEqual(Literal(Timestamp(1745542802000l, 123000))).value()->IsRemain().value());
    ASSERT_TRUE(
        reader->VisitEqual(Literal(Timestamp(1745542902000l, 123000))).value()->IsRemain().value());
    ASSERT_TRUE(
        reader->VisitEqual(Literal(Timestamp(1745542602000l, 123000))).value()->IsRemain().value());
    ASSERT_TRUE(reader->VisitEqual(Literal(Timestamp(-1745l, 123000))).value()->IsRemain().value());
    ASSERT_TRUE(reader->VisitEqual(Literal(Timestamp(-1765l, 123000))).value()->IsRemain().value());
    ASSERT_TRUE(
        reader->VisitEqual(Literal(Timestamp(1745542802000l, 123001))).value()->IsRemain().value());
    ASSERT_TRUE(reader->VisitEqual(Literal(Timestamp(-1725l, 123000))).value()->IsRemain().value());
}

}  // namespace paimon::test

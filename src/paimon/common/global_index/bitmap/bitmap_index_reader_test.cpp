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

#include "paimon/common/global_index/bitmap/bitmap_index_reader.h"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/global_index/bitmap/bitmap_global_index_writer.h"
#include "paimon/common/global_index/key_serializer.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/testing/mock/mock_index_path_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BitmapIndexReaderTest : public ::testing::Test {
 protected:
    static Literal StringLiteral(const std::string& value) {
        return Literal(FieldType::STRING, value.data(), value.size());
    }

    static void CheckResult(const Result<std::shared_ptr<GlobalIndexResult>>& result,
                            const std::vector<int64_t>& expected) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> index_result, result);
        std::shared_ptr<BitmapGlobalIndexResult> bitmap_result =
            std::dynamic_pointer_cast<BitmapGlobalIndexResult>(index_result);
        ASSERT_TRUE(bitmap_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, bitmap_result->GetBitmap());
        ASSERT_EQ(RoaringBitmap64::From(expected), *bitmap);
    }

    void SetUp() override {
        pool_ = GetDefaultPool();
        test_dir_ = UniqueTestDirectory::Create("local");
        ASSERT_TRUE(test_dir_);
        file_manager_ = std::make_shared<GlobalIndexFileManager>(
            test_dir_->GetFileSystem(), std::make_shared<MockIndexPathFactory>(test_dir_->Str()));
        ASSERT_OK_AND_ASSIGN(compression_factory_,
                             BlockCompressionFactory::Create(BlockCompressionType::NONE));
    }

    Result<std::shared_ptr<BitmapGlobalIndexWriter>> CreateWriter(
        const std::shared_ptr<arrow::Field>& field, int32_t dictionary_block_size = 24) {
        std::shared_ptr<arrow::StructType> struct_type =
            std::static_pointer_cast<arrow::StructType>(arrow::struct_({field}));
        return BitmapGlobalIndexWriter::Create(field->name(), struct_type, file_manager_,
                                               dictionary_block_size, compression_factory_, pool_);
    }

    Result<std::shared_ptr<BitmapIndexReader>> CreateReader(
        const std::shared_ptr<arrow::DataType>& type, const GlobalIndexIOMeta& meta) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> serializer,
                               KeySerializer::Create(type, pool_));
        return BitmapIndexReader::Create(serializer, file_manager_, meta, pool_);
    }

    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::shared_ptr<GlobalIndexFileManager> file_manager_;
    std::shared_ptr<BlockCompressionFactory> compression_factory_;
};

TEST_F(BitmapIndexReaderTest, WriteAndReadStringPredicates) {
    std::shared_ptr<arrow::Field> field = arrow::field("f0", arrow::utf8());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapGlobalIndexWriter> writer, CreateWriter(field));
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({field}),
            R"([["apple"], ["apple"], [null], ["banana"], ["band"], ["cab"], [null]])")
            .ValueOrDie();
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> metas, writer->Finish());
    ASSERT_EQ(1, metas.size());
    ASSERT_TRUE(metas[0].metadata);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapIndexReader> reader,
                         CreateReader(field->type(), metas[0]));
    CheckResult(reader->VisitIsNull(), {2, 6});
    CheckResult(reader->VisitIsNotNull(), {0, 1, 3, 4, 5});
    CheckResult(reader->VisitEqual(StringLiteral("apple")), {0, 1});
    CheckResult(reader->VisitEqual(StringLiteral("missing")), {});
    CheckResult(
        reader->VisitIn({StringLiteral("apple"), StringLiteral("apple"), StringLiteral("cab")}),
        {0, 1, 5});
    CheckResult(reader->VisitNotEqual(StringLiteral("banana")), {0, 1, 4, 5});
    CheckResult(reader->VisitNotIn({StringLiteral("apple"), StringLiteral("cab")}), {3, 4});

    CheckResult(reader->VisitLessThan(StringLiteral("band")), {0, 1, 3});
    CheckResult(reader->VisitLessOrEqual(StringLiteral("band")), {0, 1, 3, 4});
    CheckResult(reader->VisitGreaterThan(StringLiteral("banana")), {4, 5});
    CheckResult(reader->VisitGreaterOrEqual(StringLiteral("banana")), {3, 4, 5});

    CheckResult(reader->VisitStartsWith(StringLiteral("ban")), {3, 4});
    CheckResult(reader->VisitEndsWith(StringLiteral("le")), {0, 1});
    CheckResult(reader->VisitContains(StringLiteral("an")), {3, 4});
    CheckResult(reader->VisitLike(StringLiteral("ban_n_")), {3});
    ASSERT_OK(reader->Close());
}

TEST_F(BitmapIndexReaderTest, WriteAndReadLogicalIntegerOrder) {
    std::shared_ptr<arrow::Field> field = arrow::field("f0", arrow::int32());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapGlobalIndexWriter> writer,
                         CreateWriter(field, /*dictionary_block_size=*/8));
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}),
                                                  R"([[-257], [-1], [0], [1], [256], [1000]])")
            .ValueOrDie();
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    std::vector<int64_t> row_ids(array->length());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    ASSERT_OK(writer->AddBatch(&c_array, std::move(row_ids)));
    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> metas, writer->Finish());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapIndexReader> reader,
                         CreateReader(field->type(), metas[0]));
    CheckResult(reader->VisitEqual(Literal(static_cast<int32_t>(256))), {4});
    CheckResult(reader->VisitLessThan(Literal(static_cast<int32_t>(1))), {0, 1, 2});
    CheckResult(reader->VisitGreaterOrEqual(Literal(static_cast<int32_t>(0))), {2, 3, 4, 5});
}

TEST_F(BitmapIndexReaderTest, RejectsDescendingKeys) {
    std::shared_ptr<arrow::Field> field = arrow::field("f0", arrow::int32());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapGlobalIndexWriter> writer, CreateWriter(field));
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([[2], [1]])")
            .ValueOrDie();
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    ASSERT_NOK_WITH_MSG(writer->AddBatch(&c_array, {0, 1}), "monotonically increasing");
}

TEST_F(BitmapIndexReaderTest, EmptyWriterCreatesNoFile) {
    std::shared_ptr<arrow::Field> field = arrow::field("f0", arrow::utf8());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapGlobalIndexWriter> writer, CreateWriter(field));
    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> metas, writer->Finish());
    ASSERT_TRUE(metas.empty());
}

}  // namespace paimon::test

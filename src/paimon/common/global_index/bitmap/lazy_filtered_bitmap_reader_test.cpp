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

#include "paimon/common/global_index/bitmap/lazy_filtered_bitmap_reader.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/global_index/bitmap/bitmap_global_index_writer.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/testing/mock/mock_index_path_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

Literal StringLiteral(const std::string& value) {
    return Literal(FieldType::STRING, value.data(), value.size());
}

class CountingGlobalIndexFileReader : public GlobalIndexFileReader {
 public:
    explicit CountingGlobalIndexFileReader(std::shared_ptr<GlobalIndexFileReader> delegate)
        : delegate_(std::move(delegate)) {}

    Result<std::unique_ptr<InputStream>> GetInputStream(
        const std::string& file_path) const override {
        ++open_count_;
        return delegate_->GetInputStream(file_path);
    }

    int32_t OpenCount() const {
        return open_count_;
    }

 private:
    std::shared_ptr<GlobalIndexFileReader> delegate_;
    mutable int32_t open_count_ = 0;
};

}  // namespace

class LazyFilteredBitmapReaderTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
        test_dir_ = UniqueTestDirectory::Create("local");
        ASSERT_TRUE(test_dir_);
        file_manager_ = std::make_shared<GlobalIndexFileManager>(
            test_dir_->GetFileSystem(), std::make_shared<MockIndexPathFactory>(test_dir_->Str()),
            /*checkpoint_path_factory=*/nullptr);
        ASSERT_OK_AND_ASSIGN(compression_factory_,
                             BlockCompressionFactory::Create(BlockCompressionType::NONE));
    }

    GlobalIndexIOMeta WriteSingleFile(const std::string& json,
                                      const std::vector<int64_t>& row_ids) {
        std::shared_ptr<arrow::Field> field = arrow::field("tag", arrow::utf8());
        std::shared_ptr<arrow::StructType> struct_type =
            std::static_pointer_cast<arrow::StructType>(arrow::struct_({field}));
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<BitmapGlobalIndexWriter> writer,
                             BitmapGlobalIndexWriter::Create(
                                 field->name(), struct_type, file_manager_,
                                 /*dictionary_block_size=*/32, compression_factory_, pool_));
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(struct_type, json).ValueOrDie();
        ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        EXPECT_OK(writer->AddBatch(&c_array, std::vector<int64_t>(row_ids)));
        EXPECT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> metas, writer->Finish());
        EXPECT_EQ(1, metas.size());
        return metas[0];
    }

    std::shared_ptr<LazyFilteredBitmapReader> CreateReader(
        const std::vector<GlobalIndexIOMeta>& metas, int64_t fallback_scan_max_size,
        const std::shared_ptr<GlobalIndexFileReader>& file_reader = nullptr) {
        EXPECT_OK_AND_ASSIGN(
            std::shared_ptr<LazyFilteredBitmapReader> reader,
            LazyFilteredBitmapReader::Create(file_reader == nullptr ? file_manager_ : file_reader,
                                             metas, arrow::utf8(), fallback_scan_max_size, pool_,
                                             /*executor=*/nullptr));
        return reader;
    }

    static void CheckResult(const Result<std::shared_ptr<GlobalIndexResult>>& result,
                            const std::vector<int64_t>& expected) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> index_result, result);
        ASSERT_TRUE(index_result);
        std::shared_ptr<BitmapGlobalIndexResult> bitmap_result =
            std::dynamic_pointer_cast<BitmapGlobalIndexResult>(index_result);
        ASSERT_TRUE(bitmap_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, bitmap_result->GetBitmap());
        ASSERT_EQ(RoaringBitmap64::From(expected), *bitmap);
    }

    static void CheckUnsupported(const Result<std::shared_ptr<GlobalIndexResult>>& result) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> index_result, result);
        ASSERT_FALSE(index_result);
    }

    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::shared_ptr<GlobalIndexFileManager> file_manager_;
    std::shared_ptr<BlockCompressionFactory> compression_factory_;
};

TEST_F(LazyFilteredBitmapReaderTest, MultiFileLookupAndManifestPruning) {
    GlobalIndexIOMeta first = WriteSingleFile(R"([["A"], ["B"], [null]])", {0, 1, 2});
    GlobalIndexIOMeta second = WriteSingleFile(R"([["Y"], ["Z"]])", {3, 4});
    auto counting_reader = std::make_shared<CountingGlobalIndexFileReader>(file_manager_);
    std::shared_ptr<LazyFilteredBitmapReader> reader =
        CreateReader({first, second}, std::numeric_limits<int64_t>::max(), counting_reader);

    CheckResult(reader->VisitEqual(StringLiteral("Z")), {4});
    ASSERT_EQ(1, counting_reader->OpenCount());

    CheckResult(reader->VisitStartsWith(StringLiteral("Z")), {4});
    ASSERT_EQ(1, counting_reader->OpenCount());

    CheckResult(reader->VisitIsNull(), {2});
    ASSERT_EQ(2, counting_reader->OpenCount());

    CheckResult(reader->VisitNotEqual(StringLiteral("A")), {1, 3, 4});
    ASSERT_EQ(2, counting_reader->OpenCount());
}

TEST_F(LazyFilteredBitmapReaderTest, FallbackBudgetUsesSelectedFiles) {
    GlobalIndexIOMeta first = WriteSingleFile(R"([["A"], ["B"]])", {0, 1});
    GlobalIndexIOMeta second = WriteSingleFile(R"([["Y"], ["Z"]])", {2, 3});
    std::shared_ptr<LazyFilteredBitmapReader> reader =
        CreateReader({first, second}, second.file_size);

    CheckResult(reader->VisitGreaterOrEqual(StringLiteral("Y")), {2, 3});
    CheckUnsupported(reader->VisitContains(StringLiteral("Z")));
}

TEST_F(LazyFilteredBitmapReaderTest, FallbackScanDisabledDoesNotDisableDirectLookup) {
    GlobalIndexIOMeta meta = WriteSingleFile(R"([["alpha"], ["alphabet"], ["beta"]])", {0, 1, 2});
    std::shared_ptr<LazyFilteredBitmapReader> reader =
        CreateReader({meta}, /*fallback_scan_max_size=*/0);

    CheckResult(reader->VisitEqual(StringLiteral("beta")), {2});
    CheckResult(reader->VisitStartsWith(StringLiteral("alpha")), {0, 1});
    CheckResult(reader->VisitLike(StringLiteral("beta")), {2});
    CheckResult(reader->VisitLike(StringLiteral("alpha%")), {0, 1});

    CheckUnsupported(reader->VisitLessThan(StringLiteral("beta")));
    CheckUnsupported(reader->VisitEndsWith(StringLiteral("ta")));
    CheckUnsupported(reader->VisitContains(StringLiteral("ph")));
    CheckUnsupported(reader->VisitLike(StringLiteral("%ha%")));
    CheckUnsupported(reader->VisitLike(StringLiteral("a_pha")));
}

TEST_F(LazyFilteredBitmapReaderTest, FallbackStringAndRangePredicates) {
    GlobalIndexIOMeta meta = WriteSingleFile(
        R"([["alpha"], ["alphabet"], ["beta"], ["delta"], [null]])", {0, 1, 2, 3, 4});
    std::shared_ptr<LazyFilteredBitmapReader> reader =
        CreateReader({meta}, std::numeric_limits<int64_t>::max());

    CheckResult(reader->VisitEndsWith(StringLiteral("ta")), {2, 3});
    CheckResult(reader->VisitContains(StringLiteral("ph")), {0, 1});
    CheckResult(reader->VisitLike(StringLiteral("%ha%")), {0, 1});
    CheckResult(reader->VisitLessThan(StringLiteral("delta")), {0, 1, 2});
    CheckResult(reader->VisitLessOrEqual(StringLiteral("beta")), {0, 1, 2});
    CheckResult(reader->VisitGreaterThan(StringLiteral("beta")), {3});
    CheckResult(reader->VisitGreaterOrEqual(StringLiteral("beta")), {2, 3});
}

}  // namespace paimon::test

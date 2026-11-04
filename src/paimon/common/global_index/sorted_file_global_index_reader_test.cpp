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

#include "paimon/common/global_index/sorted_file_global_index_reader.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/global_index/key_serializer.h"
#include "paimon/common/global_index/sorted_index_file_meta.h"
#include "paimon/testing/mock/mock_global_index_reader.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class TestSortedFileGlobalIndexReader : public SortedFileGlobalIndexReader {
 public:
    TestSortedFileGlobalIndexReader(std::unique_ptr<SortedFileMetaSelector> selector,
                                    int64_t fallback_scan_max_size,
                                    const std::shared_ptr<GlobalIndexReader>& reader)
        : SortedFileGlobalIndexReader(std::move(selector), fallback_scan_max_size,
                                      /*total_row_count=*/3, /*executor=*/nullptr),
          reader_(reader) {}

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& search) override {
        return reader_->VisitVectorSearch(search);
    }

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& search) override {
        return reader_->VisitFullTextSearch(search);
    }

    std::string GetIndexType() const override {
        return reader_->GetIndexType();
    }

 protected:
    Result<std::shared_ptr<GlobalIndexReader>> OpenReader(const GlobalIndexIOMeta&) override {
        return reader_;
    }

 private:
    std::shared_ptr<GlobalIndexReader> reader_;
};

}  // namespace

class SortedFileGlobalIndexReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        auto pool = GetDefaultPool();
        ASSERT_OK_AND_ASSIGN(serializer_, KeySerializer::Create(arrow::utf8(), pool));
        ASSERT_OK_AND_ASSIGN(auto first,
                             serializer_->Serialize(Literal(FieldType::STRING, "a", 1)));
        ASSERT_OK_AND_ASSIGN(auto last, serializer_->Serialize(Literal(FieldType::STRING, "z", 1)));
        SortedIndexFileMeta meta(first, last, /*has_nulls=*/true);
        files_.emplace_back("index", /*file_size=*/1, /*row_count=*/3, meta.Serialize(pool.get()));
        backend_ = std::make_shared<MockGlobalIndexReader>();
        backend_->SetReturnNullptr();
    }

    std::unique_ptr<TestSortedFileGlobalIndexReader> MakeReader(int64_t fallback_scan_max_size) {
        EXPECT_OK_AND_ASSIGN(auto selector, SortedFileMetaSelector::Create(files_, serializer_));
        return std::make_unique<TestSortedFileGlobalIndexReader>(std::move(selector),
                                                                 fallback_scan_max_size, backend_);
    }

 protected:
    std::shared_ptr<KeySerializer> serializer_;
    std::vector<GlobalIndexIOMeta> files_;
    std::shared_ptr<MockGlobalIndexReader> backend_;
};

TEST_F(SortedFileGlobalIndexReaderTest, UnsupportedResultsRemainUnavailableForComplements) {
    auto reader = MakeReader(/*fallback_scan_max_size=*/100);
    ASSERT_OK_AND_ASSIGN(auto not_null, reader->VisitIsNotNull());
    ASSERT_EQ(not_null, nullptr);
    const Literal value(FieldType::STRING, "m", 1);
    ASSERT_OK_AND_ASSIGN(auto not_equal, reader->VisitNotEqual(value));
    ASSERT_EQ(not_equal, nullptr);
    ASSERT_OK_AND_ASSIGN(auto not_in, reader->VisitNotIn({value}));
    ASSERT_EQ(not_in, nullptr);
}

TEST_F(SortedFileGlobalIndexReaderTest, NullLiteralsDoNotProduceIndexResults) {
    auto reader = MakeReader(/*fallback_scan_max_size=*/100);
    const Literal null_literal(FieldType::STRING);
    for (auto visit :
         {&SortedFileGlobalIndexReader::VisitLessThan,
          &SortedFileGlobalIndexReader::VisitLessOrEqual,
          &SortedFileGlobalIndexReader::VisitGreaterThan,
          &SortedFileGlobalIndexReader::VisitGreaterOrEqual,
          &SortedFileGlobalIndexReader::VisitStartsWith,
          &SortedFileGlobalIndexReader::VisitEndsWith, &SortedFileGlobalIndexReader::VisitContains,
          &SortedFileGlobalIndexReader::VisitLike}) {
        ASSERT_OK_AND_ASSIGN(auto result, (reader.get()->*visit)(null_literal));
        ASSERT_EQ(result, nullptr);
    }
}

TEST_F(SortedFileGlobalIndexReaderTest, DisabledFallbackLeavesScanPredicatesUnavailable) {
    auto reader = MakeReader(/*fallback_scan_max_size=*/0);
    const Literal value(FieldType::STRING, "a_b", 3);
    for (auto visit :
         {&SortedFileGlobalIndexReader::VisitLessThan,
          &SortedFileGlobalIndexReader::VisitLessOrEqual,
          &SortedFileGlobalIndexReader::VisitGreaterThan,
          &SortedFileGlobalIndexReader::VisitGreaterOrEqual,
          &SortedFileGlobalIndexReader::VisitEndsWith, &SortedFileGlobalIndexReader::VisitContains,
          &SortedFileGlobalIndexReader::VisitLike}) {
        ASSERT_OK_AND_ASSIGN(auto result, (reader.get()->*visit)(value));
        ASSERT_EQ(result, nullptr);
    }
}

}  // namespace paimon::test

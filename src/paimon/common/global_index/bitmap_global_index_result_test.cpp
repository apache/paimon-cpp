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
#include "paimon/global_index/bitmap_global_index_result.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"
namespace paimon::test {
class BitmapGlobalIndexResultTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    class FakeGlobalIndexResult : public GlobalIndexResult {
     public:
        explicit FakeGlobalIndexResult(const std::vector<int64_t>& values) : values_(values) {}
        class Iterator : public GlobalIndexResult::Iterator {
         public:
            Iterator(const std::vector<int64_t>* values,
                     const std::vector<int64_t>::const_iterator& iter)
                : values_(values), iter_(iter) {}
            bool HasNext() const override {
                return iter_ != values_->end();
            }
            int64_t Next() override {
                int64_t value = *iter_;
                iter_++;
                return value;
            }
            const std::vector<int64_t>* values_;
            std::vector<int64_t>::const_iterator iter_;
        };

        Result<std::unique_ptr<GlobalIndexResult::Iterator>> CreateIterator() const override {
            auto iter = values_.begin();
            return std::make_unique<Iterator>(&values_, iter);
        }

        std::string ToString() const override {
            return "fake";
        }

        Result<bool> IsEmpty() const override {
            return values_.empty();
        }

        Result<std::shared_ptr<GlobalIndexResult>> AddOffset(int64_t offset) override {
            std::vector<int64_t> values = values_;
            for (auto& value : values) {
                value += offset;
            }
            return std::make_shared<FakeGlobalIndexResult>(values);
        }

     private:
        std::vector<int64_t> values_;
    };
};
TEST_F(BitmapGlobalIndexResultTest, TestIterator) {
    auto check_iterator = [](const std::vector<int64_t>& expected_ids) {
        auto bitmap_supplier = [&]() -> Result<RoaringBitmap64> {
            return RoaringBitmap64::From(expected_ids);
        };
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapGlobalIndexResult> index_result,
                             BitmapGlobalIndexResult::Create(bitmap_supplier));
        if (expected_ids.empty()) {
            ASSERT_TRUE(index_result->IsEmpty().value());
        }

        ASSERT_OK_AND_ASSIGN(auto iter, index_result->CreateIterator());
        std::vector<int64_t> result_ids;
        while (iter->HasNext()) {
            result_ids.push_back(iter->Next());
        }
        ASSERT_EQ(result_ids, expected_ids);
    };

    check_iterator({});
    check_iterator({1, 4, 7, RoaringBitmap32::MAX_VALUE, RoaringBitmap64::MAX_VALUE});
    check_iterator({100, 101, 102, 103});
}

TEST_F(BitmapGlobalIndexResultTest, TestAnd) {
    auto check_and_result = [](const std::vector<int64_t>& left, const std::vector<int64_t>& right,
                               const std::string& expected_str) {
        auto bitmap_supplier1 = [&]() -> Result<RoaringBitmap64> {
            return RoaringBitmap64::From(left);
        };
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapGlobalIndexResult> index_result1,
                             BitmapGlobalIndexResult::Create(bitmap_supplier1));
        auto bitmap_supplier2 = [&]() -> Result<RoaringBitmap64> {
            return RoaringBitmap64::From(right);
        };
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapGlobalIndexResult> index_result2,
                             BitmapGlobalIndexResult::Create(bitmap_supplier2));

        ASSERT_OK_AND_ASSIGN(auto result, index_result1->And(index_result2));
        ASSERT_EQ(result->ToString(), expected_str);
    };
    check_and_result({1, 2, 3}, {1, 2, 7}, "{1,2}");
    check_and_result({1, 2, 3}, {1, 2, 3}, "{1,2,3}");
    check_and_result({1, 2, 3}, {100, 200, 300}, "{}");
    check_and_result({1, 2, 3}, {}, "{}");
    check_and_result({}, {}, "{}");
    check_and_result({1, 2, 3, RoaringBitmap64::MAX_VALUE}, {1, RoaringBitmap64::MAX_VALUE},
                     "{1,9223372036854775807}");
}

TEST_F(BitmapGlobalIndexResultTest, TestBitmapResultAndOtherResult) {
    auto bitmap_supplier1 = [&]() -> Result<RoaringBitmap64> {
        return RoaringBitmap64::From({1, 2, 3});
    };
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapGlobalIndexResult> index_result1,
                         BitmapGlobalIndexResult::Create(bitmap_supplier1));

    auto fake_result = std::make_shared<FakeGlobalIndexResult>(std::vector<int64_t>({1l, 2l, 7l}));

    ASSERT_OK_AND_ASSIGN(auto result, index_result1->And(fake_result));
    ASSERT_EQ(result->ToString(), "{1,2}");
}

TEST_F(BitmapGlobalIndexResultTest, TestOr) {
    auto check_and_result = [](const std::vector<int64_t>& left, const std::vector<int64_t>& right,
                               const std::string& expected_str) {
        auto bitmap_supplier1 = [&]() -> Result<RoaringBitmap64> {
            return RoaringBitmap64::From(left);
        };
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapGlobalIndexResult> index_result1,
                             BitmapGlobalIndexResult::Create(bitmap_supplier1));
        auto bitmap_supplier2 = [&]() -> Result<RoaringBitmap64> {
            return RoaringBitmap64::From(right);
        };
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapGlobalIndexResult> index_result2,
                             BitmapGlobalIndexResult::Create(bitmap_supplier2));

        ASSERT_OK_AND_ASSIGN(auto result, index_result1->Or(index_result2));
        ASSERT_EQ(result->ToString(), expected_str);
    };
    check_and_result({1, 2, 3}, {1, 2, 7}, "{1,2,3,7}");
    check_and_result({1, 2, 3}, {1, 2, 3}, "{1,2,3}");
    check_and_result({1, 2, 3}, {100, 200, 300}, "{1,2,3,100,200,300}");
    check_and_result({1, 2, 3}, {}, "{1,2,3}");
    check_and_result({}, {}, "{}");
    check_and_result({1, 2, 3, RoaringBitmap64::MAX_VALUE}, {1, RoaringBitmap64::MAX_VALUE},
                     "{1,2,3,9223372036854775807}");
}

TEST_F(BitmapGlobalIndexResultTest, TestOperationsDoNotRetainOperands) {
    auto left = std::make_shared<BitmapGlobalIndexResult>(RoaringBitmap64::From({1, 2}));
    auto right = std::make_shared<BitmapGlobalIndexResult>(RoaringBitmap64::From({2, 3}));
    std::weak_ptr<GlobalIndexResult> weak_left = left;
    std::weak_ptr<GlobalIndexResult> weak_right = right;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> intersection, left->And(right));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> union_result, left->Or(right));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> offset_result, left->AddOffset(10));
    left.reset();
    right.reset();
    ASSERT_TRUE(weak_left.expired());
    ASSERT_TRUE(weak_right.expired());
    ASSERT_EQ(intersection->ToString(), "{2}");
    ASSERT_EQ(union_result->ToString(), "{1,2,3}");
    ASSERT_EQ(offset_result->ToString(), "{11,12}");
}

TEST_F(BitmapGlobalIndexResultTest, TestBitmapResultOrOtherResult) {
    auto bitmap_supplier1 = [&]() -> Result<RoaringBitmap64> {
        return RoaringBitmap64::From({1, 2, 3});
    };
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BitmapGlobalIndexResult> index_result1,
                         BitmapGlobalIndexResult::Create(bitmap_supplier1));

    auto fake_result = std::make_shared<FakeGlobalIndexResult>(std::vector<int64_t>({1l, 2l, 7l}));

    ASSERT_OK_AND_ASSIGN(auto result, index_result1->Or(fake_result));
    ASSERT_EQ(result->ToString(), "{1,2,3,7}");
}

TEST_F(BitmapGlobalIndexResultTest, TestInvalidBitmapResult) {
    auto bitmap_supplier = [&]() -> Result<RoaringBitmap64> {
        return Status::Invalid("invalid supplier");
    };
    ASSERT_NOK_WITH_MSG(BitmapGlobalIndexResult::Create(bitmap_supplier), "invalid supplier");
}

TEST_F(BitmapGlobalIndexResultTest, TestCreateMaterializesAndReleasesSupplier) {
    auto source = std::make_shared<RoaringBitmap64>(RoaringBitmap64::From({1, 4, 7}));
    std::weak_ptr<RoaringBitmap64> weak_source = source;
    int32_t supplier_calls = 0;
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BitmapGlobalIndexResult> result,
        BitmapGlobalIndexResult::Create(
            [source = std::move(source), &supplier_calls]() -> Result<RoaringBitmap64> {
                ++supplier_calls;
                return *source;
            }));
    ASSERT_EQ(supplier_calls, 1);
    ASSERT_TRUE(weak_source.expired());
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* first, result->GetBitmap());
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* second, result->GetBitmap());
    ASSERT_EQ(first, second);
    ASSERT_EQ(result->ToString(), "{1,4,7}");
    ASSERT_OK_AND_ASSIGN(bool empty, result->IsEmpty());
    ASSERT_FALSE(empty);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<GlobalIndexResult::Iterator> iterator,
                         result->CreateIterator());
    ASSERT_TRUE(iterator->HasNext());
    ASSERT_EQ(iterator->Next(), 1);
    ASSERT_EQ(supplier_calls, 1);
}

TEST_F(BitmapGlobalIndexResultTest, TestFromRanges) {
    {
        auto result = BitmapGlobalIndexResult::FromRanges({Range(0, 5)});
        ASSERT_EQ(result->ToString(), "{0,1,2,3,4,5}");
    }
    {
        auto result = BitmapGlobalIndexResult::FromRanges({Range(10, 10)});
        ASSERT_EQ(result->ToString(), "{10}");
    }
    {
        auto result = BitmapGlobalIndexResult::FromRanges({Range(0, 5), Range(10, 10)});
        ASSERT_EQ(result->ToString(), "{0,1,2,3,4,5,10}");
    }
}

TEST_F(BitmapGlobalIndexResultTest, TestAddOffset) {
    {
        int32_t supplier_calls = 0;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<BitmapGlobalIndexResult> result,
            BitmapGlobalIndexResult::Create([&supplier_calls]() -> Result<RoaringBitmap64> {
                ++supplier_calls;
                return RoaringBitmap64::From({0, 1, 2, 3, 4, 5});
            }));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> result_with_offset,
                             result->AddOffset(0));
        ASSERT_EQ(result_with_offset.get(), result.get());
        ASSERT_EQ(supplier_calls, 1);
        ASSERT_EQ(result_with_offset->ToString(), "{0,1,2,3,4,5}");
        ASSERT_EQ(supplier_calls, 1);

        ASSERT_OK_AND_ASSIGN(result_with_offset, result->AddOffset(10));
        ASSERT_EQ(result_with_offset->ToString(), "{10,11,12,13,14,15}");
    }
    {
        auto result = BitmapGlobalIndexResult::FromRanges({});
        ASSERT_OK_AND_ASSIGN(auto result_with_offset, result->AddOffset(10));
        ASSERT_EQ(result_with_offset->ToString(), "{}");
    }
}
}  // namespace paimon::test

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

#include "paimon/data/blob_view.h"

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BlobViewTest : public testing::Test {
 public:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    BlobView blob_view_{Identifier("default", "source"), /*field_id=*/7, /*row_id=*/5};
};

TEST_F(BlobViewTest, TestFieldsAndEquality) {
    ASSERT_EQ(blob_view_.identifier, Identifier("default", "source"));
    ASSERT_EQ(blob_view_.field_id, 7);
    ASSERT_EQ(blob_view_.row_id, 5);

    BlobView equal{Identifier("default", "source"), /*field_id=*/7, /*row_id=*/5};
    BlobView different_identifier{Identifier("other", "source"), /*field_id=*/7, /*row_id=*/5};
    BlobView different_field{Identifier("default", "source"), /*field_id=*/8, /*row_id=*/5};
    BlobView different_row{Identifier("default", "source"), /*field_id=*/7, /*row_id=*/6};
    ASSERT_EQ(blob_view_, equal);
    ASSERT_NE(blob_view_, different_identifier);
    ASSERT_NE(blob_view_, different_field);
    ASSERT_NE(blob_view_, different_row);
}

TEST_F(BlobViewTest, TestToString) {
    ASSERT_EQ(blob_view_.ToString(), "BlobView{identifier=default.source, fieldId=7, rowId=5}");
}

TEST_F(BlobViewTest, TestRoundTrip) {
    ASSERT_OK_AND_ASSIGN(PAIMON_UNIQUE_PTR<Bytes> serialized, BlobView::ToView(blob_view_, pool_));
    ASSERT_OK_AND_ASSIGN(BlobView restored,
                         BlobView::FromView(serialized->data(), serialized->size()));
    ASSERT_EQ(restored, blob_view_);
}

TEST_F(BlobViewTest, TestJavaSerializationCompatibility) {
    // Serialized by Java BlobViewStruct(Identifier.fromString("default.source"), 7, 5L).
    std::vector<char> java_serialized = {
        1,  87,  69,  73,  86,  66, 79,  76, 66, 14, 0, 0, 0, 100, 101, 102, 97, 117, 108, 116,
        46, 115, 111, 117, 114, 99, 101, 7,  0,  0,  0, 5, 0, 0,   0,   0,   0,  0,   0};

    ASSERT_OK_AND_ASSIGN(BlobView restored,
                         BlobView::FromView(java_serialized.data(), java_serialized.size()));
    ASSERT_EQ(restored, blob_view_);

    ASSERT_OK_AND_ASSIGN(PAIMON_UNIQUE_PTR<Bytes> cpp_serialized,
                         BlobView::ToView(blob_view_, pool_));
    ASSERT_EQ(std::string(cpp_serialized->data(), cpp_serialized->size()),
              std::string(java_serialized.data(), java_serialized.size()));
}

TEST_F(BlobViewTest, TestToViewRejectsInvalidArguments) {
    BlobView unknown_database{Identifier("source"), /*field_id=*/7, /*row_id=*/5};
    ASSERT_NOK_WITH_MSG(BlobView::ToView(unknown_database, pool_),
                        "upstream table identifier must include database name");
    ASSERT_NOK_WITH_MSG(BlobView::ToView(blob_view_, /*pool=*/nullptr), "memory pool is nullptr");
}

}  // namespace paimon::test

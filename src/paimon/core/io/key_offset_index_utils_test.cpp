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

#include "paimon/core/io/key_offset_index_utils.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/global_index/key_serializer.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"
#include "xxhash.h"  // NOLINT(build/include_subdir)

namespace paimon::test {
namespace {

std::shared_ptr<arrow::StructArray> MakeKeys(const std::shared_ptr<arrow::Field>& field,
                                             const std::string& json) {
    return checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), json).ValueOrDie());
}

}  // namespace

TEST(KeyOffsetIndexUtilsTest, TestIntegerHashesMatchSerializedKeysAndPreserveOrder) {
    const std::shared_ptr<arrow::Field> field = arrow::field("id", arrow::int64());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<KeySerializer> serializer,
                         KeySerializer::Create(field->type(), GetDefaultPool()));
    const std::shared_ptr<arrow::StructArray> keys = MakeKeys(field, "[[42], [-7], [42], [0]]");
    ASSERT_OK_AND_ASSIGN(std::vector<uint64_t> hashes,
                         KeyOffsetIndexUtils::HashKeys(keys, field, serializer));
    std::vector<uint64_t> expected;
    for (int64_t value : {42, -7, 42, 0}) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, serializer->Serialize(Literal(value)));
        expected.push_back(XXH64(bytes->data(), bytes->size(), /*seed=*/0));
    }
    ASSERT_EQ(expected, hashes);

    const std::shared_ptr<arrow::StructArray> slice =
        checked_pointer_cast<arrow::StructArray>(keys->Slice(1, 2));
    ASSERT_OK_AND_ASSIGN(std::vector<uint64_t> sliced_hashes,
                         KeyOffsetIndexUtils::HashKeys(slice, field, serializer));
    const std::vector<uint64_t> expected_slice = {expected[1], expected[2]};
    ASSERT_EQ(expected_slice, sliced_hashes);
}

TEST(KeyOffsetIndexUtilsTest, TestStringKeysAndEmptyBatch) {
    const std::shared_ptr<arrow::Field> field = arrow::field("id", arrow::utf8());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<KeySerializer> serializer,
                         KeySerializer::Create(field->type(), GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(
        std::vector<uint64_t> hashes,
        KeyOffsetIndexUtils::HashKeys(MakeKeys(field, R"([[""], ["alpha"], ["alpha"]])"), field,
                                      serializer));
    const std::vector<uint64_t> expected = {XXH64("", 0, /*seed=*/0), XXH64("alpha", 5, /*seed=*/0),
                                            XXH64("alpha", 5, /*seed=*/0)};
    ASSERT_EQ(expected, hashes);
    ASSERT_OK_AND_ASSIGN(hashes,
                         KeyOffsetIndexUtils::HashKeys(MakeKeys(field, "[]"), field, serializer));
    ASSERT_TRUE(hashes.empty());
}

TEST(KeyOffsetIndexUtilsTest, TestRejectsInvalidKeyBatches) {
    const std::shared_ptr<arrow::Field> field = arrow::field("id", arrow::int64());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<KeySerializer> serializer,
                         KeySerializer::Create(field->type(), GetDefaultPool()));
    ASSERT_NOK_WITH_MSG(KeyOffsetIndexUtils::HashKeys(nullptr, field, serializer),
                        "exactly the configured key field");
    const std::shared_ptr<arrow::StructArray> multiple_fields =
        checked_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({field, arrow::field("other", arrow::int64())}), "[[1, 2]]")
                .ValueOrDie());
    ASSERT_NOK_WITH_MSG(KeyOffsetIndexUtils::HashKeys(multiple_fields, field, serializer),
                        "exactly the configured key field");
    ASSERT_NOK_WITH_MSG(
        KeyOffsetIndexUtils::HashKeys(MakeKeys(arrow::field("id", arrow::utf8()), R"([["1"]])"),
                                      field, serializer),
        "exactly the configured key field");
    ASSERT_NOK_WITH_MSG(
        KeyOffsetIndexUtils::HashKeys(MakeKeys(field, "[[null]]"), field, serializer),
        "deduplicate key must not contain null");
    ASSERT_NOK_WITH_MSG(KeyOffsetIndexUtils::HashKeys(MakeKeys(field, "[null]"), field, serializer),
                        "deduplicate key must not contain null");
}

}  // namespace paimon::test

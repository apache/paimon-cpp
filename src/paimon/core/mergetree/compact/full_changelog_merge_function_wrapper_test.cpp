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

#include "paimon/core/mergetree/compact/full_changelog_merge_function_wrapper.h"

#include <memory>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/internal_row_equalizer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

constexpr int32_t MAX_LEVEL = 3;

KeyValue MakeKeyValue(const RowKind* kind, int64_t sequence_number, int32_t level, int32_t key,
                      int32_t value, const std::shared_ptr<MemoryPool>& pool) {
    return KeyValue(kind, sequence_number, level,
                    BinaryRowGenerator::GenerateRowPtr({key}, pool.get()),
                    BinaryRowGenerator::GenerateRowPtr({value}, pool.get()));
}

std::unique_ptr<RowCompactedSerializer> CreateValueSerializer(
    const std::shared_ptr<MemoryPool>& pool) {
    return RowCompactedSerializer::Create(arrow::schema({arrow::field("value", arrow::int32())}),
                                          pool)
        .value();
}

std::unique_ptr<FullChangelogMergeFunctionWrapper> CreateWrapper(
    const std::shared_ptr<MemoryPool>& pool,
    FieldsComparator::FieldComparatorFunc value_equalizer = {}) {
    return std::make_unique<FullChangelogMergeFunctionWrapper>(
        std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false), MAX_LEVEL,
        CreateValueSerializer(pool), std::move(value_equalizer));
}

void CheckKeyValue(const KeyValue& actual, const RowKind* kind, int64_t sequence_number,
                   int32_t level, int32_t value) {
    ASSERT_EQ(kind, actual.value_kind);
    ASSERT_EQ(sequence_number, actual.sequence_number);
    ASSERT_EQ(level, actual.level);
    ASSERT_EQ(value, actual.value->GetInt(0));
}

}  // namespace

TEST(FullChangelogMergeFunctionWrapperTest, TestSingleRecord) {
    auto pool = GetDefaultPool();
    auto wrapper = CreateWrapper(pool);

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(
        MakeKeyValue(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0, 1, 10, pool)));
    ASSERT_OK_AND_ASSIGN(std::optional<ChangelogResult> insert_result, wrapper->GetResult());
    ASSERT_TRUE(insert_result);
    ASSERT_TRUE(insert_result->result);
    ASSERT_EQ(1, insert_result->changelogs.size());
    CheckKeyValue(insert_result->changelogs[0], RowKind::Insert(), 1, KeyValue::UNKNOWN_LEVEL, 10);
    CheckKeyValue(*insert_result->result, RowKind::Insert(), 1, 0, 10);

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(
        MakeKeyValue(RowKind::Delete(), /*sequence_number=*/2, /*level=*/0, 2, 20, pool)));
    ASSERT_OK_AND_ASSIGN(std::optional<ChangelogResult> delete_result, wrapper->GetResult());
    ASSERT_TRUE(delete_result);
    ASSERT_FALSE(delete_result->result);
    ASSERT_TRUE(delete_result->changelogs.empty());

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(
        MakeKeyValue(RowKind::Insert(), /*sequence_number=*/3, MAX_LEVEL, 3, 30, pool)));
    ASSERT_OK_AND_ASSIGN(std::optional<ChangelogResult> top_level_result, wrapper->GetResult());
    ASSERT_TRUE(top_level_result);
    ASSERT_TRUE(top_level_result->result);
    ASSERT_TRUE(top_level_result->changelogs.empty());
    CheckKeyValue(*top_level_result->result, RowKind::Insert(), 3, MAX_LEVEL, 30);
}

TEST(FullChangelogMergeFunctionWrapperTest, TestInsertUpdateAndDelete) {
    auto pool = GetDefaultPool();
    auto wrapper = CreateWrapper(pool);

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(
        MakeKeyValue(RowKind::Insert(), /*sequence_number=*/1, MAX_LEVEL, 1, 10, pool)));
    ASSERT_OK(wrapper->Add(
        MakeKeyValue(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0, 1, 20, pool)));
    ASSERT_OK_AND_ASSIGN(std::optional<ChangelogResult> update_result, wrapper->GetResult());
    ASSERT_TRUE(update_result);
    ASSERT_TRUE(update_result->result);
    ASSERT_EQ(2, update_result->changelogs.size());
    CheckKeyValue(update_result->changelogs[0], RowKind::UpdateBefore(), 1, KeyValue::UNKNOWN_LEVEL,
                  10);
    CheckKeyValue(update_result->changelogs[1], RowKind::UpdateAfter(), 2, KeyValue::UNKNOWN_LEVEL,
                  20);
    CheckKeyValue(*update_result->result, RowKind::Insert(), 2, 0, 20);

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(
        MakeKeyValue(RowKind::Insert(), /*sequence_number=*/3, MAX_LEVEL, 2, 30, pool)));
    ASSERT_OK(wrapper->Add(
        MakeKeyValue(RowKind::Delete(), /*sequence_number=*/4, /*level=*/0, 2, 30, pool)));
    ASSERT_OK_AND_ASSIGN(std::optional<ChangelogResult> delete_result, wrapper->GetResult());
    ASSERT_TRUE(delete_result);
    ASSERT_FALSE(delete_result->result);
    ASSERT_EQ(1, delete_result->changelogs.size());
    CheckKeyValue(delete_result->changelogs[0], RowKind::Delete(), 3, KeyValue::UNKNOWN_LEVEL, 30);

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(
        MakeKeyValue(RowKind::Insert(), /*sequence_number=*/5, /*level=*/0, 3, 40, pool)));
    ASSERT_OK(wrapper->Add(
        MakeKeyValue(RowKind::Insert(), /*sequence_number=*/6, /*level=*/0, 3, 50, pool)));
    ASSERT_OK_AND_ASSIGN(std::optional<ChangelogResult> insert_result, wrapper->GetResult());
    ASSERT_TRUE(insert_result);
    ASSERT_TRUE(insert_result->result);
    ASSERT_EQ(1, insert_result->changelogs.size());
    CheckKeyValue(insert_result->changelogs[0], RowKind::Insert(), 6, KeyValue::UNKNOWN_LEVEL, 50);
    CheckKeyValue(*insert_result->result, RowKind::Insert(), 6, 0, 50);
}

TEST(FullChangelogMergeFunctionWrapperTest, TestRowDeduplicate) {
    auto pool = GetDefaultPool();

    auto wrapper_without_deduplicate = CreateWrapper(pool);
    wrapper_without_deduplicate->Reset();
    ASSERT_OK(wrapper_without_deduplicate->Add(
        MakeKeyValue(RowKind::Insert(), /*sequence_number=*/1, MAX_LEVEL, 1, 10, pool)));
    ASSERT_OK(wrapper_without_deduplicate->Add(
        MakeKeyValue(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0, 1, 10, pool)));
    ASSERT_OK_AND_ASSIGN(std::optional<ChangelogResult> result_without_deduplicate,
                         wrapper_without_deduplicate->GetResult());
    ASSERT_TRUE(result_without_deduplicate);
    ASSERT_EQ(2, result_without_deduplicate->changelogs.size());

    auto value_schema = arrow::schema({arrow::field("value", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(FieldsComparator::FieldComparatorFunc value_equalizer,
                         InternalRowEqualizer::Create(value_schema, /*ignore_fields=*/{}));
    auto wrapper = CreateWrapper(pool, std::move(value_equalizer));

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(
        MakeKeyValue(RowKind::Insert(), /*sequence_number=*/1, MAX_LEVEL, 1, 10, pool)));
    ASSERT_OK(wrapper->Add(
        MakeKeyValue(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0, 1, 10, pool)));
    ASSERT_OK_AND_ASSIGN(std::optional<ChangelogResult> result, wrapper->GetResult());
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    ASSERT_TRUE(result->changelogs.empty());
    CheckKeyValue(*result->result, RowKind::Insert(), 2, 0, 10);
}

TEST(FullChangelogMergeFunctionWrapperTest, TestRowDeduplicateWithIgnoreFields) {
    auto pool = GetDefaultPool();
    auto value_schema = arrow::schema(
        {arrow::field("value", arrow::int32()), arrow::field("ignored", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(FieldsComparator::FieldComparatorFunc value_equalizer,
                         InternalRowEqualizer::Create(value_schema, {"ignored"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RowCompactedSerializer> value_serializer,
                         RowCompactedSerializer::Create(value_schema, pool));
    FullChangelogMergeFunctionWrapper wrapper(
        std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false), MAX_LEVEL,
        std::move(value_serializer), std::move(value_equalizer));

    wrapper.Reset();
    ASSERT_OK(wrapper.Add(KeyValue(RowKind::Insert(), /*sequence_number=*/1, MAX_LEVEL,
                                   BinaryRowGenerator::GenerateRowPtr({1}, pool.get()),
                                   BinaryRowGenerator::GenerateRowPtr({10, 1}, pool.get()))));
    ASSERT_OK(wrapper.Add(KeyValue(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0,
                                   BinaryRowGenerator::GenerateRowPtr({1}, pool.get()),
                                   BinaryRowGenerator::GenerateRowPtr({10, 2}, pool.get()))));
    ASSERT_OK_AND_ASSIGN(std::optional<ChangelogResult> ignored_field_result, wrapper.GetResult());
    ASSERT_TRUE(ignored_field_result);
    ASSERT_TRUE(ignored_field_result->result);
    ASSERT_TRUE(ignored_field_result->changelogs.empty());
    ASSERT_EQ(10, ignored_field_result->result->value->GetInt(0));
    ASSERT_EQ(2, ignored_field_result->result->value->GetInt(1));

    wrapper.Reset();
    ASSERT_OK(wrapper.Add(KeyValue(RowKind::Insert(), /*sequence_number=*/3, MAX_LEVEL,
                                   BinaryRowGenerator::GenerateRowPtr({1}, pool.get()),
                                   BinaryRowGenerator::GenerateRowPtr({10, 1}, pool.get()))));
    ASSERT_OK(wrapper.Add(KeyValue(RowKind::Insert(), /*sequence_number=*/4, /*level=*/0,
                                   BinaryRowGenerator::GenerateRowPtr({1}, pool.get()),
                                   BinaryRowGenerator::GenerateRowPtr({11, 2}, pool.get()))));
    ASSERT_OK_AND_ASSIGN(std::optional<ChangelogResult> value_field_result, wrapper.GetResult());
    ASSERT_TRUE(value_field_result);
    ASSERT_TRUE(value_field_result->result);
    ASSERT_EQ(2, value_field_result->changelogs.size());
    ASSERT_EQ(RowKind::UpdateBefore(), value_field_result->changelogs[0].value_kind);
    ASSERT_EQ(RowKind::UpdateAfter(), value_field_result->changelogs[1].value_kind);
}

TEST(FullChangelogMergeFunctionWrapperTest, TestRejectMultipleTopLevelRecords) {
    auto pool = GetDefaultPool();
    auto wrapper = CreateWrapper(pool);

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(
        MakeKeyValue(RowKind::Insert(), /*sequence_number=*/1, MAX_LEVEL, 1, 10, pool)));
    ASSERT_NOK_WITH_MSG(wrapper->Add(MakeKeyValue(RowKind::Insert(), /*sequence_number=*/2,
                                                  MAX_LEVEL, 1, 20, pool)),
                        "Top level key-value already exists");
}

}  // namespace paimon::test

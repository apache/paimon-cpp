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

#include "paimon/core/mergetree/compact/lookup_changelog_merge_function_wrapper.h"

#include <memory>
#include <variant>

#include "gtest/gtest.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"
#include "paimon/core/mergetree/compact/aggregate/aggregate_merge_function.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/internal_row_equalizer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
std::unique_ptr<RowCompactedSerializer> CreateValueSerializer(
    const std::shared_ptr<MemoryPool>& pool) {
    return RowCompactedSerializer::Create(arrow::schema({arrow::field("value", arrow::int32())}),
                                          pool)
        .value();
}
}  // namespace

TEST(LookupChangelogMergeFunctionWrapperTest, TestCreateInvalid) {
    auto pool = GetDefaultPool();
    auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/true);
    auto lookup_mfunc = std::make_unique<LookupMergeFunction>(std::move(mfunc));
    auto lookup = [&](const std::shared_ptr<InternalRow>& key) -> Result<std::optional<KeyValue>> {
        return std::optional<KeyValue>(
            KeyValue(RowKind::Insert(), /*sequence_number=*/1000, /*level=*/3, key,
                     BinaryRowGenerator::GenerateRowPtr({1001}, pool.get())));
    };
    LookupStrategy lookup_strategy(/*is_first_row=*/false, /*produce_changelog=*/false,
                                   /*deletion_vector=*/true, /*force_lookup=*/true);
    ASSERT_NOK_WITH_MSG(LookupChangelogMergeFunctionWrapper<KeyValue>::Create(
                            std::move(lookup_mfunc), lookup, lookup_strategy,
                            /*should_produce_changelog=*/false,
                            /*deletion_vectors_maintainer=*/nullptr,
                            /*comparator=*/nullptr, CreateValueSerializer(pool),
                            /*value_equalizer=*/{}),
                        "deletionVectorsMaintainer should not be null, there is a bug.");
}

TEST(LookupChangelogMergeFunctionWrapperTest, TestSimple) {
    auto pool = GetDefaultPool();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/0, /*level=*/2, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/1, /*level=*/1,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({300}, pool.get()));

    auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/true);
    auto lookup_mfunc = std::make_unique<LookupMergeFunction>(std::move(mfunc));
    auto lookup = [&](const std::shared_ptr<InternalRow>& key) -> Result<std::optional<KeyValue>> {
        return std::optional<KeyValue>(
            KeyValue(RowKind::Insert(), /*sequence_number=*/1000, /*level=*/3, key,
                     BinaryRowGenerator::GenerateRowPtr({1001}, pool.get())));
    };
    LookupStrategy lookup_strategy(/*is_first_row=*/false, /*produce_changelog=*/true,
                                   /*deletion_vector=*/false, /*force_lookup=*/true);
    ASSERT_OK_AND_ASSIGN(auto wrapper, LookupChangelogMergeFunctionWrapper<KeyValue>::Create(
                                           std::move(lookup_mfunc), lookup, lookup_strategy,
                                           /*should_produce_changelog=*/false,
                                           /*deletion_vectors_maintainer=*/nullptr,
                                           /*comparator=*/nullptr,
                                           /*value_serializer=*/nullptr,
                                           /*value_equalizer=*/{}));

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(std::move(kv1)));
    ASSERT_OK(wrapper->Add(std::move(kv2)));
    ASSERT_OK(wrapper->Add(std::move(kv3)));
    ASSERT_OK_AND_ASSIGN(auto result, wrapper->GetResult());
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    ASSERT_EQ(result->result->sequence_number, 2);
    ASSERT_TRUE(result->changelogs.empty());
}

TEST(LookupChangelogMergeFunctionWrapperTest, TestWithLookup) {
    auto pool = GetDefaultPool();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/3, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({300}, pool.get()));

    auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/true);
    auto lookup_mfunc = std::make_unique<LookupMergeFunction>(std::move(mfunc));
    auto lookup = [&](const std::shared_ptr<InternalRow>& key) -> Result<std::optional<KeyValue>> {
        return std::optional<KeyValue>(
            KeyValue(RowKind::Insert(), /*sequence_number=*/0, /*level=*/3, key,
                     BinaryRowGenerator::GenerateRowPtr({1001}, pool.get())));
    };
    LookupStrategy lookup_strategy(/*is_first_row=*/false, /*produce_changelog=*/true,
                                   /*deletion_vector=*/false, /*force_lookup=*/true);
    ASSERT_OK_AND_ASSIGN(auto wrapper, LookupChangelogMergeFunctionWrapper<KeyValue>::Create(
                                           std::move(lookup_mfunc), lookup, lookup_strategy,
                                           /*should_produce_changelog=*/true,
                                           /*deletion_vectors_maintainer=*/nullptr,
                                           /*comparator=*/nullptr, CreateValueSerializer(pool),
                                           /*value_equalizer=*/{}));

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(std::move(kv1)));
    ASSERT_OK(wrapper->Add(std::move(kv2)));
    ASSERT_OK(wrapper->Add(std::move(kv3)));
    ASSERT_OK_AND_ASSIGN(auto result, wrapper->GetResult());
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    ASSERT_EQ(result->result->sequence_number, 3);
    ASSERT_EQ(result->changelogs.size(), 2);
    ASSERT_EQ(result->changelogs[0].value_kind, RowKind::UpdateBefore());
    ASSERT_EQ(result->changelogs[0].level, KeyValue::UNKNOWN_LEVEL);
    ASSERT_EQ(result->changelogs[0].value->GetInt(0), 1001);
    ASSERT_EQ(result->changelogs[1].value_kind, RowKind::UpdateAfter());
    ASSERT_EQ(result->changelogs[1].level, KeyValue::UNKNOWN_LEVEL);
    ASSERT_EQ(result->changelogs[1].value->GetInt(0), 300);
}

TEST(LookupChangelogMergeFunctionWrapperTest, TestRowDeduplicate) {
    auto pool = GetDefaultPool();
    KeyValue kv(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0,
                BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                BinaryRowGenerator::GenerateRowPtr({300}, pool.get()));

    auto merge_function = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/true);
    auto lookup_merge_function = std::make_unique<LookupMergeFunction>(std::move(merge_function));
    auto lookup = [&](const std::shared_ptr<InternalRow>& key) -> Result<std::optional<KeyValue>> {
        return std::optional<KeyValue>(
            KeyValue(RowKind::Insert(), /*sequence_number=*/0, /*level=*/3, key,
                     BinaryRowGenerator::GenerateRowPtr({300}, pool.get())));
    };
    LookupStrategy lookup_strategy(/*is_first_row=*/false, /*produce_changelog=*/true,
                                   /*deletion_vector=*/false, /*force_lookup=*/true);
    auto value_equalizer = [](const InternalRow& lhs, const InternalRow& rhs) {
        return lhs.GetInt(0) == rhs.GetInt(0) ? 0 : 1;
    };
    ASSERT_OK_AND_ASSIGN(auto wrapper,
                         LookupChangelogMergeFunctionWrapper<KeyValue>::Create(
                             std::move(lookup_merge_function), lookup, lookup_strategy,
                             /*should_produce_changelog=*/true,
                             /*deletion_vectors_maintainer=*/nullptr,
                             /*comparator=*/nullptr, CreateValueSerializer(pool), value_equalizer));

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(std::move(kv)));
    ASSERT_OK_AND_ASSIGN(auto result, wrapper->GetResult());
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    ASSERT_TRUE(result->changelogs.empty());
}

TEST(LookupChangelogMergeFunctionWrapperTest, TestRowDeduplicateWithIgnoreFields) {
    auto pool = GetDefaultPool();
    auto value_schema = arrow::schema(
        {arrow::field("value", arrow::int32()), arrow::field("ignored", arrow::int32())});
    auto merge_function = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/true);
    auto lookup_merge_function = std::make_unique<LookupMergeFunction>(std::move(merge_function));
    auto lookup = [&](const std::shared_ptr<InternalRow>& key) -> Result<std::optional<KeyValue>> {
        return std::optional<KeyValue>(
            KeyValue(RowKind::Insert(), /*sequence_number=*/0, /*level=*/3, key,
                     BinaryRowGenerator::GenerateRowPtr({300, 1}, pool.get())));
    };
    LookupStrategy lookup_strategy(/*is_first_row=*/false, /*produce_changelog=*/true,
                                   /*deletion_vector=*/false, /*force_lookup=*/true);
    ASSERT_OK_AND_ASSIGN(auto value_equalizer,
                         InternalRowEqualizer::Create(value_schema, {"ignored"}));
    ASSERT_OK_AND_ASSIGN(auto value_serializer, RowCompactedSerializer::Create(value_schema, pool));
    ASSERT_OK_AND_ASSIGN(auto wrapper,
                         LookupChangelogMergeFunctionWrapper<KeyValue>::Create(
                             std::move(lookup_merge_function), lookup, lookup_strategy,
                             /*should_produce_changelog=*/true,
                             /*deletion_vectors_maintainer=*/nullptr,
                             /*comparator=*/nullptr, std::move(value_serializer), value_equalizer));

    // A change to an ignored field does not produce an update changelog.
    wrapper->Reset();
    ASSERT_OK(wrapper->Add(KeyValue(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0,
                                    BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                                    BinaryRowGenerator::GenerateRowPtr({300, 2}, pool.get()))));
    ASSERT_OK_AND_ASSIGN(auto ignored_field_result, wrapper->GetResult());
    ASSERT_TRUE(ignored_field_result);
    ASSERT_TRUE(ignored_field_result->result);
    ASSERT_TRUE(ignored_field_result->changelogs.empty());

    // A change to a non-ignored field still produces update-before and update-after.
    wrapper->Reset();
    ASSERT_OK(wrapper->Add(KeyValue(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0,
                                    BinaryRowGenerator::GenerateRowPtr({20}, pool.get()),
                                    BinaryRowGenerator::GenerateRowPtr({301, 2}, pool.get()))));
    ASSERT_OK_AND_ASSIGN(auto value_field_result, wrapper->GetResult());
    ASSERT_TRUE(value_field_result);
    ASSERT_TRUE(value_field_result->result);
    ASSERT_EQ(value_field_result->changelogs.size(), 2);
    ASSERT_EQ(value_field_result->changelogs[0].value_kind, RowKind::UpdateBefore());
    ASSERT_EQ(value_field_result->changelogs[0].value->GetInt(0), 300);
    ASSERT_EQ(value_field_result->changelogs[0].value->GetInt(1), 1);
    ASSERT_EQ(value_field_result->changelogs[1].value_kind, RowKind::UpdateAfter());
    ASSERT_EQ(value_field_result->changelogs[1].value->GetInt(0), 301);
    ASSERT_EQ(value_field_result->changelogs[1].value->GetInt(1), 2);
}

TEST(LookupChangelogMergeFunctionWrapperTest, TestWithLookupWithDv) {
    auto pool = GetDefaultPool();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/3, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({300}, pool.get()));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::FIELDS_DEFAULT_AGG_FUNC, "sum"}}));
    ASSERT_OK_AND_ASSIGN(auto mfunc, AggregateMergeFunction::Create(
                                         arrow::schema({arrow::field("value", arrow::int32())}),
                                         {"key"}, core_options, GetDefaultPool()));
    auto lookup_mfunc = std::make_unique<LookupMergeFunction>(std::move(mfunc));
    auto lookup =
        [&](const std::shared_ptr<InternalRow>& key) -> Result<std::optional<PositionedKeyValue>> {
        return std::optional<PositionedKeyValue>(
            {KeyValue(RowKind::Insert(), /*sequence_number=*/0, /*level=*/3, key,
                      BinaryRowGenerator::GenerateRowPtr({1001}, pool.get())),
             "data.file", /*row_position=*/10});
    };
    auto dv_index_file =
        std::make_shared<DeletionVectorsIndexFile>(/*fs=*/nullptr, /*path_factory=*/nullptr,
                                                   /*bitmap64=*/false, pool);
    std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors;
    auto dv_maintainer = std::make_shared<BucketedDvMaintainer>(dv_index_file, deletion_vectors);

    LookupStrategy lookup_strategy(/*is_first_row=*/false, /*produce_changelog=*/false,
                                   /*deletion_vector=*/true, /*force_lookup=*/false);
    ASSERT_OK_AND_ASSIGN(auto wrapper,
                         LookupChangelogMergeFunctionWrapper<PositionedKeyValue>::Create(
                             std::move(lookup_mfunc), lookup, lookup_strategy,
                             /*should_produce_changelog=*/false, dv_maintainer,
                             /*comparator=*/nullptr,
                             /*value_serializer=*/nullptr,
                             /*value_equalizer=*/{}));

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(std::move(kv1)));
    ASSERT_OK(wrapper->Add(std::move(kv2)));
    ASSERT_OK(wrapper->Add(std::move(kv3)));
    ASSERT_OK_AND_ASSIGN(auto result, wrapper->GetResult());
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    ASSERT_EQ(result->result->sequence_number, 3);
    ASSERT_EQ(result->result->value->GetInt(0), 100 + 200 + 300 + 1001);

    auto dv = dv_maintainer->DeletionVectorOf("data.file");
    ASSERT_TRUE(dv);
    ASSERT_FALSE(dv.value()->IsDeleted(0).value());
    ASSERT_TRUE(dv.value()->IsDeleted(10).value());
}

TEST(LookupChangelogMergeFunctionWrapperTest, TestWithLookupWithDvAndChangelog) {
    auto pool = GetDefaultPool();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/3, /*level=*/0,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({300}, pool.get()));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::FIELDS_DEFAULT_AGG_FUNC, "sum"}}));
    ASSERT_OK_AND_ASSIGN(auto mfunc, AggregateMergeFunction::Create(
                                         arrow::schema({arrow::field("value", arrow::int32())}),
                                         {"key"}, core_options, pool));
    auto lookup_mfunc = std::make_unique<LookupMergeFunction>(std::move(mfunc));
    auto lookup =
        [&](const std::shared_ptr<InternalRow>& key) -> Result<std::optional<PositionedKeyValue>> {
        return std::optional<PositionedKeyValue>(
            {KeyValue(RowKind::Insert(), /*sequence_number=*/0, /*level=*/3, key,
                      BinaryRowGenerator::GenerateRowPtr({1001}, pool.get())),
             "data.file", /*row_position=*/10});
    };
    auto dv_index_file =
        std::make_shared<DeletionVectorsIndexFile>(/*fs=*/nullptr, /*path_factory=*/nullptr,
                                                   /*bitmap64=*/false, pool);
    std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors;
    auto dv_maintainer = std::make_shared<BucketedDvMaintainer>(dv_index_file, deletion_vectors);

    LookupStrategy lookup_strategy(/*is_first_row=*/false, /*produce_changelog=*/true,
                                   /*deletion_vector=*/true, /*force_lookup=*/false);
    ASSERT_OK_AND_ASSIGN(auto wrapper,
                         LookupChangelogMergeFunctionWrapper<PositionedKeyValue>::Create(
                             std::move(lookup_mfunc), lookup, lookup_strategy,
                             /*should_produce_changelog=*/true, dv_maintainer,
                             /*comparator=*/nullptr, CreateValueSerializer(pool),
                             /*value_equalizer=*/{}));

    wrapper->Reset();
    ASSERT_OK(wrapper->Add(std::move(kv1)));
    ASSERT_OK(wrapper->Add(std::move(kv2)));
    ASSERT_OK(wrapper->Add(std::move(kv3)));
    ASSERT_OK_AND_ASSIGN(auto result, wrapper->GetResult());
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->result);
    ASSERT_EQ(result->result->sequence_number, 3);
    ASSERT_EQ(result->result->value->GetInt(0), 100 + 200 + 300 + 1001);
    ASSERT_EQ(result->changelogs.size(), 2);
    ASSERT_EQ(result->changelogs[0].value_kind, RowKind::UpdateBefore());
    ASSERT_EQ(result->changelogs[0].sequence_number, 0);
    ASSERT_EQ(result->changelogs[0].value->GetInt(0), 1001);
    ASSERT_EQ(result->changelogs[1].value_kind, RowKind::UpdateAfter());
    ASSERT_EQ(result->changelogs[1].sequence_number, 3);
    ASSERT_EQ(result->changelogs[1].value->GetInt(0), 100 + 200 + 300 + 1001);

    auto dv = dv_maintainer->DeletionVectorOf("data.file");
    ASSERT_TRUE(dv);
    ASSERT_FALSE(dv.value()->IsDeleted(0).value());
    ASSERT_TRUE(dv.value()->IsDeleted(10).value());
}

TEST(LookupChangelogMergeFunctionWrapperTest, TestCreateSequenceComparator) {
    auto pool = GetDefaultPool();
    {
        // test no user_defined_seq_comparator
        auto cmp = LookupChangelogMergeFunctionWrapper<bool>::CreateSequenceComparator(
            /*user_defined_seq_comparator=*/nullptr);
        KeyValue kv1(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0, /*key=*/
                     BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                     /*value=*/BinaryRowGenerator::GenerateRowPtr({500}, pool.get()));
        KeyValue kv2(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0,
                     /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                     /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
        ASSERT_TRUE(cmp(kv1, kv2));
        ASSERT_FALSE(cmp(kv2, kv1));
    }
    {
        // test with user_defined_seq_comparator
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FieldsComparator> user_defined_seq_comparator,
            FieldsComparator::Create({DataField(1, arrow::field("value", arrow::int32()))},
                                     /*is_ascending_order=*/true));
        auto cmp = LookupChangelogMergeFunctionWrapper<bool>::CreateSequenceComparator(
            user_defined_seq_comparator);
        KeyValue kv1(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0, /*key=*/
                     BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                     /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
        KeyValue kv2(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0,
                     /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                     /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
        ASSERT_TRUE(cmp(kv1, kv2));
        ASSERT_FALSE(cmp(kv2, kv1));

        // same sequence field, compare sequence number
        KeyValue kv3(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0, /*key=*/
                     BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                     /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
        KeyValue kv4(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0,
                     /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                     /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
        ASSERT_TRUE(cmp(kv3, kv4));
        ASSERT_FALSE(cmp(kv4, kv3));
    }
}
}  // namespace paimon::test

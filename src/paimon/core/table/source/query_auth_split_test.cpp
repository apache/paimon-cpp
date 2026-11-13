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

#include "paimon/core/table/source/query_auth_split.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/catalog/table_query_auth_result.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/data/timestamp.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/table/source/split.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/range.h"

namespace paimon::test {

namespace {

Result<std::string> ReadFixture(const std::string& name) {
    LocalFileSystem file_system;
    std::string bytes;
    PAIMON_RETURN_NOT_OK(file_system.ReadFile(GetDataDir() + "/query_auth/" + name, &bytes));
    return bytes;
}

Result<std::shared_ptr<DataSplitImpl>> FixtureDataSplit() {
    auto file = std::make_shared<DataFileMeta>(
        "file1.orc", 100l, 200l, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
        SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), 50l, 249l, 0, 0,
        std::vector<std::optional<std::string>>(), Timestamp(1765535214349l, 0), 0, nullptr,
        FileSource::Append(), std::nullopt, std::nullopt, 50l, std::nullopt,
        /*column_max_sequence_numbers=*/std::nullopt);
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0, "data/test_table/bucket-0",
                                   std::vector<std::shared_ptr<DataFileMeta>>({file}));
    return builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build();
}

}  // namespace

TEST(QueryAuthSplitTest, TestDeserializeSplitsGeneratedByJava) {
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> expected_data_split, FixtureDataSplit());
    const std::string id_ref = R"({"index": 0, "name": "id", "type": "INT NOT NULL"})";
    const std::string name_ref = R"({"index": 1, "name": "name", "type": "STRING"})";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableQueryAuthResult> expected_rules,
        TableQueryAuthResult::Create(
            {fmt::format(R"({{"kind": "LEAF", "transform": {{"name": "FIELD_REF", "fieldRef": )"
                         R"({}}}, "function": "GREATER_THAN", "literals": [1]}})",
                         id_ref),
             fmt::format(R"({{"kind": "LEAF", "transform": {{"name": "FIELD_REF", "fieldRef": )"
                         R"({}}}, "function": "NOT_EQUAL", "literals": ["测试"]}})",
                         name_ref)},
            {}));
    auto batch = checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({arrow::field("id", arrow::int32(), /*nullable=*/false),
                            arrow::field("name", arrow::utf8()),
                            arrow::field("phone", arrow::utf8()),
                            arrow::field("salary", arrow::int64())}),
            R"([[2, "bob", "13900002222", 200]])")
            .ValueOrDie());

    for (const std::string name :
         {"query_auth_split-data_split", "query_auth_split-indexed_split"}) {
        SCOPED_TRACE(name);
        ASSERT_OK_AND_ASSIGN(std::string bytes, ReadFixture(name));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> split,
                             Split::Deserialize(bytes.data(), bytes.size(), pool));
        auto auth_split = std::dynamic_pointer_cast<QueryAuthSplit>(split);
        ASSERT_NE(nullptr, auth_split);
        ASSERT_FALSE(auth_split->GetScope());
        if (auto indexed = std::dynamic_pointer_cast<IndexedSplitImpl>(auth_split->GetSplit())) {
            ASSERT_TRUE(indexed->TEST_Equal(IndexedSplitImpl(
                expected_data_split, std::vector<Range>{Range(55, 56)}, std::vector<float>{0.5f})));
        } else {
            auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(auth_split->GetSplit());
            ASSERT_NE(nullptr, data_split);
            ASSERT_TRUE(data_split->TEST_Equal(*expected_data_split));
        }
        const TableQueryAuthResult& rules = *auth_split->GetAuthResult();
        ASSERT_TRUE(rules.SameFilter(*expected_rules));
        ASSERT_EQ((std::set<std::string>{"name", "phone", "salary"}), rules.MaskedFields());
        for (const auto& [column, expected] : std::vector<std::pair<std::string, std::string>>{
                 {"name", R"(["名字"])"}, {"phone", R"(["139"])"}}) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> masked,
                                 rules.Masks().at(column)->Evaluate(*batch, arrow::utf8(),
                                                                    arrow::default_memory_pool()));
            ASSERT_TRUE(masked->Equals(
                *arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), expected).ValueOrDie()))
                << column << ": " << masked->ToString();
        }
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> salary,
                             rules.Masks().at("salary")->Evaluate(*batch, arrow::int64(),
                                                                  arrow::default_memory_pool()));
        ASSERT_EQ(1, salary->null_count());
        ASSERT_OK_AND_ASSIGN(std::string written, Split::Serialize(split, pool));
        ASSERT_EQ(bytes, written);
        const std::string with_tail = bytes + std::string(8, '\0');
        ASSERT_NOK_WITH_MSG(Split::Deserialize(with_tail.data(), with_tail.size(), pool),
                            "remaining 8 bytes after deserializing");
    }

    ASSERT_OK_AND_ASSIGN(std::string empty_rules, ReadFixture("query_auth_split-empty_rules"));
    ASSERT_OK_AND_ASSIGN(std::string null_rules, ReadFixture("query_auth_split-null_rules"));
    for (const std::string* bytes : {&empty_rules, &null_rules}) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> split,
                             Split::Deserialize(bytes->data(), bytes->size(), pool));
        auto auth_split = std::dynamic_pointer_cast<QueryAuthSplit>(split);
        ASSERT_NE(nullptr, auth_split);
        ASSERT_FALSE(auth_split->GetAuthResult()->HasRules());
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(auth_split->GetSplit());
        ASSERT_NE(nullptr, data_split);
        ASSERT_TRUE(data_split->TEST_Equal(*expected_data_split));
        ASSERT_OK_AND_ASSIGN(std::string written, Split::Serialize(split, pool));
        ASSERT_EQ(empty_rules, written);
    }
    ASSERT_NE(empty_rules, null_rules);

    const size_t empty_auth_size = 3 + 2 * sizeof(int32_t);
    for (int8_t flag : {static_cast<int8_t>(2), static_cast<int8_t>(-1)}) {
        std::string noncanonical_flags = empty_rules;
        for (size_t offset : {empty_auth_size, empty_auth_size - 1, sizeof(int32_t) + 1}) {
            noncanonical_flags[noncanonical_flags.size() - offset] = static_cast<char>(flag);
        }
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<Split> split,
            Split::Deserialize(noncanonical_flags.data(), noncanonical_flags.size(), pool));
        ASSERT_OK_AND_ASSIGN(std::string written, Split::Serialize(split, pool));
        ASSERT_EQ(empty_rules, written);
    }

    ASSERT_OK_AND_ASSIGN(std::string null_auth_result,
                         ReadFixture("query_auth_split-null_auth_result"));
    ASSERT_NOK_WITH_MSG(Split::Deserialize(null_auth_result.data(), null_auth_result.size(), pool),
                        "invalid QueryAuthSplit, it carries no rules");
}

TEST(QueryAuthSplitTest, TestReadsNestedIndexedSplitAsJava) {
    ASSERT_OK_AND_ASSIGN(std::string fixture, ReadFixture("query_auth_split-indexed_split"));
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> split,
                         Split::Deserialize(fixture.data(), fixture.size(), pool));
    auto auth_split = std::dynamic_pointer_cast<QueryAuthSplit>(split);
    ASSERT_NE(nullptr, auth_split);
    auto indexed = std::dynamic_pointer_cast<IndexedSplitImpl>(auth_split->GetSplit());
    ASSERT_NE(nullptr, indexed);
    ASSERT_EQ(1u, indexed->RowRanges().size());
    ASSERT_EQ(1u, indexed->Scores().size());
    ASSERT_OK_AND_ASSIGN(std::string data_bytes, Split::Serialize(indexed->GetDataSplit(), pool));
    for (const std::optional<QueryAuthScope>& scope :
         {std::optional<QueryAuthScope>(), std::optional<QueryAuthScope>(QueryAuthScope())}) {
        ASSERT_OK_AND_ASSIGN(std::string bytes,
                             Split::Serialize(std::make_shared<QueryAuthSplit>(
                                                  indexed, auth_split->GetAuthResult(), scope),
                                              pool));
        const size_t range_count_offset = (scope ? 12 : 0) + 2 * 16 + 12 + data_bytes.size();
        const size_t score_count_offset = range_count_offset + 4 + 2 * sizeof(int64_t) + 1;
        for (size_t offset : {range_count_offset, score_count_offset}) {
            for (const std::string& count :
                 {std::string(4, '\xFF'), std::string("\x00\x00\x04\x00", 4)}) {
                SCOPED_TRACE(offset);
                std::string invalid = bytes;
                invalid.replace(offset, count.size(), count);
                ASSERT_NOK_WITH_MSG(Split::Deserialize(invalid.data(), invalid.size(), pool),
                                    "invalid IndexedSplit");
            }
        }
    }

    auto original_data = std::dynamic_pointer_cast<DataSplitImpl>(indexed->GetDataSplit());
    ASSERT_NE(nullptr, original_data);
    DataSplitImpl::Builder builder(
        original_data->Partition(), original_data->Bucket(), original_data->BucketPath(),
        std::vector<std::shared_ptr<DataFileMeta>>(original_data->DataFiles()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> data,
                         builder.WithSnapshot(original_data->SnapshotId())
                             .WithTotalBuckets(8)
                             .IsStreaming(true)
                             .RawConvertible(true)
                             .Build());
    auto with_total_buckets =
        std::make_shared<IndexedSplitImpl>(data, indexed->RowRanges(), indexed->Scores());
    ASSERT_OK_AND_ASSIGN(
        std::string bytes,
        Split::Serialize(std::make_shared<QueryAuthSplit>(
                             with_total_buckets, auth_split->GetAuthResult(), std::nullopt),
                         pool));
    ASSERT_OK_AND_ASSIGN(data_bytes, Split::Serialize(data, pool));
    const size_t data_start = 2 * 16 + 12;
    const size_t data_end = data_start + data_bytes.size();
    const size_t has_total_buckets_offset =
        data_start + 20 + 2 * sizeof(int32_t) + original_data->Partition().GetSizeInBytes() +
        sizeof(int32_t) + sizeof(uint16_t) + original_data->BucketPath().size();
    const size_t has_scores_offset = data_end + 4 + 2 * sizeof(int64_t);
    for (char flag : {'\x02', '\xFF'}) {
        std::string noncanonical = bytes;
        noncanonical[has_total_buckets_offset] = flag;
        noncanonical[data_end - 2] = flag;
        noncanonical[data_end - 1] = flag;
        noncanonical[has_scores_offset] = flag;
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> parsed,
                             Split::Deserialize(noncanonical.data(), noncanonical.size(), pool));
        ASSERT_OK_AND_ASSIGN(std::string written, Split::Serialize(parsed, pool));
        ASSERT_EQ(bytes, written);
    }
}

TEST(QueryAuthSplitTest, TestRoundTripsAndRefusesMalformedBytes) {
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> data_split, FixtureDataSplit());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableQueryAuthResult> rules,
        TableQueryAuthResult::Create(
            {R"({"kind": "LEAF", "transform": {"name": "FIELD_REF", "fieldRef": {"index": 3,)"
             R"( "name": "salary", "type": "BIGINT"}}, "function": "GREATER_THAN",)"
             R"( "literals": [150]})"},
            {{"name", R"({"name": "CONCAT", "inputs": ["名字"]})"}}));
    QueryAuthScope planned_scope;
    planned_scope.table_path = "file:///warehouse/db/table";
    planned_scope.branch = "dev";
    planned_scope.authorized_field_ids = {{"id", 0}, {"name", 1}, {"phone", 2}, {"salary", 3}};
    planned_scope.rule_field_ids = {{"name", 1}, {"salary", 3}};
    auto planned = std::make_shared<QueryAuthSplit>(data_split, rules, planned_scope);

    auto indexed_split = std::make_shared<IndexedSplitImpl>(
        data_split, std::vector<Range>{Range(0, 1)}, std::vector<float>{0.5f});
    for (const std::optional<QueryAuthScope>& scope :
         {planned->GetScope(), std::optional<QueryAuthScope>()}) {
        ASSERT_OK_AND_ASSIGN(std::string bytes,
                             Split::Serialize(std::make_shared<QueryAuthSplit>(
                                                  indexed_split, planned->GetAuthResult(), scope),
                                              pool));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> copy,
                             Split::Deserialize(bytes.data(), bytes.size(), pool));
        auto auth_copy = std::dynamic_pointer_cast<QueryAuthSplit>(copy);
        ASSERT_NE(nullptr, auth_copy);
        auto indexed_copy = std::dynamic_pointer_cast<IndexedSplitImpl>(auth_copy->GetSplit());
        ASSERT_NE(nullptr, indexed_copy);
        ASSERT_TRUE(indexed_copy->TEST_Equal(*indexed_split));
        ASSERT_TRUE(auth_copy->GetAuthResult()->SameFilter(*planned->GetAuthResult()));
        ASSERT_EQ(planned->GetAuthResult()->ColumnMasking(),
                  auth_copy->GetAuthResult()->ColumnMasking());
        ASSERT_EQ(scope, auth_copy->GetScope());
    }

    ASSERT_NOK_WITH_MSG(Split::Serialize(std::make_shared<QueryAuthSplit>(
                                             planned, planned->GetAuthResult(), std::nullopt),
                                         pool),
                        "a QueryAuthSplit can only wrap a DataSplit or an IndexedSplit");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> no_rules_result,
                         TableQueryAuthResult::Create({}, {}));
    ASSERT_OK_AND_ASSIGN(
        std::string no_rules,
        Split::Serialize(
            std::make_shared<QueryAuthSplit>(data_split, no_rules_result, QueryAuthScope()), pool));
    ASSERT_OK(Split::Deserialize(no_rules.data(), no_rules.size(), pool).status());
    const size_t envelope_header_size = 12;
    const size_t empty_scope_size = 4 * sizeof(int32_t);
    const std::string java_format_bytes = no_rules.substr(
        envelope_header_size, no_rules.size() - envelope_header_size - empty_scope_size);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Split> java_split,
        Split::Deserialize(java_format_bytes.data(), java_format_bytes.size(), pool));
    ASSERT_FALSE(std::dynamic_pointer_cast<QueryAuthSplit>(java_split)->GetScope());
    ASSERT_OK_AND_ASSIGN(std::string java_copy, Split::Serialize(java_split, pool));
    ASSERT_EQ(java_format_bytes, java_copy);
    const size_t split_serializer_header_size = 16;
    std::string nested;
    for (int32_t i = 0; i < 100000; ++i) {
        nested += java_format_bytes.substr(0, split_serializer_header_size);
    }
    nested += java_format_bytes.substr(split_serializer_header_size);
    ASSERT_NOK_WITH_MSG(Split::Deserialize(nested.data(), nested.size(), pool),
                        "invalid QueryAuthSplit, it wraps another QueryAuthSplit");

    ASSERT_NOK(Split::Deserialize(no_rules.data(), no_rules.size() - empty_scope_size, pool));
    ASSERT_NOK(Split::Deserialize(no_rules.data(), no_rules.size() - 1, pool));
    std::string negative_count = no_rules;
    negative_count.replace(negative_count.size() - empty_scope_size, 4, std::string(4, '\xFF'));
    ASSERT_NOK_WITH_MSG(Split::Deserialize(negative_count.data(), negative_count.size(), pool),
                        "invalid QueryAuthSplit, -1 entries do not fit the split");
    std::string oversized_count = no_rules;
    oversized_count.replace(oversized_count.size() - empty_scope_size, 4,
                            std::string("\x00\x00\x01\x00", 4));
    ASSERT_NOK_WITH_MSG(Split::Deserialize(oversized_count.data(), oversized_count.size(), pool),
                        "invalid QueryAuthSplit, 256 entries do not fit the split");
    std::string unknown_version = no_rules;
    unknown_version.replace(8, 4, std::string("\x00\x00\x00\x02", 4));
    ASSERT_NOK_WITH_MSG(Split::Deserialize(unknown_version.data(), unknown_version.size(), pool),
                        "Unsupported QueryAuthSplit version: 2");
    const std::string entry(
        "\x00\x00\x00\x01"
        "a\x00\x00\x00\x00",
        9);
    const std::string duplicate_column = no_rules.substr(0, no_rules.size() - empty_scope_size) +
                                         std::string("\x00\x00\x00\x02", 4) + entry + entry +
                                         std::string(3 * sizeof(int32_t), '\0');
    ASSERT_NOK_WITH_MSG(Split::Deserialize(duplicate_column.data(), duplicate_column.size(), pool),
                        "invalid QueryAuthSplit, its scope names a column twice");
    auto auth_string = [](const std::string& value) {
        const auto size = static_cast<uint32_t>(value.size());
        std::string bytes;
        for (int32_t shift = 24; shift >= 0; shift -= 8) {
            bytes.push_back(static_cast<char>(size >> shift));
        }
        return bytes + value;
    };
    const std::string mask_entry = auth_string("name") + auth_string(R"({"name": "NULL"})");
    const std::string without_mask_count =
        java_format_bytes.substr(0, java_format_bytes.size() - sizeof(int32_t));
    const std::string duplicate_mask =
        without_mask_count + std::string("\x00\x00\x00\x02", 4) + mask_entry + mask_entry;
    ASSERT_NOK_WITH_MSG(Split::Deserialize(duplicate_mask.data(), duplicate_mask.size(), pool),
                        "invalid QueryAuthSplit, its rules mask a column twice");
    const std::string one_mask =
        without_mask_count + std::string("\x00\x00\x00\x01", 4) + mask_entry;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> masked_split,
                         Split::Deserialize(one_mask.data(), one_mask.size(), pool));
    ASSERT_EQ(
        (std::map<std::string, std::string>{{"name", R"({"name": "NULL"})"}}),
        std::dynamic_pointer_cast<QueryAuthSplit>(masked_split)->GetAuthResult()->ColumnMasking());
}

}  // namespace paimon::test

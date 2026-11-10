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

#include "paimon/core/table/source/query_auth_table_read.h"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/reader/predicate_batch_reader.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/catalog/table_query_auth_result.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/query_auth_split.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/table/source/split.h"
#include "paimon/table/source/table_read.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

class OneBatchReader : public BatchReader {
 public:
    explicit OneBatchReader(std::shared_ptr<arrow::Array> batch) : batch_(std::move(batch)) {}

    Result<ReadBatch> NextBatch() override {
        if (batch_ == nullptr) {
            return BatchReader::MakeEofBatch();
        }
        auto c_array = std::make_unique<::ArrowArray>();
        auto c_schema = std::make_unique<::ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*batch_, c_array.get(), c_schema.get()));
        batch_.reset();
        return std::make_pair(std::move(c_array), std::move(c_schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return nullptr;
    }

    void Close() override {}

 private:
    std::shared_ptr<arrow::Array> batch_;
};

class CloseCountingReader : public BatchReader {
 public:
    explicit CloseCountingReader(int32_t* closes) : closes_(closes) {}

    Result<ReadBatch> NextBatch() override {
        return BatchReader::MakeEofBatch();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return nullptr;
    }

    void Close() override {
        ++*closes_;
    }

 private:
    int32_t* closes_;
};

class DummySplit final : public Split {};

class SplitTableRead : public TableRead {
 public:
    using ReaderFactory =
        std::function<Result<std::unique_ptr<BatchReader>>(const std::shared_ptr<Split>&)>;

    explicit SplitTableRead(ReaderFactory create_reader)
        : create_reader_(std::move(create_reader)) {}

    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::shared_ptr<Split>& split) override {
        return create_reader_(split);
    }

    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::vector<std::shared_ptr<Split>>& /*splits*/) override {
        return Status::NotImplemented("the query auth read opens one split at a time");
    }

 private:
    ReaderFactory create_reader_;
};

QueryAuthTableRead::InnerReadFactory CreateSplitTableRead(
    SplitTableRead::ReaderFactory create_reader) {
    return [create_reader = std::move(create_reader)](
               std::unique_ptr<ReadContext>) -> Result<std::unique_ptr<TableRead>> {
        return std::unique_ptr<TableRead>(std::make_unique<SplitTableRead>(create_reader));
    };
}

}  // namespace

TEST(QueryAuthTableReadTest, TestBatchReaderDecodesDictionaryAndLargeStringColumns) {
    auto json = [](const std::shared_ptr<arrow::DataType>& type, const std::string& values) {
        return arrow::ipc::internal::json::ArrayFromJSON(type, values).ValueOrDie();
    };
    const auto dictionary_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    std::shared_ptr<arrow::Array> name =
        arrow::DictionaryArray::FromArrays(dictionary_type, json(arrow::int32(), "[0, 1, null]"),
                                           json(arrow::utf8(), R"(["alice", "bob"])"))
            .ValueOrDie();
    std::shared_ptr<arrow::Array> city =
        arrow::DictionaryArray::FromArrays(dictionary_type, json(arrow::int32(), "[0, 0, 0]"),
                                           json(arrow::utf8(), R"(["x"])"))
            .ValueOrDie();
    std::shared_ptr<arrow::Array> nick =
        arrow::DictionaryArray::FromArrays(dictionary_type, json(arrow::int32(), "[1, null, 0]"),
                                           json(arrow::utf8(), R"(["al", "bo"])"))
            .ValueOrDie();
    std::shared_ptr<arrow::Array> phone =
        json(arrow::large_utf8(), R"(["13800001111", "13900002222", null])");
    std::shared_ptr<arrow::Array> batch =
        arrow::StructArray::Make(
            {name, phone, city, nick, city},
            {arrow::field("name", dictionary_type), arrow::field("phone", arrow::large_utf8()),
             arrow::field("city", dictionary_type), arrow::field("nick", dictionary_type),
             arrow::field("alias", dictionary_type)})
            .ValueOrDie();
    const std::string name_ref = R"({"index": 0, "name": "name", "type": "STRING"})";
    const std::string phone_ref = R"({"index": 1, "name": "phone", "type": "STRING"})";
    const std::string nick_ref = R"({"index": 3, "name": "nick", "type": "STRING"})";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableQueryAuthResult> rules,
        TableQueryAuthResult::Create(
            {},
            {{"name", fmt::format(R"({{"name": "SUBSTRING", "inputs": [{}, 1, 1]}})", name_ref)},
             {"phone", fmt::format(R"({{"name": "CONCAT", "inputs": [{}, "-"]}})", phone_ref)},
             {"city", R"({"name": "CONCAT", "inputs": ["***"]})"},
             {"alias", fmt::format(R"({{"name": "FIELD_REF", "fieldRef": {}}})", nick_ref)}}));

    QueryAuthBatchReader reader(std::make_unique<OneBatchReader>(batch), rules->Masks(),
                                /*dropped_fields=*/{}, /*decoded_fields=*/{},
                                GetArrowPool(GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch masked_batch, reader.NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(masked_batch));
    std::shared_ptr<arrow::Array> masked =
        arrow::ImportArray(masked_batch.first.get(), masked_batch.second.get()).ValueOrDie();
    ASSERT_TRUE(masked->ValidateFull().ok()) << masked->ValidateFull().ToString();
    auto masked_struct = checked_pointer_cast<arrow::StructArray>(masked);
    for (const auto& [column, values] : std::vector<std::pair<std::string, std::string>>{
             {"name", R"(["a", "b", null])"},
             {"phone", R"(["13800001111-", "13900002222-", null])"},
             {"city", R"(["***", "***", "***"])"},
             {"alias", R"(["bo", null, "al"])"}}) {
        std::shared_ptr<arrow::Array> field = masked_struct->GetFieldByName(column);
        ASSERT_NE(nullptr, field) << column;
        ASSERT_TRUE(field->Equals(*json(arrow::utf8(), values)))
            << column << ": " << field->ToString();
    }
    ASSERT_TRUE(masked_struct->GetFieldByName("nick")->type()->Equals(*dictionary_type));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof, reader.NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof));

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> name_or_phone,
        PredicateBuilder::Or(
            {PredicateBuilder::Equal(/*field_index=*/0, "name", FieldType::STRING,
                                     Literal(FieldType::STRING, "bob", 3)),
             PredicateBuilder::Equal(/*field_index=*/1, "phone", FieldType::STRING,
                                     Literal(FieldType::STRING, "13800001111", 11))}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PredicateBatchReader> filtered,
        PredicateBatchReader::Create(std::make_unique<QueryAuthBatchReader>(
                                         std::make_unique<OneBatchReader>(batch),
                                         std::map<std::string, std::shared_ptr<MaskTransform>>(),
                                         /*dropped_fields=*/std::set<std::string>(),
                                         /*decoded_fields=*/std::set<std::string>{"name", "phone"},
                                         GetArrowPool(GetDefaultPool())),
                                     name_or_phone, GetArrowPool(GetDefaultPool())));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch filtered_batch, filtered->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(filtered_batch));
    auto filtered_struct = checked_pointer_cast<arrow::StructArray>(
        arrow::ImportArray(filtered_batch.first.get(), filtered_batch.second.get()).ValueOrDie());
    ASSERT_TRUE(filtered_struct->GetFieldByName("name")->Equals(
        *json(arrow::utf8(), R"(["alice", "bob"])")));
    ASSERT_TRUE(filtered_struct->GetFieldByName("phone")->Equals(
        *json(arrow::utf8(), R"(["13800001111", "13900002222"])")));
    ASSERT_TRUE(filtered_struct->GetFieldByName("city")->type()->Equals(*dictionary_type));

    ASSERT_OK_AND_ASSIGN(
        filtered,
        PredicateBatchReader::Create(
            std::make_unique<QueryAuthBatchReader>(
                std::make_unique<OneBatchReader>(batch),
                std::map<std::string, std::shared_ptr<MaskTransform>>(),
                /*dropped_fields=*/std::set<std::string>(),
                /*decoded_fields=*/std::set<std::string>{"name"}, GetArrowPool(GetDefaultPool()),
                /*logical_type=*/
                arrow::struct_(
                    {arrow::field("name", arrow::utf8()), arrow::field("phone", arrow::utf8()),
                     arrow::field("city", arrow::utf8()), arrow::field("nick", arrow::utf8()),
                     arrow::field("alias", arrow::utf8())})),
            PredicateBuilder::Equal(/*field_index=*/0, "name", FieldType::STRING,
                                    Literal(FieldType::STRING, "bob", 3)),
            GetArrowPool(GetDefaultPool())));
    ASSERT_OK_AND_ASSIGN(filtered_batch, filtered->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(filtered_batch));
    filtered_struct = checked_pointer_cast<arrow::StructArray>(
        arrow::ImportArray(filtered_batch.first.get(), filtered_batch.second.get()).ValueOrDie());
    ASSERT_TRUE(filtered_struct->GetFieldByName("nick")->Equals(*json(arrow::utf8(), "[null]")));
    ASSERT_TRUE(filtered_struct->GetFieldByName("city")->Equals(*json(arrow::utf8(), R"(["x"])")));
    ASSERT_TRUE(filtered_struct->GetFieldByName("phone")->type()->Equals(*arrow::large_utf8()));
}

TEST(QueryAuthTableReadTest, TestClosesTheReadersOfSplitsBeforeOneThatFails) {
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableSchema> created,
        TableSchema::Create(/*schema_id=*/0, arrow::schema({arrow::field("id", arrow::int32())}),
                            /*partition_keys=*/{}, /*primary_keys=*/{},
                            {{Options::QUERY_AUTH_ENABLED, "true"}}));
    std::shared_ptr<TableSchema> table_schema = std::move(created);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context,
                         ReadContextBuilder("table_root_path").Finish());
    int32_t closes = 0;
    auto failing_split = std::make_shared<DummySplit>();
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableRead> read,
        QueryAuthTableRead::Create(
            std::shared_ptr<ReadContext>(std::move(context)), table_schema,
            GetArrowPool(GetDefaultPool()),
            CreateSplitTableRead([&closes, failing_split](const std::shared_ptr<Split>& split)
                                     -> Result<std::unique_ptr<BatchReader>> {
                if (split == failing_split) {
                    return Status::IOError("cannot open the split");
                }
                return std::unique_ptr<BatchReader>(std::make_unique<CloseCountingReader>(&closes));
            })));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableQueryAuthResult> no_rules,
                         TableQueryAuthResult::Create({}, {}));
    QueryAuthScope scope;
    scope.table_path = "table_root_path";
    scope.branch = "main";
    for (const DataField& field : table_schema->Fields()) {
        scope.authorized_field_ids.emplace(field.Name(), field.Id());
    }
    auto authorized = [&](const std::shared_ptr<Split>& split) -> std::shared_ptr<Split> {
        return std::make_shared<QueryAuthSplit>(split, no_rules, scope);
    };

    ASSERT_NOK_WITH_MSG(read->CreateReader(std::vector<std::shared_ptr<Split>>{
                            authorized(std::make_shared<DummySplit>()), authorized(failing_split)}),
                        "cannot open the split");
    ASSERT_EQ(1, closes);
    ASSERT_NOK_WITH_MSG(
        read->CreateReader(std::vector<std::shared_ptr<Split>>{
            authorized(std::make_shared<DummySplit>()), authorized(std::make_shared<DummySplit>()),
            std::make_shared<DummySplit>()}),
        "must be planned by a scan of the table through its catalog");
    ASSERT_EQ(3, closes);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                         read->CreateReader(std::vector<std::shared_ptr<Split>>{
                             authorized(std::make_shared<DummySplit>()),
                             authorized(std::make_shared<DummySplit>())}));
    ASSERT_EQ(3, closes);
    reader->Close();
    ASSERT_EQ(5, closes);
}

TEST(QueryAuthTableReadTest, TestRowFilterRemovesRowsFromTheBatchWithBitmap) {
    auto json = [](const std::shared_ptr<arrow::DataType>& type, const std::string& values) {
        return arrow::ipc::internal::json::ArrayFromJSON(type, values).ValueOrDie();
    };
    const arrow::FieldVector fields = {
        arrow::field("tenant", arrow::utf8()),
        arrow::field("value", arrow::int32()),
        arrow::field("email", arrow::utf8()),
        arrow::field("profile", arrow::struct_({arrow::field("email", arrow::utf8())})),
        arrow::field("emails", arrow::list(arrow::utf8())),
        arrow::field("contacts", arrow::map(arrow::utf8(), arrow::utf8()))};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> created,
                         TableSchema::Create(/*schema_id=*/0, arrow::schema(fields),
                                             /*partition_keys=*/{}, /*primary_keys=*/{},
                                             {{Options::QUERY_AUTH_ENABLED, "true"}}));
    std::shared_ptr<TableSchema> table_schema = std::move(created);
    // The dictionary contains B's email even after every remaining index belongs to A.
    std::shared_ptr<arrow::Array> email =
        arrow::DictionaryArray::FromArrays(
            arrow::dictionary(arrow::int32(), arrow::utf8()), json(arrow::int32(), "[0, 1, 2]"),
            json(arrow::utf8(), R"(["a@example", "b@example", "a2@example"])"))
            .ValueOrDie();
    std::shared_ptr<arrow::Array> offsets = json(arrow::int32(), "[0, 1, 2, 3]");
    std::shared_ptr<arrow::Array> profile =
        arrow::StructArray::Make({email}, std::vector<std::string>{"email"}).ValueOrDie();
    std::shared_ptr<arrow::Array> emails =
        arrow::ListArray::FromArrays(*offsets, *email).ValueOrDie();
    std::shared_ptr<arrow::Array> contacts =
        arrow::MapArray::FromArrays(offsets, json(arrow::utf8(), R"(["home", "home", "home"])"),
                                    email)
            .ValueOrDie();
    std::shared_ptr<arrow::Array> batch =
        arrow::StructArray::Make(
            {json(arrow::utf8(), R"(["A", "B", "A"])"), json(arrow::int32(), "[1, 2, 3]"), email,
             profile, emails, contacts},
            std::vector<std::string>{"tenant", "value", "email", "profile", "emails", "contacts"})
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableQueryAuthResult> rules,
        TableQueryAuthResult::Create(
            {R"({"kind": "LEAF", "transform": {"name": "FIELD_REF", "fieldRef": {"index": 0,)"
             R"( "name": "tenant", "type": "STRING"}}, "function": "EQUAL", "literals": ["A"]})"},
            {}));
    QueryAuthScope scope;
    scope.table_path = "table_root_path";
    scope.branch = "main";
    for (const DataField& field : table_schema->Fields()) {
        scope.authorized_field_ids.emplace(field.Name(), field.Id());
    }
    scope.rule_field_ids = rules->BindFieldIds(table_schema->Fields());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context,
                         ReadContextBuilder("table_root_path").Finish());
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableRead> read,
        QueryAuthTableRead::Create(
            std::shared_ptr<ReadContext>(std::move(context)), table_schema,
            GetArrowPool(GetDefaultPool()),
            CreateSplitTableRead(
                [batch](const std::shared_ptr<Split>&) -> Result<std::unique_ptr<BatchReader>> {
                    return std::unique_ptr<BatchReader>(std::make_unique<OneBatchReader>(batch));
                })));
    auto split = std::make_shared<QueryAuthSplit>(std::make_shared<DummySplit>(), rules, scope);

    auto assert_only_tenant_a = [&](std::unique_ptr<BatchReader> reader) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                             reader->NextBatchWithBitmap());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch_with_bitmap));
        auto& [read_batch, bitmap] = batch_with_bitmap;
        std::function<void(const ArrowArray&)> assert_no_dictionary = [&](const ArrowArray& array) {
            ASSERT_EQ(nullptr, array.dictionary);
            for (int64_t i = 0; i < array.n_children; ++i) {
                assert_no_dictionary(*array.children[i]);
            }
        };
        assert_no_dictionary(*read_batch.first);
        std::shared_ptr<arrow::Array> array =
            arrow::ImportArray(read_batch.first.get(), read_batch.second.get()).ValueOrDie();
        auto rows = checked_pointer_cast<arrow::StructArray>(array);
        ASSERT_TRUE(rows->ValidateFull().ok());
        ASSERT_TRUE(rows->Equals(
            *json(arrow::struct_(fields),
                  R"([["A", 1, "a@example", ["a@example"], ["a@example"], [["home", "a@example"]]],
                ["A", 3, "a2@example", ["a2@example"], ["a2@example"], [["home", "a2@example"]]]])")))
            << rows->ToString();
        ASSERT_EQ(2, bitmap.Cardinality());
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap eof, reader->NextBatchWithBitmap());
        ASSERT_TRUE(BatchReader::IsEofBatch(eof));
        reader->Close();
    };
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, read->CreateReader(split));
    assert_only_tenant_a(std::move(reader));
    ASSERT_OK_AND_ASSIGN(reader, read->CreateReader(std::vector<std::shared_ptr<Split>>{split}));
    assert_only_tenant_a(std::move(reader));
}

}  // namespace paimon::test

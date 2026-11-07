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

#include "paimon/realtime/index/realtime_index.h"

#include <atomic>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/realtime/index/realtime_index_evaluator.h"
#include "paimon/core/realtime/index/realtime_index_set.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/realtime/index/realtime_index_factory.h"
#include "paimon/realtime/index/realtime_index_reader.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class RealtimeIndexTest : public testing::Test {
 public:
    void SetUp() override {
        schema_ = arrow::schema(
            {DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()),
             arrow::field("id", arrow::int64()), arrow::field("value", arrow::utf8())});
        ASSERT_OK_AND_ASSIGN(index_, CreateIndex({"id", "value"}));
    }

    Result<std::shared_ptr<RealtimeIndexSet>> CreateIndex(
        const std::vector<std::string>& fields) const {
        std::string columns;
        for (const std::string& field : fields) {
            if (!columns.empty()) {
                columns.append(",");
            }
            columns.append(field);
        }
        return RealtimeIndexSet::Create(schema_, {{"realtime.index.hash.columns", columns}},
                                        GetDefaultPool());
    }

    Result<std::optional<RoaringBitmap64>> Evaluate(const std::shared_ptr<Predicate>& predicate,
                                                    const OffsetRange& range) const {
        return RealtimeIndexEvaluator::Evaluate(index_, predicate, range);
    }

    Status Add(const std::string& json, const OffsetRange& range) const {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema_->fields()), json));
        ArrowArray batch;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &batch));
        ScopeGuard batch_guard([&batch]() { ArrowArrayRelease(&batch); });
        return index_->AddBatch(&batch, range);
    }

    std::shared_ptr<Predicate> EqualId(int64_t id) const {
        // Query positions intentionally differ from write positions.
        return PredicateBuilder::Equal(0, "id", FieldType::BIGINT, Literal(id));
    }

    std::shared_ptr<Predicate> EqualValue(const std::string& value) const {
        return PredicateBuilder::Equal(0, "value", FieldType::STRING,
                                       Literal(FieldType::STRING, value.data(), value.size()));
    }

 protected:
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<RealtimeIndexSet> index_;
};

TEST_F(RealtimeIndexTest, RetainsAllKeyAndValueOffsetsWithinBothBounds) {
    ASSERT_OK(Add(R"([[10, 1, "a"], [13, 2, "a"], [20, 1, "b"]])", OffsetRange(10, 21)));
    ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap64> keys,
                         Evaluate(EqualId(1), OffsetRange(0, 21)));
    ASSERT_TRUE(keys.has_value());
    ASSERT_EQ(RoaringBitmap64::From({10, 20}), *keys);
    ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap64> values,
                         Evaluate(EqualValue("a"), OffsetRange(11, 20)));
    ASSERT_TRUE(values.has_value());
    ASSERT_EQ(RoaringBitmap64::From({13}), *values);

    const uint64_t initial_memory = index_->GetMemoryUsage();
    ASSERT_OK(Add(R"([[21, 1, "a"], [25, 3, "a"]])", OffsetRange(21, 26)));
    // Appends must not change a bounded query's result.
    ASSERT_OK_AND_ASSIGN(values, Evaluate(EqualValue("a"), OffsetRange(11, 20)));
    ASSERT_TRUE(values.has_value());
    ASSERT_EQ(RoaringBitmap64::From({13}), *values);
    ASSERT_OK_AND_ASSIGN(keys, Evaluate(EqualId(1), OffsetRange(20, 25)));
    ASSERT_TRUE(keys.has_value());
    ASSERT_EQ(RoaringBitmap64::From({20, 21}), *keys);
    ASSERT_GE(index_->GetMemoryUsage(), initial_memory);
}

TEST_F(RealtimeIndexTest, InAndCompoundPredicatesPreserveConservativeFallback) {
    ASSERT_OK(Add(R"([[1, 1, "a"], [3, 2, "b"], [5, 3, null]])", OffsetRange(1, 6)));
    const std::shared_ptr<Predicate> in = PredicateBuilder::In(
        0, "id", FieldType::BIGINT, {Literal(int64_t{1}), Literal(int64_t{3})});
    ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap64> candidates,
                         Evaluate(in, OffsetRange(1, 6)));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_EQ(RoaringBitmap64::From({1, 5}), *candidates);

    const std::shared_ptr<Predicate> unsupported =
        PredicateBuilder::GreaterThan(0, "id", FieldType::BIGINT, Literal(int64_t{0}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> conjunction,
                         PredicateBuilder::And({in, unsupported}));
    ASSERT_OK_AND_ASSIGN(candidates, Evaluate(conjunction, OffsetRange(1, 6)));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_EQ(RoaringBitmap64::From({1, 5}), *candidates);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> disjunction,
                         PredicateBuilder::Or({in, unsupported}));
    ASSERT_OK_AND_ASSIGN(candidates, Evaluate(disjunction, OffsetRange(1, 6)));
    ASSERT_FALSE(candidates.has_value());

    ASSERT_OK_AND_ASSIGN(disjunction, PredicateBuilder::Or({EqualValue("a"), EqualId(2)}));
    ASSERT_OK_AND_ASSIGN(candidates, Evaluate(disjunction, OffsetRange(1, 6)));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_EQ(RoaringBitmap64::From({1, 3}), *candidates);
    ASSERT_OK_AND_ASSIGN(conjunction, PredicateBuilder::And({EqualValue("a"), EqualId(2)}));
    ASSERT_OK_AND_ASSIGN(candidates, Evaluate(conjunction, OffsetRange(1, 6)));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_TRUE(candidates->IsEmpty());
}

TEST_F(RealtimeIndexTest, ConcurrentReadersRetainPublishedRangesDuringPostingAndDictionaryGrowth) {
    ASSERT_OK(Add(R"([[0, 1, "a"], [1, 2, "a"], [2, 1, "b"]])", OffsetRange(0, 3)));
    std::atomic<int64_t> visible_end{3};
    std::atomic<bool> finished{false};
    std::vector<std::thread> readers;
    ScopeGuard join_readers([&]() {
        finished.store(true, std::memory_order_release);
        for (std::thread& reader : readers) {
            reader.join();
        }
    });
    for (int32_t i = 0; i < 4; ++i) {
        readers.emplace_back([&]() {
            const std::shared_ptr<Predicate> old_key = EqualId(1);
            const std::shared_ptr<Predicate> shared_value = EqualValue("a");
            uint64_t memory_usage = index_->GetMemoryUsage();
            do {
                const int64_t end = visible_end.load(std::memory_order_acquire);
                ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap64> candidates,
                                     Evaluate(old_key, OffsetRange(1, 3)));
                ASSERT_TRUE(candidates.has_value());
                ASSERT_EQ(RoaringBitmap64::From({2}), *candidates);

                ASSERT_OK_AND_ASSIGN(candidates, Evaluate(shared_value, OffsetRange(1, end)));
                ASSERT_TRUE(candidates.has_value());
                RoaringBitmap64 expected;
                expected.Add(1);
                expected.AddRange(3, end);
                ASSERT_EQ(expected, *candidates);
                if (end > 3) {
                    ASSERT_OK_AND_ASSIGN(candidates,
                                         Evaluate(EqualId(end - 1), OffsetRange(0, end)));
                    ASSERT_TRUE(candidates.has_value());
                    ASSERT_EQ(RoaringBitmap64::From({end - 1}), *candidates);
                }
                const uint64_t updated_memory_usage = index_->GetMemoryUsage();
                ASSERT_GE(updated_memory_usage, memory_usage);
                memory_usage = updated_memory_usage;
            } while (!finished.load(std::memory_order_acquire));
        });
    }
    for (int64_t offset = 3; offset < 1027; offset += 2) {
        // Grow both postings and the dictionary.
        ASSERT_OK(
            Add(fmt::format(R"([[{}, 1, "a"], [{}, {}, "a"]])", offset, offset + 1, offset + 1),
                OffsetRange(offset, offset + 2)));
        // Publish only after indexing succeeds.
        visible_end.store(offset + 2, std::memory_order_release);
    }
}

TEST_F(RealtimeIndexTest, MissingValuesEmptyRangesAndUnknownFields) {
    ASSERT_OK(Add(R"([[10, 1, null]])", OffsetRange(10, 11)));
    ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap64> candidates,
                         Evaluate(EqualValue("missing"), OffsetRange(0, 100)));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_TRUE(candidates->IsEmpty());
    ASSERT_OK_AND_ASSIGN(candidates, Evaluate(EqualId(1), OffsetRange(10, 10)));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_TRUE(candidates->IsEmpty());
    ASSERT_OK_AND_ASSIGN(candidates, Evaluate(nullptr, OffsetRange(10, 11)));
    ASSERT_FALSE(candidates.has_value());
    const std::shared_ptr<Predicate> unknown =
        PredicateBuilder::Equal(0, "unknown", FieldType::BIGINT, Literal(int64_t{1}));
    ASSERT_OK_AND_ASSIGN(candidates, Evaluate(unknown, OffsetRange(10, 11)));
    ASSERT_FALSE(candidates.has_value());
    ASSERT_NOK(Evaluate(EqualId(1), OffsetRange(11, 10)));
    ASSERT_NOK(Evaluate(EqualId(1), OffsetRange(-1, 10)));
    ASSERT_NOK(CreateIndex({"missing"}));
    ASSERT_NOK(CreateIndex({}));
}

TEST_F(RealtimeIndexTest, FieldReadersBindIndependentRangesAndShareAppendOnlyPostings) {
    ASSERT_OK(Add(R"([[10, 1, "a"], [13, 2, "b"], [20, 1, "c"]])", OffsetRange(10, 21)));
    std::vector<std::shared_ptr<RealtimeIndex>> key_indexes =
        index_->FindIndexes("id", FieldType::BIGINT);
    const std::vector<std::shared_ptr<RealtimeIndex>> value_indexes =
        index_->FindIndexes("value", FieldType::STRING);
    ASSERT_EQ(1, key_indexes.size());
    ASSERT_EQ(1, value_indexes.size());
    std::shared_ptr<RealtimeIndex> key_index = std::move(key_indexes[0]);
    ASSERT_NE(key_index, value_indexes[0]);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> first,
                         key_index->CreateReader(OffsetRange(10, 14)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> second,
                         key_index->CreateReader(OffsetRange(20, 21)));
    ASSERT_OK(Add(R"([[25, 1, "d"]])", OffsetRange(25, 26)));
    ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap64> candidates,
                         first->VisitEqual(Literal(int64_t{1})));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_EQ(RoaringBitmap64::From({10}), *candidates);
    ASSERT_OK_AND_ASSIGN(candidates, second->VisitIn({Literal(int64_t{1}), Literal(int64_t{2})}));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_EQ(RoaringBitmap64::From({20}), *candidates);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> later,
                         key_index->CreateReader(OffsetRange(25, 26)));
    ASSERT_OK_AND_ASSIGN(candidates, later->VisitEqual(Literal(int64_t{1})));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_EQ(RoaringBitmap64::From({25}), *candidates);
    ASSERT_OK_AND_ASSIGN(candidates, first->VisitEqual(Literal(int64_t{1})));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_EQ(RoaringBitmap64::From({10}), *candidates);
    ASSERT_OK_AND_ASSIGN(candidates, first->VisitGreaterThan(Literal(int64_t{0})));
    ASSERT_FALSE(candidates.has_value());
    ASSERT_NOK(key_index->CreateReader(OffsetRange(-1, 20)));
    ASSERT_NOK(key_index->CreateReader(OffsetRange(20, 10)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> empty,
                         key_index->CreateReader(OffsetRange(10, 10)));
    ASSERT_OK_AND_ASSIGN(candidates, empty->VisitEqual(Literal(int64_t{1})));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_TRUE(candidates->IsEmpty());

    ASSERT_TRUE(index_->FindIndexes("missing", FieldType::BIGINT).empty());
    ASSERT_TRUE(index_->FindIndexes("id", FieldType::STRING).empty());
    // Readers retain the index after its segment releases it.
    key_index.reset();
    index_.reset();
    ASSERT_OK_AND_ASSIGN(candidates, second->VisitEqual(Literal(int64_t{1})));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_EQ(RoaringBitmap64::From({20}), *candidates);
}

TEST_F(RealtimeIndexTest, RejectsUnknownRegisteredPlugin) {
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema_, &c_schema).ok());
    ScopeGuard schema_guard([&c_schema]() { ArrowSchemaRelease(&c_schema); });
    ASSERT_NOK_WITH_MSG(RealtimeIndexFactory::Get("missing", &c_schema, {}, GetDefaultPool()),
                        "unknown real-time index type");
}

TEST_F(RealtimeIndexTest, ColumnsOptionsSelectPluginsAndDeduplicateDeclarations) {
    const std::map<std::string, std::string> unrelated = {{"file-index.hash.columns", "id"},
                                                          {"realtime.index.hash.capacity", "16"}};
    ASSERT_FALSE(RealtimeIndexSet::IsEnabled(unrelated));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexSet> disabled,
                         RealtimeIndexSet::Create(schema_, unrelated, GetDefaultPool()));
    ASSERT_EQ(nullptr, disabled);

    const std::map<std::string, std::string> options = {
        {"realtime.index.hash.columns", ",id,, id, value,,"}};
    ASSERT_TRUE(RealtimeIndexSet::IsEnabled(options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexSet> indexes,
                         RealtimeIndexSet::Create(schema_, options, GetDefaultPool()));
    ASSERT_EQ(1, indexes->FindIndexes("id", FieldType::BIGINT).size());
    ASSERT_EQ(1, indexes->FindIndexes("value", FieldType::STRING).size());

    ASSERT_NOK_WITH_MSG(RealtimeIndexSet::Create(
                            schema_, {{"realtime.index.missing.columns", "id"}}, GetDefaultPool()),
                        "unknown real-time index type: missing");
    for (const char* key : {"realtime.index.columns", "realtime.index..columns"}) {
        ASSERT_NOK_WITH_MSG(RealtimeIndexSet::Create(schema_, {{key, "id"}}, GetDefaultPool()),
                            "invalid real-time index option");
    }
}

TEST_F(RealtimeIndexTest, FactoryAcceptsOnlyOneIndexedFieldAndReaderNeedsNoFieldSelection) {
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema_, &c_schema).ok());
    ScopeGuard schema_guard([&c_schema]() { ArrowSchemaRelease(&c_schema); });
    ASSERT_NOK_WITH_MSG(RealtimeIndexFactory::Get("hash", &c_schema, {}, GetDefaultPool()),
                        "exactly one indexed field");

    const std::shared_ptr<arrow::Schema> field_schema =
        arrow::schema({schema_->field(0), schema_->field(1)});
    ASSERT_TRUE(arrow::ExportSchema(*field_schema, &c_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndex> index,
                         RealtimeIndexFactory::Get("hash", &c_schema, {}, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader,
                         index->CreateReader(OffsetRange(0, 10)));
    ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap64> candidates,
                         reader->VisitEqual(Literal(int64_t{1})));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_TRUE(candidates->IsEmpty());
    ASSERT_NOK_WITH_MSG(reader->VisitEqual(Literal(FieldType::STRING, "1", 1)),
                        "literal type mismatch: expected BIGINT, actual STRING");
    ASSERT_NOK_WITH_MSG(reader->VisitIn({Literal(int64_t{1}), Literal(FieldType::STRING, "1", 1)}),
                        "literal type mismatch: expected BIGINT, actual STRING");

    const std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(field_schema->fields()),
                                                  R"([[10, 1], [30, 1]])")
            .ValueOrDie();
    ArrowArray batch;
    ASSERT_TRUE(arrow::ExportArray(*data, &batch).ok());
    ScopeGuard batch_guard([&batch]() { ArrowArrayRelease(&batch); });
    ASSERT_OK(index->AddBatch(&batch));
    ASSERT_OK_AND_ASSIGN(reader, index->CreateReader(OffsetRange(11, 31)));
    ASSERT_OK_AND_ASSIGN(candidates, reader->VisitEqual(Literal(int64_t{1})));
    ASSERT_TRUE(candidates.has_value());
    ASSERT_EQ(RoaringBitmap64::From({30}), *candidates);
}

TEST_F(RealtimeIndexTest, RejectsDecimalAndTimestampFields) {
    const std::vector<std::shared_ptr<arrow::DataType>> types = {
        arrow::decimal128(10, 2),
        arrow::timestamp(arrow::TimeUnit::SECOND),
        arrow::timestamp(arrow::TimeUnit::MILLI),
        arrow::timestamp(arrow::TimeUnit::MICRO),
        arrow::timestamp(arrow::TimeUnit::NANO),
        arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")};
    for (const std::shared_ptr<arrow::DataType>& type : types) {
        const std::shared_ptr<arrow::Schema> schema =
            arrow::schema({schema_->field(0), arrow::field("value", type)});
        ASSERT_NOK_WITH_MSG(
            RealtimeIndexSet::Create(schema, {{"realtime.index.hash.columns", "value"}},
                                     GetDefaultPool()),
            "unsupported real-time hash index field type");
    }
}

TEST_F(RealtimeIndexTest, FrameworkRejectsUnorderedAndOutOfBoundsOffsetsBeforePublication) {
    ASSERT_NOK(Add(R"([[10, 1, "a"], [10, 2, "b"]])", OffsetRange(10, 11)));
    ASSERT_NOK(Add(R"([[9, 1, "a"]])", OffsetRange(10, 11)));
    ASSERT_NOK(Add(R"([[11, 1, "a"]])", OffsetRange(10, 11)));
    ASSERT_NOK(Add(R"([[null, 1, "a"]])", OffsetRange(10, 11)));
    ASSERT_OK(Add(R"([[10, 1, "a"]])", OffsetRange(10, 11)));
    ASSERT_NOK(Add(R"([[10, 2, "b"]])", OffsetRange(10, 11)));
}

}  // namespace
}  // namespace paimon::test

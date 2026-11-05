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

#include "paimon/core/io/file_index_evaluator.h"

#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index.h"
#include "paimon/common/fs/external_path_provider.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/file_index/file_indexer_factory.h"
#include "paimon/file_index/scored_file_index_result.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/full_text_match.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_search_file_index.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::test {
class FileIndexEvaluatorTest : public ::testing::Test {
 public:
    void SetUp() override {
        std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                              DataField(1, arrow::field("f1", arrow::int32())),
                                              DataField(2, arrow::field("f2", arrow::int32())),
                                              DataField(3, arrow::field("f3", arrow::float64()))};
        data_schema_ = DataField::ConvertDataFieldsToArrowSchema(read_fields);

        pool_ = GetDefaultPool();
        fs_ = std::make_shared<LocalFileSystem>();
    }
    void TearDown() override {
        pool_.reset();
        fs_.reset();
        data_schema_.reset();
    }

    void CheckResult(const std::shared_ptr<FileIndexResult>& result,
                     const std::vector<int32_t>& expected) const {
        auto typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap32* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*(typed_result->GetBitmap().value()), RoaringBitmap32::From(expected))
            << "result:" << typed_result->GetBitmap().value()->ToString()
            << ", expected:" << RoaringBitmap32::From(expected).ToString();
    }

    void CheckResult(const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
                     const std::shared_ptr<DataFileMeta>& data_file_meta) const {
        ASSERT_OK_AND_ASSIGN(
            auto evaluator,
            FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(), data_file_path_factory,
                                       data_file_meta, fs_, pool_));
        {
            auto predicate =
                PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {7});
        }
        {
            auto predicate =
                PredicateBuilder::IsNotNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {0, 1, 2, 3, 4, 5, 6});
        }
        {
            auto predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {0, 7});
        }
        {
            auto predicate = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::STRING,
                                                        Literal(FieldType::STRING, "Alice", 5));
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {1, 2, 3, 4, 5, 6});
        }
        {
            auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::INT, Literal(10));
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            ASSERT_FALSE(dynamic_cast<BitmapIndexResult*>(file_index_result.get()));
        }
        {
            auto predicate = PredicateBuilder::GreaterOrEqual(
                /*field_index=*/1, /*field_name=*/"f1", FieldType::INT, Literal(10));
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            ASSERT_FALSE(dynamic_cast<BitmapIndexResult*>(file_index_result.get()));
        }
        {
            auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::INT, Literal(10));
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            ASSERT_FALSE(dynamic_cast<BitmapIndexResult*>(file_index_result.get()));
        }
        {
            auto predicate = PredicateBuilder::LessOrEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::INT, Literal(10));
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            ASSERT_FALSE(dynamic_cast<BitmapIndexResult*>(file_index_result.get()));
        }
        {
            auto predicate = PredicateBuilder::In(
                /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                {Literal(FieldType::STRING, "Alice", 5), Literal(FieldType::STRING, "Bob", 3),
                 Literal(FieldType::STRING, "Lucy", 4)});
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {0, 1, 4, 5, 7});
        }
        {
            auto predicate = PredicateBuilder::NotIn(
                /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                {Literal(FieldType::STRING, "Alice", 5), Literal(FieldType::STRING, "Bob", 3),
                 Literal(FieldType::STRING, "Lucy", 4)});
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {2, 3, 6});
        }
        {
            auto f0_predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));
            auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::INT, Literal(20));
            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::And({f0_predicate, f1_predicate}));
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {7});
        }
        {
            auto f0_predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));
            auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::INT, Literal(20));
            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::Or({f0_predicate, f1_predicate}));
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            CheckResult(file_index_result, {0, 4, 6, 7});
        }
        {
            // test non result
            auto predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "unknown", 7));
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_FALSE(file_index_result->IsRemain().value());
        }
        {
            // test early stop
            auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                        FieldType::INT, Literal(10));
            auto f2_predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                        FieldType::INT, Literal(6));
            auto f0_predicate =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));

            ASSERT_OK_AND_ASSIGN(auto predicate,
                                 PredicateBuilder::And({f1_predicate, f2_predicate, f0_predicate}));
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_FALSE(file_index_result->IsRemain().value());
        }
        {
            // test no predicate
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(nullptr));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            ASSERT_FALSE(dynamic_cast<BitmapIndexResult*>(file_index_result.get()));
        }
        {
            // test predicate on f3 (do not have index)
            auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                     FieldType::DOUBLE, Literal(14.1));
            ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(file_index_result->IsRemain().value());
            ASSERT_FALSE(dynamic_cast<BitmapIndexResult*>(file_index_result.get()));
        }
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<arrow::Schema> data_schema_;
    CoreOptions core_options_;
};

TEST_F(FileIndexEvaluatorTest, TestEvaluateEmbeddingIndex) {
    std::string path =
        paimon::test::GetDataDir() + "/orc/append_with_bitmap.db/append_with_bitmap/";
    std::vector<uint8_t> embedded_bytes = {
        0,  5,   78,  78,  208, 26,  53,  174, 0,   0,   0,   1,   0,   0,   0,   96,  0,   0,
        0,  3,   0,   2,   102, 48,  0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112,
        0,  0,   0,   96,  0,   0,   0,   176, 0,   2,   102, 49,  0,   0,   0,   1,   0,   6,
        98, 105, 116, 109, 97,  112, 0,   0,   1,   16,  0,   0,   0,   102, 0,   2,   102, 50,
        0,  0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112, 0,   0,   1,   118, 0,   0,
        0,  108, 0,   0,   0,   0,   2,   0,   0,   0,   8,   0,   0,   0,   5,   0,   0,   0,
        0,  1,   0,   0,   0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,   0,   0,   0,
        85, 0,   0,   0,   5,   0,   0,   0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,
        0,  0,   0,   20,  0,   0,   0,   3,   66,  111, 98,  0,   0,   0,   20,  0,   0,   0,
        20, 0,   0,   0,   5,   69,  109, 105, 108, 121, 255, 255, 255, 253, 255, 255, 255, 255,
        0,  0,   0,   4,   76,  117, 99,  121, 255, 255, 255, 251, 255, 255, 255, 255, 0,   0,
        0,  4,   84,  111, 110, 121, 0,   0,   0,   40,  0,   0,   0,   20,  58,  48,  0,   0,
        1,  0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   0,   0,   7,   0,   58,  48,
        0,  0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   1,   0,   5,   0,
        58, 48,  0,   0,   1,   0,   0,   0,   0,   0,   1,   0,   16,  0,   0,   0,   3,   0,
        6,  0,   2,   0,   0,   0,   8,   0,   0,   0,   2,   0,   0,   0,   0,   1,   0,   0,
        0,  10,  0,   0,   0,   0,   0,   0,   0,   28,  0,   0,   0,   2,   0,   0,   0,   10,
        0,  0,   0,   22,  0,   0,   0,   26,  0,   0,   0,   20,  0,   0,   0,   0,   0,   0,
        0,  22,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,   16,  0,   0,   0,
        4,  0,   6,   0,   7,   0,   58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   4,   0,
        16, 0,   0,   0,   0,   0,   1,   0,   2,   0,   3,   0,   5,   0,   2,   0,   0,   0,
        8,  0,   0,   0,   2,   1,   255, 255, 255, 248, 0,   0,   0,   18,  0,   0,   0,   1,
        0,  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   28,  0,   0,   0,   2,   0,   0,
        0,  0,   0,   0,   0,   0,   0,   0,   0,   22,  0,   0,   0,   1,   0,   0,   0,   22,
        0,  0,   0,   24,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,   16,  0,
        0,  0,   2,   0,   3,   0,   6,   0,   58,  48,  0,   0,   1,   0,   0,   0,   0,   0,
        3,  0,   16,  0,   0,   0,   0,   0,   1,   0,   4,   0,   5,   0};
    auto embedded_index = std::make_shared<Bytes>(embedded_bytes.size(), pool_.get());
    memcpy(embedded_index->data(), reinterpret_cast<char*>(embedded_bytes.data()),
           embedded_bytes.size());
    auto data_file_meta = std::make_shared<DataFileMeta>(
        "data-62feb610-c83f-4217-9b50-bbad9cd08eb4-0.orc", /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/embedded_index, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt,
        /*column_max_sequence_numbers=*/std::nullopt);
    CheckResult(/*data_file_path_factory=*/nullptr, data_file_meta);
}

TEST_F(FileIndexEvaluatorTest, TestEvaluateNoEmbeddingIndex) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/append_with_bitmap_no_embedding.db/append_with_bitmap_no_embedding/";
    auto data_file_meta = std::make_shared<DataFileMeta>(
        "data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc", /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>(
            {"data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc.index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt,
        /*column_max_sequence_numbers=*/std::nullopt);
    auto data_file_path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(data_file_path_factory->Init(path + "/bucket-0/", /*format_identifier=*/"orc",
                                           /*data_file_prefix=*/"data-", nullptr));
    CheckResult(data_file_path_factory, data_file_meta);
}

TEST_F(FileIndexEvaluatorTest, TestTimestampType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };
    auto data_schema = arrow::schema(fields);

    std::string path = paimon::test::GetDataDir() + "/orc/timestamp_index.db/timestamp_index/";
    auto data_file_meta = std::make_shared<DataFileMeta>(
        "data-18569866-0c37-45e9-9d88-2eaf6dd084b0-0.orc", /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>(
            {"data-18569866-0c37-45e9-9d88-2eaf6dd084b0-0.orc.index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt,
        /*column_max_sequence_numbers=*/std::nullopt);
    auto data_file_path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(data_file_path_factory->Init(path + "/bucket-0/", /*format_identifier=*/"orc",
                                           /*data_file_prefix=*/"data-", nullptr));
    // {0,2,3,6}
    auto in_predicate = PredicateBuilder::In(
        /*field_index=*/0, /*field_name=*/"ts_sec", FieldType::TIMESTAMP,
        {Literal(Timestamp(1745542802000l, 0)), Literal(Timestamp(-1745000l, 0)),
         Literal(Timestamp(1745542602000l, 0))});
    // {0, 6}
    auto greater_than = PredicateBuilder::GreaterThan(
        /*field_index=*/6, /*field_name=*/"ts_tz_micro", FieldType::TIMESTAMP,
        Literal(Timestamp(1745542602001l, 1000)));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({in_predicate, greater_than}));
    ASSERT_OK_AND_ASSIGN(
        auto evaluator,
        FileIndexEvaluator::Create(data_schema, core_options_.ToMap(), data_file_path_factory,
                                   data_file_meta, /*file_system=*/fs_, pool_));
    ASSERT_OK_AND_ASSIGN(auto file_index_result, evaluator->Evaluate(predicate));
    CheckResult(file_index_result, {0, 6});
}

TEST_F(FileIndexEvaluatorTest, TestCreateRequiresExternalIndexDependencies) {
    auto data_file_meta = std::make_shared<DataFileMeta>(
        "data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc", /*file_size=*/689,
        /*row_count=*/8, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>(
            {"data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc.index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt,
        /*column_max_sequence_numbers=*/std::nullopt);
    ASSERT_NOK_WITH_MSG(
        FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                   /*data_file_path_factory=*/nullptr, data_file_meta,
                                   /*file_system=*/nullptr, pool_),
        "read process for FileIndexEvaluator must have data_file_path_factory and file_system");
}

class FileIndexSearchEvaluatorTest : public FileIndexEvaluatorTest,
                                     public ::testing::WithParamInterface<bool> {
 protected:
    void SetUp() override {
        FileIndexEvaluatorTest::SetUp();
        auto factory = std::make_unique<MockFullTextSearchFileIndexFactory>(statistics_);
        FactoryCreator::GetInstance()->Register(statistics_index_type_, factory.release());
        auto vector_factory = std::make_unique<MockVectorSearchFileIndexFactory>(statistics_);
        FactoryCreator::GetInstance()->Register(vector_statistics_index_type_,
                                                vector_factory.release());
        data_schema_ = arrow::schema({arrow::field("embedding", arrow::list(arrow::float32())),
                                      arrow::field("title", arrow::utf8())});
        dir_ = UniqueTestDirectory::Create();
        path_factory_ = std::make_shared<DataFilePathFactory>();
        ASSERT_OK(path_factory_->Init(dir_->Str(), /*format_identifier=*/"orc",
                                      /*data_file_prefix=*/"data-", nullptr));
    }

    void TearDown() override {
        FactoryCreator::GetInstance()->TEST_Unregister(statistics_index_type_);
        FactoryCreator::GetInstance()->TEST_Unregister(vector_statistics_index_type_);
        FileIndexEvaluatorTest::TearDown();
    }

    Result<std::shared_ptr<DataFileMeta>> MakeIndex(const std::shared_ptr<Bytes>& bytes) const {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<DataFileMeta> meta,
            DataFileMeta::ForAppend(
                "data-search.orc", /*file_size=*/0, /*row_count=*/4, SimpleStats::EmptyStats(),
                /*min_sequence_number=*/0,
                /*max_sequence_number=*/3, /*schema_id=*/0, FileSource::Append(),
                /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
                /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt));
        bool is_embedded = GetParam();
        if (is_embedded) {
            meta->embedded_index = bytes;
        } else {
            const std::string index_name = meta->file_name + ".index";
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<OutputStream> file,
                fs_->Create(path_factory_->ToAlignedPath(index_name, meta), /*overwrite=*/true));
            PAIMON_RETURN_NOT_OK(file->Write(bytes->data(), bytes->size()));
            PAIMON_RETURN_NOT_OK(file->Close());
            meta->extra_files = {index_name};
        }
        return meta;
    }

    std::shared_ptr<VectorSearch> MakeVectorSearch(const std::string& field) const {
        return std::make_shared<VectorSearch>(
            field, /*limit=*/2, std::vector<float>{1.0f, 2.0f}, /*pre_filter=*/nullptr,
            /*predicate=*/nullptr, VectorSearch::DistanceType::EUCLIDEAN,
            std::map<std::string, std::string>{});
    }

    std::shared_ptr<FullTextSearch> MakeFullTextSearch(const std::string& field) const {
        return std::make_shared<FullTextSearch>(field, R"({"match":{"query":"paimon"}})",
                                                /*limit=*/2);
    }

    void CheckSearchError(SearchIndexType index_type, const std::shared_ptr<DataFileMeta>& meta,
                          const std::string& field, const std::string& message) const {
        ASSERT_OK_AND_ASSIGN(auto evaluator,
                             FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                        path_factory_, meta, fs_, pool_));
        if (index_type == SearchIndexType::VECTOR) {
            ASSERT_NOK_WITH_MSG(evaluator->SearchVector(MakeVectorSearch(field)), message);
        } else {
            ASSERT_NOK_WITH_MSG(evaluator->SearchFullText(MakeFullTextSearch(field)), message);
        }
    }

    void CheckInvalidSearchIndexes(SearchIndexType index_type) const {
        const std::string field = index_type == SearchIndexType::VECTOR ? "embedding" : "title";
        const std::string search_type =
            index_type == SearchIndexType::VECTOR ? "VECTOR_SEARCH" : "FULL_TEXT_SEARCH";
        const std::string identifier = index_type == SearchIndexType::VECTOR
                                           ? MockSearchFileIndex::VECTOR_IDENTIFIER
                                           : MockSearchFileIndex::FULL_TEXT_IDENTIFIER;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<Bytes> bytes,
            MockSearchFileIndex::MakeIndex(field, identifier, {{3, 0.75f}, {1, 0.25f}}, pool_));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> meta, MakeIndex(bytes));
        CheckSearchError(index_type, meta, "missing",
                         "Search field 'missing' does not exist in data schema");

        ASSERT_OK_AND_ASSIGN(bytes,
                             MockSearchFileIndex::MakeIndex("another-field", identifier,
                                                            {{3, 0.75f}, {1, 0.25f}}, pool_));
        ASSERT_OK_AND_ASSIGN(meta, MakeIndex(bytes));
        CheckSearchError(index_type, meta, field,
                         "No File Index reader supports " + search_type + " for field '" + field +
                             "' in data file '" + meta->file_name + "'");

        FileIndexFormat::ColumnIndexes indexes;
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> payload,
                             MockSearchFileIndex::MakePayload({{3, 0.75f}, {1, 0.25f}}, pool_));
        const std::string another_identifier = "another-" + identifier;
        auto* factories = FactoryCreator::GetInstance();
        if (!factories->Create(another_identifier)) {
            std::unique_ptr<FileIndexerFactory> factory;
            if (index_type == SearchIndexType::VECTOR) {
                factory = std::make_unique<MockVectorSearchFileIndexFactory>();
            } else {
                factory = std::make_unique<MockFullTextSearchFileIndexFactory>();
            }
            factories->Register(another_identifier, factory.release());
        }
        indexes[field][identifier] = payload;
        indexes[field][another_identifier] = nullptr;
        ASSERT_OK_AND_ASSIGN(bytes, MockSearchFileIndex::Serialize(indexes, pool_));
        ASSERT_OK_AND_ASSIGN(meta, MakeIndex(bytes));
        CheckSearchError(index_type, meta, field,
                         "Multiple File Index readers support " + search_type + " for field '" +
                             field + "' in data file '" + meta->file_name + "'");

        meta->embedded_index.reset();
        meta->extra_files.clear();
        CheckSearchError(index_type, meta, field,
                         "has no File Index for search field '" + field + "'");
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    const std::string statistics_index_type_ = "evaluator-statistics-full-text";
    const std::string vector_statistics_index_type_ = "evaluator-statistics-vector";
    std::shared_ptr<MockSearchFileIndex::Statistics> statistics_ =
        std::make_shared<MockSearchFileIndex::Statistics>();
};

TEST_P(FileIndexSearchEvaluatorTest, TestVectorSearchReadsIndexWithScores) {
    FileIndexFormat::ColumnIndexes indexes;
    ASSERT_OK_AND_ASSIGN(indexes["embedding"][vector_statistics_index_type_],
                         MockSearchFileIndex::MakePayload({{3, 0.75f}, {1, 0.25f}}, pool_));
    indexes["embedding"]["bitmap"] = nullptr;
    indexes["embedding"]["unregistered-empty-index"] = nullptr;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes,
                         MockSearchFileIndex::Serialize(indexes, pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> meta, MakeIndex(bytes));
    ASSERT_OK_AND_ASSIGN(auto evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                    path_factory_, meta, fs_, pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> result,
                         evaluator->SearchVector(MakeVectorSearch("embedding")));
    ASSERT_TRUE(result);
    EXPECT_EQ(RoaringBitmap32::From({1, 3}), result->GetRowPositions());
    EXPECT_EQ(std::vector<float>({0.25f, 0.75f}), result->GetScores());
    ASSERT_EQ(statistics_->executions.size(), 1);
    ASSERT_EQ(statistics_->executions.front().field_name, "embedding");
    ASSERT_TRUE(statistics_->executions.front().needs_scores);
}

TEST_P(FileIndexSearchEvaluatorTest, TestFullTextSearchReadsIndexWithScores) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Bytes> bytes,
        MockSearchFileIndex::MakeIndex("title", MockSearchFileIndex::FULL_TEXT_IDENTIFIER,
                                       {{3, 0.75f}, {1, 0.25f}}, pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> meta, MakeIndex(bytes));
    ASSERT_OK_AND_ASSIGN(auto evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                    path_factory_, meta, fs_, pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> result,
                         evaluator->SearchFullText(MakeFullTextSearch("title")));
    ASSERT_TRUE(result);
    ASSERT_EQ(result->GetRowPositions(), RoaringBitmap32::From({1, 3}));
    ASSERT_EQ(result->GetScores(), std::vector<float>({0.25f, 0.75f}));
}

TEST_P(FileIndexSearchEvaluatorTest, TestEmptyVectorSearchIndex) {
    FileIndexFormat::ColumnIndexes indexes;
    indexes["embedding"][MockSearchFileIndex::VECTOR_IDENTIFIER] = nullptr;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes,
                         MockSearchFileIndex::Serialize(indexes, pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> meta, MakeIndex(bytes));
    ASSERT_OK_AND_ASSIGN(auto evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                    path_factory_, meta, fs_, pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> vector_result,
                         evaluator->SearchVector(MakeVectorSearch("embedding")));
    ASSERT_TRUE(vector_result);
    EXPECT_TRUE(vector_result->IsEmpty());
    EXPECT_TRUE(vector_result->GetScores().empty());
}

TEST_P(FileIndexSearchEvaluatorTest, TestEmptyFullTextSearchIndex) {
    FileIndexFormat::ColumnIndexes indexes;
    indexes["title"][MockSearchFileIndex::FULL_TEXT_IDENTIFIER] = nullptr;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes,
                         MockSearchFileIndex::Serialize(indexes, pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> meta, MakeIndex(bytes));
    ASSERT_OK_AND_ASSIGN(auto evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                    path_factory_, meta, fs_, pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> text_result,
                         evaluator->SearchFullText(MakeFullTextSearch("title")));
    ASSERT_TRUE(text_result);
    ASSERT_TRUE(text_result->IsEmpty());
    ASSERT_TRUE(text_result->GetScores().empty());
}

TEST_P(FileIndexSearchEvaluatorTest, TestInvalidVectorSearchIndexes) {
    CheckInvalidSearchIndexes(SearchIndexType::VECTOR);
}

TEST_P(FileIndexSearchEvaluatorTest, TestInvalidFullTextSearchIndexes) {
    CheckInvalidSearchIndexes(SearchIndexType::FULL_TEXT);
}

TEST_P(FileIndexSearchEvaluatorTest, TestEvaluateNullPredicateAndMissingIndex) {
    FileIndexFormat::ColumnIndexes indexes;
    indexes["title"]["bitmap"] = nullptr;
    ASSERT_OK_AND_ASSIGN(auto bytes, MockSearchFileIndex::Serialize(indexes, pool_));
    ASSERT_OK_AND_ASSIGN(auto meta, MakeIndex(bytes));
    ASSERT_OK_AND_ASSIGN(auto evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                    path_factory_, meta, fs_, pool_));
    ASSERT_OK_AND_ASSIGN(auto result, evaluator->Evaluate(nullptr));
    ASSERT_EQ(result, FileIndexResult::Remain());

    meta->embedded_index.reset();
    meta->extra_files.clear();
    auto missing = PredicateBuilder::IsNull(2, "missing", FieldType::INT);
    ASSERT_OK_AND_ASSIGN(evaluator, FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                               path_factory_, meta, fs_, pool_));
    ASSERT_OK_AND_ASSIGN(result, evaluator->Evaluate(missing));
    ASSERT_EQ(result, FileIndexResult::Remain());
    ASSERT_OK_AND_ASSIGN(result, evaluator->Evaluate(nullptr));
    ASSERT_EQ(result, FileIndexResult::Remain());
}

TEST_P(FileIndexSearchEvaluatorTest, TestEvaluateValidatesFieldsBeforeAndShortCircuit) {
    FileIndexFormat::ColumnIndexes indexes;
    indexes["title"]["bitmap"] = nullptr;
    ASSERT_OK_AND_ASSIGN(auto bytes, MockSearchFileIndex::Serialize(indexes, pool_));
    ASSERT_OK_AND_ASSIGN(auto meta, MakeIndex(bytes));
    ASSERT_OK_AND_ASSIGN(auto evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                    path_factory_, meta, fs_, pool_));
    auto empty = PredicateBuilder::IsNotNull(1, "title", FieldType::STRING);
    ASSERT_OK_AND_ASSIGN(auto result, evaluator->Evaluate(empty));
    ASSERT_OK_AND_ASSIGN(bool remain, result->IsRemain());
    ASSERT_FALSE(remain);

    auto missing = PredicateBuilder::IsNull(2, "z_missing", FieldType::INT);
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({empty, missing}));
    ASSERT_NOK_WITH_MSG(evaluator->Evaluate(predicate), "z_missing");
}

TEST_P(FileIndexSearchEvaluatorTest, TestSessionReturnsQueryScores) {
    MockSearchFileIndex::QueryMatches queries = {
        {"apple", {{{3, 0.9f}, {1, 0.5f}, {2, 0.3f}}, 0.75}},
        {"pear", {{{0, 0.8f}, {2, 0.2f}}, std::nullopt}}};
    ASSERT_OK_AND_ASSIGN(auto bytes, MockSearchFileIndex::MakeQueryIndex(
                                         "title", statistics_index_type_, queries, pool_));
    ASSERT_OK_AND_ASSIGN(auto meta, MakeIndex(bytes));
    ASSERT_OK_AND_ASSIGN(auto evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                    path_factory_, meta, fs_, pool_));
    ASSERT_OK(evaluator->GetSearchReader("title", FileIndexReader::SearchType::FULL_TEXT_MATCH));
    ASSERT_OK(evaluator->GetSearchReader("title", FileIndexReader::SearchType::FULL_TEXT_SEARCH));
    ASSERT_TRUE(statistics_->executions.empty());
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::FullTextMatch(
                                             1, std::make_shared<FullTextMatch>("title", "apple")));
    auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    ASSERT_TRUE(leaf);
    ASSERT_OK_AND_ASSIGN(auto estimate, evaluator->EstimateSelectivity(leaf));
    ASSERT_EQ(estimate, 0.75);
    ASSERT_OK_AND_ASSIGN(auto result, evaluator->EvaluateLeaf(leaf, true));
    auto scored = std::dynamic_pointer_cast<ScoredFileIndexResult>(result);
    ASSERT_TRUE(scored);
    ASSERT_EQ(scored->GetRowPositions(), RoaringBitmap32::From({1, 2, 3}));
    ASSERT_EQ(scored->GetScores(), std::vector<float>({0.5f, 0.3f, 0.9f}));
    ASSERT_EQ(statistics_->executions.size(), 1);
    ASSERT_TRUE(statistics_->executions.back().needs_scores);
    ASSERT_OK_AND_ASSIGN(result, evaluator->EvaluateLeaf(leaf, false));
    CheckResult(result, {1, 2, 3});
    ASSERT_FALSE(statistics_->executions.back().needs_scores);

    auto include = RoaringBitmap64::From({0, 2});
    auto search = std::make_shared<FullTextSearch>("title", "pear", 1, include);
    ASSERT_OK_AND_ASSIGN(scored, evaluator->SearchFullText(search));
    ASSERT_TRUE(scored);
    ASSERT_EQ(scored->GetRowPositions(), RoaringBitmap32::From({0}));
    ASSERT_EQ(scored->GetScores(), std::vector<float>({0.8f}));
    ASSERT_TRUE(statistics_->executions.back().needs_scores);
    ASSERT_EQ(statistics_->executions.size(), 3);

    ASSERT_OK_AND_ASSIGN(auto pear, PredicateBuilder::FullTextMatch(
                                        1, std::make_shared<FullTextMatch>("title", "pear")));
    ASSERT_OK_AND_ASSIGN(auto conjunction, PredicateBuilder::And({predicate, pear}));
    ASSERT_OK_AND_ASSIGN(result, evaluator->Evaluate(conjunction));
    CheckResult(result, {2});
    ASSERT_OK_AND_ASSIGN(auto disjunction, PredicateBuilder::Or({predicate, pear}));
    ASSERT_OK_AND_ASSIGN(result, evaluator->Evaluate(disjunction));
    CheckResult(result, {0, 1, 2, 3});
    ASSERT_EQ(statistics_->executions.size(), 7);

    ASSERT_OK_AND_ASSIGN(
        auto restricted,
        PredicateBuilder::FullTextMatch(
            1, std::make_shared<FullTextMatch>("title", "apple", RoaringBitmap64::From({1, 3}))));
    auto restricted_leaf = std::dynamic_pointer_cast<LeafPredicate>(restricted);
    ASSERT_TRUE(restricted_leaf);
    ASSERT_OK_AND_ASSIGN(result, evaluator->EvaluateLeaf(restricted_leaf, false));
    CheckResult(result, {1, 3});
    ASSERT_FALSE(statistics_->executions.back().needs_scores);
    ASSERT_OK_AND_ASSIGN(result, evaluator->EvaluateLeaf(restricted_leaf, true));
    scored = std::dynamic_pointer_cast<ScoredFileIndexResult>(result);
    ASSERT_TRUE(scored);
    ASSERT_EQ(scored->GetRowPositions(), RoaringBitmap32::From({1, 3}));
    ASSERT_EQ(scored->GetScores(), std::vector<float>({0.5f, 0.9f}));
    ASSERT_TRUE(statistics_->executions.back().needs_scores);
    ASSERT_EQ(statistics_->executions.size(), 9);
}

TEST_P(FileIndexSearchEvaluatorTest, TestSessionScalarAndSearchReadersCanCoexist) {
    auto field = arrow::field("title", arrow::utf8());
    ASSERT_OK_AND_ASSIGN(auto writer, BitmapFileIndexWriter::Create(field, {}, pool_));
    auto array_result = arrow::ipc::internal::json::ArrayFromJSON(
        arrow::struct_({field}), R"([["a"], ["b"], ["a"], ["b"]])");
    ASSERT_TRUE(array_result.ok()) << array_result.status().ToString();
    auto array = array_result.ValueUnsafe();
    ::ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    ASSERT_OK(writer->AddBatch(&c_array));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bitmap_payload, writer->SerializedBytes());
    ASSERT_OK_AND_ASSIGN(auto search_payload,
                         MockSearchFileIndex::MakePayload({{3, 0.9f}, {1, 0.5f}}, pool_));
    FileIndexFormat::ColumnIndexes indexes;
    indexes["title"]["bitmap"] = bitmap_payload;
    indexes["title"][statistics_index_type_] = search_payload;
    ASSERT_OK_AND_ASSIGN(auto bytes, MockSearchFileIndex::Serialize(indexes, pool_));
    ASSERT_OK_AND_ASSIGN(auto meta, MakeIndex(bytes));
    ASSERT_OK_AND_ASSIGN(auto evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                    path_factory_, meta, fs_, pool_));
    ASSERT_OK(evaluator->GetSearchReader("title", FileIndexReader::SearchType::FULL_TEXT_MATCH));
    ASSERT_NOK_WITH_MSG(
        evaluator->GetSearchReader("title", FileIndexReader::SearchType::VECTOR_SEARCH),
        "No File Index reader supports");
    ASSERT_TRUE(statistics_->executions.empty());
    auto scalar = std::dynamic_pointer_cast<LeafPredicate>(
        PredicateBuilder::Equal(1, "title", FieldType::STRING, Literal(FieldType::STRING, "a", 1)));
    ASSERT_TRUE(scalar);
    ASSERT_OK_AND_ASSIGN(auto estimate, evaluator->EstimateSelectivity(scalar));
    ASSERT_FALSE(estimate.has_value());
    ASSERT_OK_AND_ASSIGN(auto result, evaluator->EvaluateLeaf(scalar, false));
    CheckResult(result, {0, 2});
    auto other =
        PredicateBuilder::Equal(1, "title", FieldType::STRING, Literal(FieldType::STRING, "b", 1));
    ASSERT_OK_AND_ASSIGN(auto disjunction, PredicateBuilder::Or({scalar, other}));
    ASSERT_OK_AND_ASSIGN(result, evaluator->Evaluate(disjunction));
    CheckResult(result, {0, 1, 2, 3});
    ASSERT_OK_AND_ASSIGN(auto search_result,
                         evaluator->SearchFullText(MakeFullTextSearch("title")));
    ASSERT_EQ(search_result->GetRowPositions(), RoaringBitmap32::From({1, 3}));
    ASSERT_EQ(search_result->GetScores(), std::vector<float>({0.5f, 0.9f}));
    ASSERT_EQ(statistics_->executions.size(), 1);
    ASSERT_TRUE(statistics_->executions.back().needs_scores);
}

TEST_P(FileIndexSearchEvaluatorTest, TestSessionPrecheckAndMissingScalarIndex) {
    ASSERT_OK_AND_ASSIGN(auto bytes, MockSearchFileIndex::MakeIndex("title", statistics_index_type_,
                                                                    {{1, 0.5f}}, pool_));
    ASSERT_OK_AND_ASSIGN(auto meta, MakeIndex(bytes));
    ASSERT_OK_AND_ASSIGN(auto evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                    path_factory_, meta, fs_, pool_));
    ASSERT_NOK_WITH_MSG(
        evaluator->GetSearchReader("embedding", FileIndexReader::SearchType::VECTOR_SEARCH),
        "No File Index reader supports");
    ASSERT_NOK_WITH_MSG(
        evaluator->GetSearchReader("missing", FileIndexReader::SearchType::FULL_TEXT_MATCH),
        "does not exist in data schema");
    ASSERT_OK(evaluator->GetSearchReader("title", FileIndexReader::SearchType::FULL_TEXT_MATCH));
    ASSERT_TRUE(statistics_->executions.empty());
    auto scalar = std::dynamic_pointer_cast<LeafPredicate>(
        PredicateBuilder::IsNull(0, "embedding", FieldType::ARRAY));
    ASSERT_TRUE(scalar);
    ASSERT_OK_AND_ASSIGN(auto result, evaluator->EvaluateLeaf(scalar, false));
    ASSERT_EQ(result, FileIndexResult::Remain());
    ASSERT_OK_AND_ASSIGN(auto estimate, evaluator->EstimateSelectivity(scalar));
    ASSERT_FALSE(estimate.has_value());

    meta->embedded_index.reset();
    meta->extra_files.clear();
    ASSERT_OK_AND_ASSIGN(evaluator, FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                               path_factory_, meta, fs_, pool_));
    ASSERT_NOK_WITH_MSG(
        evaluator->GetSearchReader("title", FileIndexReader::SearchType::FULL_TEXT_MATCH),
        "has no File Index");
    ASSERT_OK_AND_ASSIGN(result, evaluator->EvaluateLeaf(scalar, false));
    ASSERT_EQ(result, FileIndexResult::Remain());
}

TEST_P(FileIndexSearchEvaluatorTest, TestEmptyScalarIndexDoesNotProvideSearchCapability) {
    FileIndexFormat::ColumnIndexes indexes;
    indexes["title"]["bitmap"] = nullptr;
    ASSERT_OK_AND_ASSIGN(auto bytes, MockSearchFileIndex::Serialize(indexes, pool_));
    ASSERT_OK_AND_ASSIGN(auto meta, MakeIndex(bytes));
    ASSERT_OK_AND_ASSIGN(auto evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                    path_factory_, meta, fs_, pool_));
    ASSERT_NOK_WITH_MSG(
        evaluator->GetSearchReader("title", FileIndexReader::SearchType::FULL_TEXT_MATCH),
        "No File Index reader supports FULL_TEXT_MATCH for field 'title' in data file "
        "'data-search.orc'");
    ASSERT_NOK_WITH_MSG(
        evaluator->GetSearchReader("title", FileIndexReader::SearchType::FULL_TEXT_SEARCH),
        "No File Index reader supports FULL_TEXT_SEARCH for field 'title' in data file "
        "'data-search.orc'");
    ASSERT_NOK_WITH_MSG(
        evaluator->GetSearchReader("title", FileIndexReader::SearchType::VECTOR_SEARCH),
        "No File Index reader supports VECTOR_SEARCH for field 'title' in data file "
        "'data-search.orc'");

    ASSERT_OK_AND_ASSIGN(indexes["title"][MockSearchFileIndex::FULL_TEXT_IDENTIFIER],
                         MockSearchFileIndex::MakePayload({{1, 0.5f}}, pool_));
    ASSERT_OK_AND_ASSIGN(bytes, MockSearchFileIndex::Serialize(indexes, pool_));
    ASSERT_OK_AND_ASSIGN(meta, MakeIndex(bytes));
    ASSERT_OK_AND_ASSIGN(evaluator, FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                               path_factory_, meta, fs_, pool_));
    ASSERT_OK(evaluator->GetSearchReader("title", FileIndexReader::SearchType::FULL_TEXT_MATCH));
    ASSERT_OK_AND_ASSIGN(auto result, evaluator->SearchFullText(MakeFullTextSearch("title")));
    ASSERT_EQ(result->GetRowPositions(), RoaringBitmap32::From({1}));
    ASSERT_EQ(result->GetScores(), std::vector<float>({0.5f}));
}

TEST_P(FileIndexSearchEvaluatorTest, TestEmptySearchIndexPreservesCapabilityAndScores) {
    FileIndexFormat::ColumnIndexes indexes;
    indexes["title"][MockSearchFileIndex::FULL_TEXT_IDENTIFIER] = nullptr;
    indexes["embedding"][MockSearchFileIndex::VECTOR_IDENTIFIER] = nullptr;
    ASSERT_OK_AND_ASSIGN(auto bytes, MockSearchFileIndex::Serialize(indexes, pool_));
    ASSERT_OK_AND_ASSIGN(auto meta, MakeIndex(bytes));
    ASSERT_OK_AND_ASSIGN(auto evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(),
                                                    path_factory_, meta, fs_, pool_));
    ASSERT_OK(evaluator->GetSearchReader("title", FileIndexReader::SearchType::FULL_TEXT_MATCH));
    ASSERT_OK(evaluator->GetSearchReader("title", FileIndexReader::SearchType::FULL_TEXT_SEARCH));
    ASSERT_NOK_WITH_MSG(
        evaluator->GetSearchReader("title", FileIndexReader::SearchType::VECTOR_SEARCH),
        "No File Index reader supports");
    ASSERT_OK(evaluator->GetSearchReader("embedding", FileIndexReader::SearchType::VECTOR_SEARCH));
    ASSERT_NOK_WITH_MSG(
        evaluator->GetSearchReader("embedding", FileIndexReader::SearchType::FULL_TEXT_MATCH),
        "No File Index reader supports");
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::FullTextMatch(
                                             1, std::make_shared<FullTextMatch>("title", "apple")));
    auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    ASSERT_TRUE(leaf);
    ASSERT_OK_AND_ASSIGN(auto result, evaluator->EvaluateLeaf(leaf, true));
    auto scored = std::dynamic_pointer_cast<ScoredFileIndexResult>(result);
    ASSERT_TRUE(scored);
    ASSERT_TRUE(scored->IsEmpty());
    ASSERT_TRUE(scored->GetScores().empty());
    ASSERT_OK_AND_ASSIGN(scored, evaluator->SearchFullText(MakeFullTextSearch("title")));
    ASSERT_TRUE(scored);
    ASSERT_TRUE(scored->IsEmpty());
}

INSTANTIATE_TEST_SUITE_P(IndexStorage, FileIndexSearchEvaluatorTest, ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& info) {
                             return info.param ? "Embedded" : "External";
                         });

TEST_F(FileIndexEvaluatorTest, TestUnsupportedRowCountSkips32BitIndex) {
    auto data_file_meta = std::make_shared<DataFileMeta>(
        "data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc", /*file_size=*/689,
        /*row_count=*/(1LL << 32), /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
        /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0,
        /*extra_files=*/
        std::vector<std::optional<std::string>>(
            {"data-414509f5-e40c-4245-b992-bbf486778ac9-0.orc.index"}),
        /*creation_time=*/Timestamp(0ll, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt,
        /*column_max_sequence_numbers=*/std::nullopt);
    auto predicate =
        PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
    ASSERT_OK_AND_ASSIGN(auto evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(), nullptr,
                                                    data_file_meta, nullptr, pool_));
    ASSERT_OK_AND_ASSIGN(auto result, evaluator->Evaluate(predicate));
    ASSERT_TRUE(result->IsRemain().value());
    ASSERT_EQ(dynamic_cast<BitmapIndexResult*>(result.get()), nullptr);

    data_file_meta->row_count = -1;
    ASSERT_OK_AND_ASSIGN(evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(), nullptr,
                                                    data_file_meta, nullptr, pool_));
    ASSERT_OK_AND_ASSIGN(result, evaluator->Evaluate(predicate));
    ASSERT_EQ(result, FileIndexResult::Remain());

    FileIndexFormat::ColumnIndexes indexes;
    indexes["f2"]["bitmap"] = nullptr;
    ASSERT_OK_AND_ASSIGN(data_file_meta->embedded_index,
                         MockSearchFileIndex::Serialize(indexes, pool_));
    ASSERT_OK_AND_ASSIGN(evaluator,
                         FileIndexEvaluator::Create(data_schema_, core_options_.ToMap(), nullptr,
                                                    data_file_meta, nullptr, pool_));
    auto missing = PredicateBuilder::IsNull(4, "missing", FieldType::INT);
    ASSERT_OK_AND_ASSIGN(result, evaluator->Evaluate(missing));
    ASSERT_EQ(result, FileIndexResult::Remain());
}

}  // namespace paimon::test

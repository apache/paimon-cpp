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

#include "paimon/common/reader/blob_fallback_batch_reader.h"

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/util/range.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/file_index/bitmap/apply_bitmap_index_batch_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/deletionvectors/apply_deletion_vector_batch_reader.h"
#include "paimon/core/deletionvectors/bitmap64_deletion_vector.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

/// "PH" stands for a placeholder row (the sentinel bytes emitted by the placeholder-aware blob
/// reader), std::nullopt for null.
using BlobRows = std::vector<std::optional<std::string>>;

struct BlobReadCounts {
    std::vector<int32_t> materialized;
    std::vector<int32_t> skipped;
};

class ReadSkipBlobReader : public FileBatchReader {
 public:
    ReadSkipBlobReader(const std::shared_ptr<arrow::Array>& data,
                       const std::shared_ptr<arrow::DataType>& schema, int32_t batch_size,
                       const std::shared_ptr<BlobReadContext>& context,
                       const std::shared_ptr<BlobReadCounts>& counts = nullptr)
        : data_(data),
          schema_(schema),
          context_(context),
          counts_(counts),
          batch_size_(batch_size) {}

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override {
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*schema_, c_schema.get()));
        return c_schema;
    }

    Status SetReadSchema(::ArrowSchema*, const std::shared_ptr<Predicate>&,
                         const std::optional<RoaringBitmap32>&) override {
        return Status::NotImplemented("Read/skip test reader does not support schema changes");
    }

    Result<ReadBatch> NextBatch() override {
        if (current_pos_ == data_->length()) {
            previous_batch_row_count_ = 0;
            return BatchReader::MakeEofBatch();
        }
        int32_t max_rows = context_->request ? context_->request->max_rows : batch_size_;
        bool read_payload = !context_->request || context_->request->read_payload;
        int32_t take = static_cast<int32_t>(
            std::min<int64_t>({batch_size_, max_rows, data_->length() - current_pos_}));
        std::shared_ptr<arrow::Array> array;
        if (read_payload) {
            array = data_->Slice(current_pos_, take);
        } else {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(array, arrow::MakeArrayOfNull(schema_, take));
        }
        if (counts_) {
            std::vector<int32_t>& rows = read_payload ? counts_->materialized : counts_->skipped;
            for (int32_t i = 0; i < take; i++) {
                rows.push_back(current_pos_ + i);
            }
        }
        previous_batch_first_row_num_ = current_pos_;
        previous_batch_row_count_ = take;
        current_pos_ += take;
        auto c_array = std::make_unique<ArrowArray>();
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));
        return std::make_pair(std::move(c_array), std::move(c_schema));
    }

    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override {
        if (batch_row_id >= previous_batch_row_count_) {
            return Status::Invalid("No matching row in the previous batch");
        }
        return previous_batch_first_row_num_ + batch_row_id;
    }

    Result<uint64_t> GetNumberOfRows() const override {
        return data_->length();
    }

    bool SupportPreciseBitmapSelection() const override {
        return false;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return nullptr;
    }

    void Close() override {}

 private:
    std::shared_ptr<arrow::Array> data_;
    std::shared_ptr<arrow::DataType> schema_;
    std::shared_ptr<BlobReadContext> context_;
    std::shared_ptr<BlobReadCounts> counts_;
    int32_t batch_size_;
    int64_t current_pos_ = 0;
    uint64_t previous_batch_first_row_num_ = 0;
    uint64_t previous_batch_row_count_ = 0;
};

class BlobFallbackBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        struct_type_ = arrow::struct_({BlobUtils::ToArrowField("blob_col", true)});
        read_schema_ = arrow::schema(struct_type_->fields());
    }

    static std::string Sentinel() {
        return std::string(BlobDefs::PlaceholderSentinelView());
    }

    std::shared_ptr<arrow::Array> MakeBlobStruct(const BlobRows& rows) const {
        arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                            {std::make_shared<arrow::LargeBinaryBuilder>()});
        auto blob_builder =
            checked_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
        for (const auto& row : rows) {
            EXPECT_TRUE(struct_builder.Append().ok());
            if (!row) {
                EXPECT_TRUE(blob_builder->AppendNull().ok());
            } else if (*row == "PH") {
                std::string sentinel = Sentinel();
                EXPECT_TRUE(blob_builder->Append(sentinel.data(), sentinel.size()).ok());
            } else {
                EXPECT_TRUE(blob_builder->Append(row->data(), row->size()).ok());
            }
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    }

    /// One segment of a group: File(rows) for a file segment, Gap(n) for a placeholder gap
    /// of n selected rows (with synthetic row ids when the schema does not read them, or the
    /// given ranges via GapRanges).
    struct SegmentSpec {
        std::vector<Range> gap_ranges;
        std::optional<BlobRows> file_rows;
        static SegmentSpec Gap(int64_t rows) {
            return SegmentSpec{{Range(0, rows - 1)}, std::nullopt};
        }
        static SegmentSpec GapRanges(std::vector<Range> ranges) {
            return SegmentSpec{std::move(ranges), std::nullopt};
        }
        static SegmentSpec File(BlobRows rows) {
            return SegmentSpec{{}, std::move(rows)};
        }
    };

    std::vector<BlobFallbackBatchReader::Segment> MakeGroup(
        const std::vector<SegmentSpec>& specs, int32_t file_batch_size,
        const std::shared_ptr<BlobReadCounts>& counts = nullptr) const {
        std::vector<BlobFallbackBatchReader::Segment> segments;
        for (const auto& spec : specs) {
            if (spec.file_rows) {
                auto context = std::make_shared<BlobReadContext>();
                auto reader = std::make_unique<ReadSkipBlobReader>(MakeBlobStruct(*spec.file_rows),
                                                                   struct_type_, file_batch_size,
                                                                   context, counts);
                segments.push_back(
                    BlobFallbackBatchReader::Segment{std::move(reader), {}, std::move(context)});
            } else {
                segments.push_back(BlobFallbackBatchReader::Segment{nullptr, spec.gap_ranges});
            }
        }
        return segments;
    }

    /// Runs the fallback over the groups with several batch sizes and compares to expected rows.
    void CheckFallback(const std::vector<std::vector<SegmentSpec>>& group_specs,
                       const BlobRows& expected_rows) const {
        auto expected_array = MakeBlobStruct(expected_rows);
        for (auto batch_size : arrow::internal::Iota(1, 8)) {
            for (auto file_batch_size : {1, 3, 1024}) {
                std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
                groups.reserve(group_specs.size());
                for (const auto& specs : group_specs) {
                    groups.push_back(MakeGroup(specs, file_batch_size));
                }
                ASSERT_OK_AND_ASSIGN(
                    auto reader, BlobFallbackBatchReader::Create(std::move(groups), read_schema_,
                                                                 batch_size, GetArrowPool(pool_)));
                ASSERT_OK_AND_ASSIGN(auto result, paimon::test::ReadResultCollector::CollectResult(
                                                      std::move(reader)));
                auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
                ASSERT_TRUE(result->Equals(expected_chunk_array))
                    << "batch_size=" << batch_size << " file_batch_size=" << file_batch_size
                    << "\nresult: " << result->ToString()
                    << "\nexpected: " << expected_chunk_array->ToString();
            }
        }
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::DataType> struct_type_;
    std::shared_ptr<arrow::Schema> read_schema_;
};

TEST_F(BlobFallbackBatchReaderTest, TestBasicFallback) {
    // newer layer updates row 1 only; rows 0 and 2 fall back to the older layer
    CheckFallback(
        {{SegmentSpec::File({"PH", "u1", "PH"})}, {SegmentSpec::File({"b0", "b1", "b2"})}},
        {"b0", "u1", "b2"});
}

TEST_F(BlobFallbackBatchReaderTest, TestAlternatingUpdates) {
    CheckFallback({{SegmentSpec::File({"u0", "PH", "u2", "PH", "u4", "PH", "u6", "PH"})},
                   {SegmentSpec::File({"b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7"})}},
                  {"u0", "b1", "u2", "b3", "u4", "b5", "u6", "b7"});
}

TEST_F(BlobFallbackBatchReaderTest, TestSkipsFullyCoveredLayersIncludingNull) {
    const BlobRows newest = {"u0", std::nullopt, "u2"};
    for (int32_t batch_size : {1, 2, 1024}) {
        for (int32_t file_batch_size : {1, 3, 1024}) {
            auto counts = std::make_shared<BlobReadCounts>();
            std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
            groups.push_back(MakeGroup({SegmentSpec::File(newest)}, file_batch_size));
            groups.push_back(
                MakeGroup({SegmentSpec::File({"b0", "b1", "b2"})}, file_batch_size, counts));
            ASSERT_OK_AND_ASSIGN(auto reader,
                                 BlobFallbackBatchReader::Create(std::move(groups), read_schema_,
                                                                 batch_size, GetArrowPool(pool_)));
            ASSERT_OK_AND_ASSIGN(auto result,
                                 ReadResultCollector::CollectResult(std::move(reader)));
            ASSERT_TRUE(
                result->Equals(std::make_shared<arrow::ChunkedArray>(MakeBlobStruct(newest))));
            ASSERT_TRUE(counts->materialized.empty());
            ASSERT_EQ(counts->skipped, (std::vector<int32_t>{0, 1, 2}));
        }
    }
}

TEST_F(BlobFallbackBatchReaderTest, TestReadSkipRunsAcrossWindowsAndThreeLayers) {
    for (int32_t batch_size : {1, 2, 4, 1024}) {
        for (int32_t file_batch_size : {1, 3, 1024}) {
            auto middle_counts = std::make_shared<BlobReadCounts>();
            auto oldest_counts = std::make_shared<BlobReadCounts>();
            std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
            groups.push_back(MakeGroup({SegmentSpec::File({"u0", "PH", "PH", "u3", "PH", "PH"})},
                                       file_batch_size));
            groups.push_back(
                MakeGroup({SegmentSpec::File({"stale", "m1", "PH", "stale", "PH", "m5"})},
                          file_batch_size, middle_counts));
            groups.push_back(
                MakeGroup({SegmentSpec::File({"stale", "stale", "o2", "stale", "o4", "stale"})},
                          file_batch_size, oldest_counts));
            ASSERT_OK_AND_ASSIGN(auto reader,
                                 BlobFallbackBatchReader::Create(std::move(groups), read_schema_,
                                                                 batch_size, GetArrowPool(pool_)));
            ASSERT_OK_AND_ASSIGN(auto result,
                                 ReadResultCollector::CollectResult(std::move(reader)));
            ASSERT_TRUE(result->Equals(std::make_shared<arrow::ChunkedArray>(
                MakeBlobStruct({"u0", "m1", "o2", "u3", "o4", "m5"}))));
            ASSERT_EQ(middle_counts->materialized, (std::vector<int32_t>{1, 2, 4, 5}));
            ASSERT_EQ(middle_counts->skipped, (std::vector<int32_t>{0, 3}));
            ASSERT_EQ(oldest_counts->materialized, (std::vector<int32_t>{2, 4}));
            ASSERT_EQ(oldest_counts->skipped, (std::vector<int32_t>{0, 1, 3, 5}));
        }
    }
}

TEST_F(BlobFallbackBatchReaderTest, TestSkippedGroupsStillDetectMisalignment) {
    for (const BlobRows& older : {BlobRows{"b0"}, BlobRows{"b0", "b1", "b2"}}) {
        auto counts = std::make_shared<BlobReadCounts>();
        std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
        groups.push_back(MakeGroup({SegmentSpec::File({"u0", "u1"})}, 1024));
        groups.push_back(MakeGroup({SegmentSpec::File(older)}, 1024, counts));
        ASSERT_OK_AND_ASSIGN(auto reader,
                             BlobFallbackBatchReader::Create(std::move(groups), read_schema_, 2,
                                                             GetArrowPool(pool_)));
        ASSERT_NOK_WITH_MSG(ReadResultCollector::CollectResult(std::move(reader)),
                            "same number of rows");
        ASSERT_TRUE(counts->materialized.empty());
        ASSERT_EQ(counts->skipped.size(), older.size());
    }
}

TEST_F(BlobFallbackBatchReaderTest, TestRejectsBatchExceedingReadLimit) {
    for (bool covered : {false, true}) {
        auto context = std::make_shared<BlobReadContext>();
        // This mock ignores the context and returns two rows for a one-row read/skip request.
        auto oversized_reader =
            std::make_unique<MockFileBatchReader>(MakeBlobStruct({"b0", "b1"}), struct_type_, 1024);
        std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
        groups.push_back(MakeGroup(
            {SegmentSpec::File(covered ? BlobRows{"u0", "u1"} : BlobRows{"PH", "PH"})}, 1024));
        std::vector<BlobFallbackBatchReader::Segment> older;
        older.push_back({std::move(oversized_reader), {}, context});
        groups.push_back(std::move(older));
        ASSERT_OK_AND_ASSIGN(auto reader,
                             BlobFallbackBatchReader::Create(std::move(groups), read_schema_, 1,
                                                             GetArrowPool(pool_)));
        ASSERT_NOK_WITH_MSG(reader->NextBatch(), "exceeding requested limit 1");
        ASSERT_FALSE(context->request.has_value());
    }
}

TEST_F(BlobFallbackBatchReaderTest, TestSkipRequestsPreserveBitmapAndDvFiltering) {
    auto counts = std::make_shared<BlobReadCounts>();
    auto context = std::make_shared<BlobReadContext>();
    auto raw = std::make_unique<ReadSkipBlobReader>(MakeBlobStruct({"b0", "b1", "b2", "b3", "b4"}),
                                                    struct_type_, 1024, context, counts);
    auto bitmap_reader = std::make_unique<ApplyBitmapIndexBatchReader>(
        std::move(raw), RoaringBitmap32::From({1, 2, 3, 4}));
    auto dv = std::make_shared<Bitmap64DeletionVector>();
    ASSERT_OK(dv->Delete(2));
    auto filtered_reader =
        std::make_unique<ApplyDeletionVectorBatchReader>(std::move(bitmap_reader), dv);
    std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
    groups.push_back(MakeGroup({SegmentSpec::File({"u1", "u3", "PH"})}, 1024));
    std::vector<BlobFallbackBatchReader::Segment> older;
    older.push_back({std::move(filtered_reader), {}, context});
    groups.push_back(std::move(older));
    ASSERT_OK_AND_ASSIGN(auto reader, BlobFallbackBatchReader::Create(
                                          std::move(groups), read_schema_, 3, GetArrowPool(pool_)));
    ASSERT_OK_AND_ASSIGN(auto result, ReadResultCollector::CollectResult(std::move(reader)));
    ASSERT_TRUE(
        result->Equals(std::make_shared<arrow::ChunkedArray>(MakeBlobStruct({"u1", "u3", "b4"}))));
    ASSERT_EQ(counts->skipped, (std::vector<int32_t>{0, 1, 2, 3}));
    ASSERT_EQ(counts->materialized, (std::vector<int32_t>{4}));
    ASSERT_FALSE(context->request.has_value());
}

TEST_F(BlobFallbackBatchReaderTest, TestSingleLayerFillsUnwrittenRows) {
    CheckFallback({{SegmentSpec::Gap(2), SegmentSpec::File({"b2", "PH"}), SegmentSpec::Gap(2)}},
                  {std::nullopt, std::nullopt, "b2", std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(BlobFallbackBatchReaderTest, TestGapPadding) {
    // the newer layer only covers rows 2-3; the gaps stand for placeholders
    CheckFallback({{SegmentSpec::Gap(2), SegmentSpec::File({"u2", "PH"})},
                   {SegmentSpec::File({"b0", "b1", "b2", "b3"})}},
                  {"b0", "b1", "u2", "b3"});
    // trailing gap
    CheckFallback({{SegmentSpec::File({"PH", "u1"}), SegmentSpec::Gap(2)},
                   {SegmentSpec::File({"b0", "b1", "b2", "b3"})}},
                  {"b0", "u1", "b2", "b3"});
    // middle gap between two files of one layer
    CheckFallback({{SegmentSpec::File({"u0"}), SegmentSpec::Gap(2), SegmentSpec::File({"u3"})},
                   {SegmentSpec::File({"b0", "b1", "b2", "b3"})}},
                  {"u0", "b1", "b2", "u3"});
    // a gap segment covering multiple disjoint selected ranges
    CheckFallback({{SegmentSpec::GapRanges({Range(0, 0), Range(2, 2)}), SegmentSpec::File({"u3"})},
                   {SegmentSpec::File({"b0", "b2", "b3"})}},
                  {"b0", "b2", "u3"});
}

TEST_F(BlobFallbackBatchReaderTest, TestAllPlaceholdersBecomesNull) {
    // a row that is a placeholder in every layer degrades to null
    CheckFallback({{SegmentSpec::File({"PH", "PH"})}, {SegmentSpec::File({"b0", "PH"})}},
                  {"b0", std::nullopt});
    CheckFallback({{SegmentSpec::Gap(2)}, {SegmentSpec::File({"PH", "PH"})}},
                  {std::nullopt, std::nullopt});
}

TEST_F(BlobFallbackBatchReaderTest, TestNullIsNotPlaceholder) {
    // a real null in a newer layer wins: null means "updated to null", not "not updated"
    CheckFallback({{SegmentSpec::File({std::nullopt, "u1"})}, {SegmentSpec::File({"b0", "b1"})}},
                  {std::nullopt, "u1"});
}

TEST_F(BlobFallbackBatchReaderTest, TestSentinelPrefixedValueIsNotPlaceholder) {
    // placeholders are identified by exact equality with the sentinel bytes only: a real value
    // that merely starts with them passes through unchanged, whether it falls back or wins as
    // the newest layer
    std::string prefixed = Sentinel() + "suffix";
    CheckFallback({{SegmentSpec::File({"PH", "u1"})}, {SegmentSpec::File({prefixed, "b1"})}},
                  {prefixed, "u1"});
    CheckFallback({{SegmentSpec::File({prefixed, "PH"})}, {SegmentSpec::File({"b0", "b1"})}},
                  {prefixed, "b1"});
}

TEST_F(BlobFallbackBatchReaderTest, TestThreeLayers) {
    CheckFallback({{SegmentSpec::File({"PH", "PH", "u2"})},
                   {SegmentSpec::File({"PH", "m1", "PH"})},
                   {SegmentSpec::File({"b0", "b1", "b2"})}},
                  {"b0", "m1", "u2"});
}

TEST_F(BlobFallbackBatchReaderTest, TestLayeredFilesAndGaps) {
    // mirrors the compacted-sequence-groups shape: layers partially cover [0, 9]
    CheckFallback(
        {{SegmentSpec::Gap(6), SegmentSpec::File({"u66", "PH"}), SegmentSpec::Gap(1),
          SegmentSpec::File({"u69"})},
         {SegmentSpec::File({"u40", "PH", "PH", "PH"}), SegmentSpec::Gap(4),
          SegmentSpec::File({"u48", "PH"})},
         {SegmentSpec::File({"b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9"})}},
        {"u40", "b1", "b2", "b3", "b4", "b5", "u66", "b7", "u48", "u69"});
}

TEST_F(BlobFallbackBatchReaderTest, TestRowTrackingFieldsPreserved) {
    // Row-tracking projections: resolved rows keep their layer's row id and sequence number;
    // an all-placeholder row keeps its row id (here provided by the newest group's gap
    // segment), reports -1 as its sequence number, and degrades the blob to null. Covers the
    // schema variants {blob, _ROW_ID, _SEQUENCE_NUMBER}, {blob, _ROW_ID} and
    // {blob, _SEQUENCE_NUMBER}.
    struct RowSpec {
        std::optional<std::string> blob;
        int64_t row_id;
        int64_t seq_num;
    };
    for (bool with_row_id : {true, false}) {
        for (bool with_seq_num : {true, false}) {
            if (!with_row_id && !with_seq_num) {
                continue;
            }
            arrow::FieldVector fields = {BlobUtils::ToArrowField("blob_col", true)};
            if (with_row_id) {
                fields.push_back(SpecialFields::RowId().ArrowField());
            }
            if (with_seq_num) {
                fields.push_back(SpecialFields::SequenceNumber().ArrowField());
            }
            auto struct_type = arrow::struct_(fields);
            auto schema = arrow::schema(fields);

            auto make_rows = [&](const std::vector<RowSpec>& rows) {
                std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders = {
                    std::make_shared<arrow::LargeBinaryBuilder>()};
                for (size_t i = 1; i < fields.size(); i++) {
                    field_builders.push_back(std::make_shared<arrow::Int64Builder>());
                }
                arrow::StructBuilder struct_builder(struct_type, arrow::default_memory_pool(),
                                                    std::move(field_builders));
                auto blob_builder =
                    checked_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
                for (const auto& row : rows) {
                    EXPECT_TRUE(struct_builder.Append().ok());
                    if (!row.blob) {
                        EXPECT_TRUE(blob_builder->AppendNull().ok());
                    } else if (*row.blob == "PH") {
                        std::string sentinel = Sentinel();
                        EXPECT_TRUE(blob_builder->Append(sentinel.data(), sentinel.size()).ok());
                    } else {
                        EXPECT_TRUE(blob_builder->Append(row.blob->data(), row.blob->size()).ok());
                    }
                    int32_t next_field = 1;
                    if (with_row_id) {
                        auto builder = checked_cast<arrow::Int64Builder*>(
                            struct_builder.field_builder(next_field++));
                        EXPECT_TRUE(builder->Append(row.row_id).ok());
                    }
                    if (with_seq_num) {
                        auto builder = checked_cast<arrow::Int64Builder*>(
                            struct_builder.field_builder(next_field));
                        EXPECT_TRUE(builder->Append(row.seq_num).ok());
                    }
                }
                std::shared_ptr<arrow::Array> array;
                EXPECT_TRUE(struct_builder.Finish(&array).ok());
                return array;
            };

            for (auto batch_size : arrow::internal::Iota(1, 9)) {
                for (auto file_batch_size : {1, 3, 1024}) {
                    auto new_context = std::make_shared<BlobReadContext>();
                    auto old_context = std::make_shared<BlobReadContext>();
                    // Rows 0-1 are a gap. Inside one file chunk, alternate newest-layer values
                    // with placeholders so normal and all-placeholder row-id runs share it.
                    std::vector<BlobFallbackBatchReader::Segment> newest;
                    newest.push_back(BlobFallbackBatchReader::Segment{nullptr, {Range(0, 1)}});
                    newest.push_back(BlobFallbackBatchReader::Segment{
                        std::make_unique<ReadSkipBlobReader>(make_rows({{"u2", 2, 20},
                                                                        {"PH", 3, 20},
                                                                        {"u4", 4, 20},
                                                                        {"PH", 5, 20},
                                                                        {"u6", 6, 20}}),
                                                             struct_type, file_batch_size,
                                                             new_context),
                        {},
                        new_context});
                    // Rows 1 and 3 are placeholders in both layers; row 5 falls back.
                    std::vector<BlobFallbackBatchReader::Segment> oldest;
                    oldest.push_back(BlobFallbackBatchReader::Segment{
                        std::make_unique<ReadSkipBlobReader>(make_rows({{"b0", 0, 10},
                                                                        {"PH", 1, 10},
                                                                        {"PH", 2, 10},
                                                                        {"PH", 3, 10},
                                                                        {"PH", 4, 10},
                                                                        {"b5", 5, 10},
                                                                        {"PH", 6, 10}}),
                                                             struct_type, file_batch_size,
                                                             old_context),
                        {},
                        old_context});
                    std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
                    groups.push_back(std::move(newest));
                    groups.push_back(std::move(oldest));

                    ASSERT_OK_AND_ASSIGN(auto reader, BlobFallbackBatchReader::Create(
                                                          std::move(groups), schema, batch_size,
                                                          GetArrowPool(pool_)));
                    ASSERT_OK_AND_ASSIGN(
                        auto result,
                        paimon::test::ReadResultCollector::CollectResult(std::move(reader)));

                    auto expected_array = make_rows({{"b0", 0, 10},
                                                     {std::nullopt, 1, -1},
                                                     {"u2", 2, 20},
                                                     {std::nullopt, 3, -1},
                                                     {"u4", 4, 20},
                                                     {"b5", 5, 10},
                                                     {"u6", 6, 20}});
                    auto expected_chunk_array =
                        std::make_shared<arrow::ChunkedArray>(expected_array);
                    ASSERT_TRUE(result->Equals(expected_chunk_array))
                        << "with_row_id=" << with_row_id << " with_seq_num=" << with_seq_num
                        << " batch_size=" << batch_size << " file_batch_size=" << file_batch_size
                        << "\nresult: " << result->ToString()
                        << "\nexpected: " << expected_chunk_array->ToString();
                }
            }
        }
    }
}

TEST_F(BlobFallbackBatchReaderTest, TestMisalignedGroupsFail) {
    std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
    groups.push_back(MakeGroup({SegmentSpec::File({"PH", "u1", "PH"})}, 1024));
    groups.push_back(MakeGroup({SegmentSpec::File({"b0", "b1"})}, 1024));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         BlobFallbackBatchReader::Create(std::move(groups), read_schema_, 1024,
                                                         GetArrowPool(pool_)));
    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "same number of rows");
}

TEST_F(BlobFallbackBatchReaderTest, TestCreateValidation) {
    std::vector<std::vector<BlobFallbackBatchReader::Segment>> empty_groups;
    ASSERT_NOK_WITH_MSG(BlobFallbackBatchReader::Create(std::move(empty_groups), read_schema_, 1024,
                                                        GetArrowPool(pool_)),
                        "at least one sequence group");

    // the read schema must contain a blob field
    std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
    groups.push_back(MakeGroup({SegmentSpec::File({"b0"})}, 1024));
    groups.push_back(MakeGroup({SegmentSpec::File({"b1"})}, 1024));
    auto plain_schema =
        arrow::schema({arrow::field("not_blob", arrow::large_binary(), /*nullable=*/true)});
    ASSERT_NOK_WITH_MSG(
        BlobFallbackBatchReader::Create(std::move(groups), plain_schema, 1024, GetArrowPool(pool_)),
        "should contain a blob field");

    // groups must not be empty
    std::vector<std::vector<BlobFallbackBatchReader::Segment>> with_empty_group;
    with_empty_group.push_back(MakeGroup({SegmentSpec::File({"b0"})}, 1024));
    with_empty_group.emplace_back();
    ASSERT_NOK_WITH_MSG(BlobFallbackBatchReader::Create(std::move(with_empty_group), read_schema_,
                                                        1024, GetArrowPool(pool_)),
                        "should not be empty");

    // a gap segment must cover at least one selected row id
    std::vector<std::vector<BlobFallbackBatchReader::Segment>> with_empty_gap;
    with_empty_gap.push_back(MakeGroup({SegmentSpec::File({"b0"})}, 1024));
    with_empty_gap.push_back(MakeGroup({SegmentSpec::GapRanges({})}, 1024));
    ASSERT_NOK_WITH_MSG(BlobFallbackBatchReader::Create(std::move(with_empty_gap), read_schema_,
                                                        1024, GetArrowPool(pool_)),
                        "at least one selected row id");
}

}  // namespace paimon::test

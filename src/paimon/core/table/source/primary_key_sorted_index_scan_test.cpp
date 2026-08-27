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

#include "paimon/core/table/source/primary_key_sorted_index_scan.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/global_index/btree/key_serializer.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/index/pk/primary_key_index_definitions.h"
#include "paimon/core/index/pksorted/pk_sorted_index_file.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/table/source/primary_key_sorted_index_result.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
constexpr int32_t kPriceFieldId = 1;
constexpr int64_t kSnapshotId = 7;
constexpr int64_t kFileARows = 100;
constexpr int64_t kFileBRows = 200;
constexpr int64_t kTotalRows = kFileARows + kFileBRows;

class TestGlobalIndexFileWriter : public GlobalIndexFileWriter {
 public:
    TestGlobalIndexFileWriter(const std::shared_ptr<FileSystem>& fs, const std::string& base_path)
        : fs_(fs), base_path_(base_path) {}

    Result<std::string> NewFileName(const std::string& prefix) const override {
        return fmt::format("{}-index-{}", prefix, file_counter_++);
    }

    Result<std::unique_ptr<OutputStream>> NewOutputStream(
        const std::string& file_name) const override {
        return fs_->Create(base_path_ + "/" + file_name, true);
    }

    Result<int64_t> GetFileSize(const std::string& file_name) const override {
        PAIMON_ASSIGN_OR_RAISE(FileStatus file_status,
                               fs_->GetFileStatus(base_path_ + "/" + file_name));
        return file_status.GetLen();
    }

    std::string ToPath(const std::string& file_name) const override {
        return base_path_ + "/" + file_name;
    }

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string base_path_;
    mutable int64_t file_counter_ = 0;
};

class TestGlobalIndexFileReader : public GlobalIndexFileReader {
 public:
    explicit TestGlobalIndexFileReader(const std::shared_ptr<FileSystem>& fs) : fs_(fs) {}

    Result<std::unique_ptr<InputStream>> GetInputStream(
        const std::string& file_path) const override {
        return fs_->Open(file_path);
    }

 private:
    std::shared_ptr<FileSystem> fs_;
};

/// A reader stub whose equality result is fully controlled by the test, used to exercise
/// the untrusted-position fallbacks.
class StubGlobalIndexReader : public GlobalIndexReader {
 public:
    explicit StubGlobalIndexReader(RoaringBitmap64 equal_result)
        : equal_result_(std::move(equal_result)) {}

    StubGlobalIndexReader(RoaringBitmap64 equal_result, std::shared_ptr<int32_t> equal_call_count)
        : equal_result_(std::move(equal_result)), equal_call_count_(std::move(equal_call_count)) {}

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override {
        if (equal_call_count_ != nullptr) {
            (*equal_call_count_)++;
        }
        RoaringBitmap64 copy = equal_result_;
        return std::make_shared<BitmapGlobalIndexResult>(
            [bitmap = std::move(copy)]() -> Result<RoaringBitmap64> { return bitmap; });
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(
        const Literal& literal) override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override {
        return NotEvaluable();
    }
    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override {
        return Status::Invalid("not supported");
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override {
        return Status::Invalid("not supported");
    }
    bool IsThreadSafe() const override {
        return false;
    }
    std::string GetIndexType() const override {
        return "btree";
    }

 private:
    static Result<std::shared_ptr<GlobalIndexResult>> NotEvaluable() {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }

    RoaringBitmap64 equal_result_;
    std::shared_ptr<int32_t> equal_call_count_;
};
}  // namespace

class PrimaryKeySortedIndexScanTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
        test_dir_ = UniqueTestDirectory::Create("local");
        fs_ = test_dir_->GetFileSystem();
        base_path_ = test_dir_->Str();

        std::vector<DataField> fields = {
            DataField(0, arrow::field("id", arrow::int64())),
            DataField(kPriceFieldId, arrow::field("price", arrow::int64())),
            DataField(2, arrow::field("status", arrow::utf8())),
        };
        std::map<std::string, std::string> options = {{"pk-btree.index.columns", "price"}};
        table_schema_ = std::make_shared<TableSchema>(
            /*version=*/3, /*id=*/0, fields, /*highest_field_id=*/2,
            /*partition_keys=*/std::vector<std::string>(),
            /*primary_keys=*/std::vector<std::string>{"id"}, options,
            /*comment=*/std::nullopt, /*time_millis=*/0);
        ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexDefinitions definitions,
                             PrimaryKeyIndexDefinitions::Create(*table_schema_));
        definitions_ = definitions.ScalarDefinitions();
        ASSERT_EQ(definitions_.size(), 1);
    }

    std::shared_ptr<DataFileMeta> MakeDataFile(const std::string& name, int64_t row_count,
                                               int32_t level, const FileSource& file_source,
                                               std::optional<int64_t> delete_row_count = 0) {
        return std::make_shared<DataFileMeta>(
            name, /*file_size=*/1024, row_count,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(), /*value_stats=*/SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/row_count, /*schema_id=*/0, level,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1721643142456LL, 0), delete_row_count,
            /*embedded_index=*/nullptr, file_source,
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt,
            /*column_max_sequence_numbers=*/std::nullopt);
    }

    Result<std::shared_ptr<IndexFileMeta>> BuildPayload(std::vector<int64_t> ordinals,
                                                        const std::string& writer_base_path,
                                                        bool is_external_path) {
        std::vector<PrimaryKeyIndexSourceFile> source_files = {{"a.parquet", kFileARows},
                                                               {"b.parquet", kFileBRows}};
        arrow::Int64Builder values_builder;
        for (int64_t i = 0; i < kTotalRows; i++) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(values_builder.Append(2 * i));
        }
        std::shared_ptr<arrow::Array> sorted_values;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(values_builder.Finish(&sorted_values));
        PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema_->GetField(kPriceFieldId));
        auto file_writer = std::make_shared<TestGlobalIndexFileWriter>(fs_, writer_base_path);
        return PkSortedIndexFile::Build(field, "btree", definitions_[0].Options(),
                                        /*data_level=*/5, source_files, sorted_values,
                                        std::move(ordinals), file_writer, is_external_path, pool_);
    }

    Result<std::shared_ptr<IndexFileMeta>> BuildPayload(std::vector<int64_t> ordinals) {
        return BuildPayload(std::move(ordinals), base_path_, /*is_external_path=*/false);
    }

    /// Builds the standard payload of this fixture: sources a.parquet(100) + b.parquet(200)
    /// on level 5, indexed value at group ordinal `i` is `2 * i`.
    Result<std::shared_ptr<IndexFileMeta>> BuildPayload() {
        std::vector<int64_t> ordinals;
        ordinals.reserve(kTotalRows);
        for (int64_t i = 0; i < kTotalRows; i++) {
            ordinals.push_back(i);
        }
        return BuildPayload(std::move(ordinals));
    }

    std::shared_ptr<DataSplitImpl> MakeSplit(
        const std::vector<std::shared_ptr<DataFileMeta>>& files, bool raw_convertible,
        const std::vector<std::optional<DeletionFile>>& deletion_files = {}) {
        std::vector<std::shared_ptr<DataFileMeta>> data_files = files;
        DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0,
                                       base_path_ + "/bucket-0", std::move(data_files));
        builder.WithSnapshot(kSnapshotId).IsStreaming(false).RawConvertible(raw_convertible);
        if (!deletion_files.empty()) {
            builder.WithDataDeletionFiles(deletion_files);
        }
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> split, builder.Build());
        return split;
    }

    std::vector<IndexManifestEntry> MakeEntries(const std::shared_ptr<IndexFileMeta>& payload) {
        return {IndexManifestEntry(FileKind::Add(), BinaryRow::EmptyRow(), /*bucket=*/0, payload)};
    }

    PrimaryKeySortedIndexScan::ReaderFactory PayloadReaderFactory() {
        std::shared_ptr<FileSystem> fs = fs_;
        std::string base_path = base_path_;
        std::shared_ptr<TableSchema> table_schema = table_schema_;
        std::shared_ptr<MemoryPool> pool = pool_;
        return [fs, base_path, table_schema, pool](
                   const PrimaryKeySortedIndexScan::FilePlan& file,
                   const PrimaryKeyIndexDefinition& definition,
                   const PkSortedIndexGroup& group) -> Result<std::shared_ptr<GlobalIndexReader>> {
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<GlobalIndexer> indexer,
                GlobalIndexerFactory::Get(definition.IndexType(), definition.Options()));
            if (indexer == nullptr) {
                return Status::Invalid("btree indexer is not registered");
            }
            const std::shared_ptr<IndexFileMeta>& payload = group.Payload();
            std::vector<GlobalIndexIOMeta> io_metas;
            io_metas.emplace_back(base_path + "/" + payload->FileName(), payload->FileSize(),
                                  payload->GetGlobalIndexMeta().value().index_meta);
            PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(definition.FieldId()));
            auto arrow_field = DataField::ConvertDataFieldToArrowField(field);
            auto arrow_schema = arrow::schema({arrow_field});
            ArrowSchema c_arrow_schema;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
            auto file_reader = std::make_shared<TestGlobalIndexFileReader>(fs);
            return indexer->CreateReader(&c_arrow_schema, file_reader, io_metas, pool);
        };
    }

    Result<std::vector<std::shared_ptr<Split>>> PlanEvaluateConvert(
        const std::vector<std::shared_ptr<DataSplitImpl>>& splits,
        const std::vector<IndexManifestEntry>& entries, const std::shared_ptr<Predicate>& predicate,
        const PrimaryKeySortedIndexScan::ReaderFactory& reader_factory) {
        PAIMON_ASSIGN_OR_RAISE(
            PrimaryKeySortedIndexScan::Plan plan,
            PrimaryKeySortedIndexScan::CreatePlan(kSnapshotId, splits, definitions_, entries));
        PAIMON_ASSIGN_OR_RAISE(PrimaryKeySortedIndexScan::EvaluatedPlan evaluated,
                               PrimaryKeySortedIndexScan::Evaluate(plan, table_schema_, predicate,
                                                                   definitions_, reader_factory));
        return PrimaryKeySortedIndexResult::ToSplits(evaluated);
    }

    std::shared_ptr<Predicate> PriceEqual(int64_t value) {
        return PredicateBuilder::Equal(/*field_index=*/1, "price", FieldType::BIGINT,
                                       Literal(value));
    }

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<UniqueTestDirectory> test_dir_;
    std::shared_ptr<FileSystem> fs_;
    std::string base_path_;
    std::shared_ptr<TableSchema> table_schema_;
    std::vector<PrimaryKeyIndexDefinition> definitions_;
};

TEST_F(PrimaryKeySortedIndexScanTest, EqualNarrowsToSingleFileRange) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    std::shared_ptr<DataSplitImpl> split =
        MakeSplit({MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact()),
                   MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact())},
                  /*raw_convertible=*/true);
    // Value 10 sits at group ordinal 5, i.e. row 5 of a.parquet.
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<Split>> splits,
        PlanEvaluateConvert({split}, MakeEntries(payload), PriceEqual(10), PayloadReaderFactory()));
    ASSERT_EQ(splits.size(), 1);
    auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(splits[0]);
    ASSERT_TRUE(indexed_split != nullptr);
    auto inner_split = std::dynamic_pointer_cast<DataSplitImpl>(indexed_split->GetDataSplit());
    ASSERT_TRUE(inner_split != nullptr);
    ASSERT_FALSE(inner_split->RawConvertible());
    ASSERT_EQ(inner_split->DataFiles().size(), 1);
    ASSERT_EQ(inner_split->DataFiles()[0]->file_name, "a.parquet");
    ASSERT_EQ(indexed_split->RowRanges().size(), 1);
    ASSERT_EQ(indexed_split->RowRanges()[0].from, 5);
    ASSERT_EQ(indexed_split->RowRanges()[0].to, 5);
}

TEST_F(PrimaryKeySortedIndexScanTest, BuildRejectsDuplicateOrdinals) {
    std::vector<int64_t> ordinals;
    ordinals.reserve(kTotalRows);
    for (int64_t i = 0; i < kTotalRows; i++) {
        ordinals.push_back(i);
    }
    ordinals[1] = 0;
    ASSERT_NOK_WITH_MSG(BuildPayload(std::move(ordinals)), "Row id 0 appears more than once");
}

TEST_F(PrimaryKeySortedIndexScanTest, ExternalPayloadPathIsNormalized) {
    std::vector<int64_t> ordinals;
    ordinals.reserve(kTotalRows);
    for (int64_t i = 0; i < kTotalRows; i++) {
        ordinals.push_back(i);
    }
    std::string writer_base_path = base_path_ + "//";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<IndexFileMeta> payload,
        BuildPayload(std::move(ordinals), writer_base_path, /*is_external_path=*/true));
    ASSERT_TRUE(payload->ExternalPath().has_value());
    ASSERT_OK_AND_ASSIGN(std::string normalized_path,
                         PathUtil::NormalizePath(writer_base_path + "/" + payload->FileName()));
    ASSERT_EQ(normalized_path, payload->ExternalPath().value());
}

TEST_F(PrimaryKeySortedIndexScanTest, RangeSpansFileBoundary) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    std::shared_ptr<DataSplitImpl> split =
        MakeSplit({MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact()),
                   MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact())},
                  /*raw_convertible=*/true);
    // Values in [190, 210] sit at group ordinals 95..105: rows 95..99 of a.parquet and
    // rows 0..5 of b.parquet.
    std::shared_ptr<Predicate> lower = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/1, "price", FieldType::BIGINT, Literal(static_cast<int64_t>(190)));
    std::shared_ptr<Predicate> upper = PredicateBuilder::LessOrEqual(
        /*field_index=*/1, "price", FieldType::BIGINT, Literal(static_cast<int64_t>(210)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({lower, upper}));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<Split>> splits,
        PlanEvaluateConvert({split}, MakeEntries(payload), predicate, PayloadReaderFactory()));
    ASSERT_EQ(splits.size(), 2);
    auto indexed_a = std::dynamic_pointer_cast<IndexedSplitImpl>(splits[0]);
    auto indexed_b = std::dynamic_pointer_cast<IndexedSplitImpl>(splits[1]);
    ASSERT_TRUE(indexed_a != nullptr);
    ASSERT_TRUE(indexed_b != nullptr);
    ASSERT_EQ(indexed_a->RowRanges().size(), 1);
    ASSERT_EQ(indexed_a->RowRanges()[0].from, 95);
    ASSERT_EQ(indexed_a->RowRanges()[0].to, 99);
    ASSERT_EQ(indexed_b->RowRanges().size(), 1);
    ASSERT_EQ(indexed_b->RowRanges()[0].from, 0);
    ASSERT_EQ(indexed_b->RowRanges()[0].to, 5);
}

TEST_F(PrimaryKeySortedIndexScanTest, GroupAndQueryAreSharedAcrossSourceFiles) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    std::shared_ptr<DataSplitImpl> split =
        MakeSplit({MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact()),
                   MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact())},
                  /*raw_convertible=*/true);
    ASSERT_OK_AND_ASSIGN(PrimaryKeySortedIndexScan::Plan plan,
                         PrimaryKeySortedIndexScan::CreatePlan(kSnapshotId, {split}, definitions_,
                                                               MakeEntries(payload)));
    ASSERT_EQ(2, plan.Files().size());
    ASSERT_EQ(plan.Files()[0].Group(kPriceFieldId), plan.Files()[1].Group(kPriceFieldId));

    RoaringBitmap64 positions;
    positions.Add(5);
    auto equal_call_count = std::make_shared<int32_t>(0);
    PrimaryKeySortedIndexScan::ReaderFactory reader_factory =
        [positions, equal_call_count](
            const PrimaryKeySortedIndexScan::FilePlan& file,
            const PrimaryKeyIndexDefinition& definition,
            const PkSortedIndexGroup& group) -> Result<std::shared_ptr<GlobalIndexReader>> {
        return std::make_shared<StubGlobalIndexReader>(positions, equal_call_count);
    };
    ASSERT_OK(PrimaryKeySortedIndexScan::Evaluate(plan, table_schema_, PriceEqual(10), definitions_,
                                                  reader_factory));
    ASSERT_EQ(1, *equal_call_count);
}

TEST_F(PrimaryKeySortedIndexScanTest, EmptyResultOmitsAllFiles) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    std::shared_ptr<DataSplitImpl> split =
        MakeSplit({MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact()),
                   MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact())},
                  /*raw_convertible=*/true);
    // All indexed values are even, so 11 matches nothing.
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<Split>> splits,
        PlanEvaluateConvert({split}, MakeEntries(payload), PriceEqual(11), PayloadReaderFactory()));
    ASSERT_TRUE(splits.empty());
}

TEST_F(PrimaryKeySortedIndexScanTest, UnindexedFieldPredicateFallsBack) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    std::shared_ptr<DataSplitImpl> split =
        MakeSplit({MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact()),
                   MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact())},
                  /*raw_convertible=*/true);
    std::shared_ptr<Predicate> predicate = PredicateBuilder::Equal(
        /*field_index=*/2, "status", FieldType::STRING, Literal(FieldType::STRING, "hit", 3));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<Split>> splits,
        PlanEvaluateConvert({split}, MakeEntries(payload), predicate, PayloadReaderFactory()));
    ASSERT_EQ(1, splits.size());
    ASSERT_EQ(split, splits[0]);
}

TEST_F(PrimaryKeySortedIndexScanTest, UncoveredFileFallsBackOthersNarrow) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    std::shared_ptr<DataSplitImpl> split =
        MakeSplit({MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact()),
                   MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact()),
                   MakeDataFile("c.parquet", 50, 0, FileSource::Append())},
                  /*raw_convertible=*/true);
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<Split>> splits,
        PlanEvaluateConvert({split}, MakeEntries(payload), PriceEqual(10), PayloadReaderFactory()));
    // a.parquet narrows to an indexed split, b.parquet is omitted, c.parquet has no
    // coverage and keeps a normal single-file scan.
    ASSERT_EQ(splits.size(), 2);
    auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(splits[0]);
    ASSERT_TRUE(indexed_split != nullptr);
    auto fallback_split = std::dynamic_pointer_cast<DataSplitImpl>(splits[1]);
    ASSERT_TRUE(fallback_split != nullptr);
    ASSERT_FALSE(fallback_split->RawConvertible());
    ASSERT_EQ(fallback_split->DataFiles().size(), 1);
    ASSERT_EQ(fallback_split->DataFiles()[0]->file_name, "c.parquet");
}

TEST_F(PrimaryKeySortedIndexScanTest, NonRawConvertibleSplitPreserved) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    std::shared_ptr<DataSplitImpl> split =
        MakeSplit({MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact()),
                   MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact())},
                  /*raw_convertible=*/false);
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<Split>> splits,
        PlanEvaluateConvert({split}, MakeEntries(payload), PriceEqual(10), PayloadReaderFactory()));
    ASSERT_EQ(splits.size(), 1);
    ASSERT_EQ(splits[0].get(), split.get());
}

TEST_F(PrimaryKeySortedIndexScanTest, UnknownDeleteCountPreservesOriginalSplit) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    std::shared_ptr<DataSplitImpl> split =
        MakeSplit({MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact(), std::nullopt),
                   MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact())},
                  /*raw_convertible=*/true);
    ASSERT_OK_AND_ASSIGN(PrimaryKeySortedIndexScan::Plan plan,
                         PrimaryKeySortedIndexScan::CreatePlan(kSnapshotId, {split}, definitions_,
                                                               MakeEntries(payload)));
    ASSERT_TRUE(plan.Files()[0].Groups().empty());
    ASSERT_TRUE(plan.Files()[1].Groups().empty());

    for (int64_t value : {10, 11}) {
        SCOPED_TRACE(value == 10 ? "non-empty index result" : "empty index result");
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> splits,
                             PlanEvaluateConvert({split}, MakeEntries(payload), PriceEqual(value),
                                                 PayloadReaderFactory()));
        ASSERT_EQ(splits.size(), 1);
        ASSERT_EQ(splits[0], split);
    }
}

TEST_F(PrimaryKeySortedIndexScanTest, NonzeroDeleteCountPreservesOriginalSplit) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    std::shared_ptr<DataSplitImpl> split =
        MakeSplit({MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact(), 1),
                   MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact())},
                  /*raw_convertible=*/true);
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<Split>> splits,
        PlanEvaluateConvert({split}, MakeEntries(payload), PriceEqual(10), PayloadReaderFactory()));
    ASSERT_EQ(splits.size(), 1);
    ASSERT_EQ(splits[0], split);
}

TEST_F(PrimaryKeySortedIndexScanTest, InvalidRowRangePayloadFallsBack) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    // Rebuild the payload metadata with a row range end beyond the source rows: the group
    // validation must reject it and every file keeps a normal scan.
    const GlobalIndexMeta& meta = payload->GetGlobalIndexMeta().value();
    auto broken_payload = std::make_shared<IndexFileMeta>(
        payload->IndexType(), payload->FileName(), payload->FileSize(), payload->RowCount(),
        std::nullopt, std::nullopt,
        GlobalIndexMeta(meta.row_range_start, meta.row_range_end + 1, meta.index_field_id,
                        meta.extra_field_ids, meta.index_meta, meta.source_meta));
    std::shared_ptr<DataSplitImpl> split =
        MakeSplit({MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact()),
                   MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact())},
                  /*raw_convertible=*/true);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> splits,
                         PlanEvaluateConvert({split}, MakeEntries(broken_payload), PriceEqual(10),
                                             PayloadReaderFactory()));
    ASSERT_EQ(1, splits.size());
    ASSERT_EQ(split, splits[0]);
}

TEST_F(PrimaryKeySortedIndexScanTest, MalformedBTreeMetadataFallsBack) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    const GlobalIndexMeta& meta = payload->GetGlobalIndexMeta().value();
    auto short_key = std::make_shared<Bytes>(std::string(1, '\0'), pool_.get());
    auto invalid_key_meta = std::make_shared<BTreeIndexMeta>(short_key, short_key, false);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> first_key,
                         KeySerializer::SerializeKey(Literal(static_cast<int64_t>(10)),
                                                     arrow::int64(), pool_.get()));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Bytes> last_key,
        KeySerializer::SerializeKey(Literal(static_cast<int64_t>(1)), arrow::int64(), pool_.get()));
    auto reversed_meta = std::make_shared<BTreeIndexMeta>(first_key, last_key, false);
    auto only_first_meta = std::make_shared<BTreeIndexMeta>(first_key, /*last_key=*/nullptr, false);
    auto only_last_meta = std::make_shared<BTreeIndexMeta>(/*first_key=*/nullptr, last_key, false);
    auto empty_nonnull_meta =
        std::make_shared<BTreeIndexMeta>(/*first_key=*/nullptr, /*last_key=*/nullptr, false);
    std::vector<std::shared_ptr<Bytes>> malformed_metadata = {
        nullptr,
        std::make_shared<Bytes>(std::string(4, '\0'), pool_.get()),
        invalid_key_meta->Serialize(pool_.get()),
        reversed_meta->Serialize(pool_.get()),
        only_first_meta->Serialize(pool_.get()),
        only_last_meta->Serialize(pool_.get()),
        empty_nonnull_meta->Serialize(pool_.get())};
    for (const std::shared_ptr<Bytes>& index_meta : malformed_metadata) {
        SCOPED_TRACE(index_meta == nullptr ? "missing metadata"
                                           : fmt::format("metadata size {}", index_meta->size()));
        auto broken_payload = std::make_shared<IndexFileMeta>(
            payload->IndexType(), payload->FileName(), payload->FileSize(), payload->RowCount(),
            std::nullopt, std::nullopt,
            GlobalIndexMeta(meta.row_range_start, meta.row_range_end, meta.index_field_id,
                            meta.extra_field_ids, index_meta, meta.source_meta));
        std::shared_ptr<DataSplitImpl> split =
            MakeSplit({MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact()),
                       MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact())},
                      /*raw_convertible=*/true);
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> splits,
                             PlanEvaluateConvert({split}, MakeEntries(broken_payload),
                                                 PriceEqual(10), PayloadReaderFactory()));
        ASSERT_EQ(splits.size(), 1);
        ASSERT_EQ(splits[0], split);
    }
}

TEST_F(PrimaryKeySortedIndexScanTest, OutOfRangePositionsFailAllCoveredFiles) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    std::shared_ptr<DataSplitImpl> split =
        MakeSplit({MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact()),
                   MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact())},
                  /*raw_convertible=*/true);
    RoaringBitmap64 poisoned;
    poisoned.Add(5);
    poisoned.Add(kTotalRows + 10);
    PrimaryKeySortedIndexScan::ReaderFactory stub_factory =
        [&poisoned](const PrimaryKeySortedIndexScan::FilePlan& file,
                    const PrimaryKeyIndexDefinition& definition,
                    const PkSortedIndexGroup& group) -> Result<std::shared_ptr<GlobalIndexReader>> {
        return std::make_shared<StubGlobalIndexReader>(poisoned);
    };
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<Split>> splits,
        PlanEvaluateConvert({split}, MakeEntries(payload), PriceEqual(10), stub_factory));
    // Both covered files fall back together, preserving the planner's original bin packing.
    ASSERT_EQ(1, splits.size());
    ASSERT_EQ(split, splits[0]);
}

TEST_F(PrimaryKeySortedIndexScanTest, OverFragmentedResultFallsBack) {
    // One data file, 20000 rows; every second row selected produces > 4096 ranges.
    std::vector<PrimaryKeyIndexSourceFile> source_files = {{"big.parquet", 20000}};
    std::shared_ptr<DataSplitImpl> split = MakeSplit(
        {MakeDataFile("big.parquet", 20000, 5, FileSource::Compact())}, /*raw_convertible=*/true);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Bytes> source_meta_bytes, ([&]() -> Result<std::shared_ptr<Bytes>> {
            PAIMON_ASSIGN_OR_RAISE(PrimaryKeyIndexSourceMeta source_meta,
                                   PrimaryKeyIndexSourceMeta::Create(5, source_files));
            return source_meta.Serialize(pool_);
        }()));
    auto big_payload = std::make_shared<IndexFileMeta>(
        "btree", "big-index-file", /*file_size=*/1, /*row_count=*/20000, std::nullopt, std::nullopt,
        GlobalIndexMeta(0, 19999, kPriceFieldId, std::nullopt, nullptr, source_meta_bytes));
    RoaringBitmap64 fragmented;
    for (int64_t i = 0; i < 20000; i += 2) {
        fragmented.Add(i);
    }
    PrimaryKeySortedIndexScan::ReaderFactory stub_factory =
        [&fragmented](
            const PrimaryKeySortedIndexScan::FilePlan& file,
            const PrimaryKeyIndexDefinition& definition,
            const PkSortedIndexGroup& group) -> Result<std::shared_ptr<GlobalIndexReader>> {
        return std::make_shared<StubGlobalIndexReader>(fragmented);
    };
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<Split>> splits,
        PlanEvaluateConvert({split}, MakeEntries(big_payload), PriceEqual(10), stub_factory));
    ASSERT_EQ(splits.size(), 1);
    ASSERT_TRUE(std::dynamic_pointer_cast<IndexedSplitImpl>(splits[0]) == nullptr);
}

TEST_F(PrimaryKeySortedIndexScanTest, DeletionFileStaysAlignedWithIndexedFile) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    DeletionFile deletion_file("dv-a", /*offset=*/0, /*length=*/16, /*cardinality=*/1);
    std::shared_ptr<DataSplitImpl> split = MakeSplit(
        {MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact()),
         MakeDataFile("b.parquet", kFileBRows, 5, FileSource::Compact())},
        /*raw_convertible=*/true, {std::optional<DeletionFile>(deletion_file), std::nullopt});
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<Split>> splits,
        PlanEvaluateConvert({split}, MakeEntries(payload), PriceEqual(10), PayloadReaderFactory()));
    ASSERT_EQ(splits.size(), 1);
    auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(splits[0]);
    ASSERT_TRUE(indexed_split != nullptr);
    auto inner_split = std::dynamic_pointer_cast<DataSplitImpl>(indexed_split->GetDataSplit());
    ASSERT_TRUE(inner_split != nullptr);
    ASSERT_EQ(inner_split->DeletionFiles().size(), 1);
    ASSERT_TRUE(inner_split->DeletionFiles()[0] != std::nullopt);
    ASSERT_EQ(inner_split->DeletionFiles()[0].value().path, "dv-a");
}

TEST_F(PrimaryKeySortedIndexScanTest, SnapshotMismatchIsRejected) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> payload, BuildPayload());
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        MakeDataFile("a.parquet", kFileARows, 5, FileSource::Compact())};
    DataSplitImpl::Builder builder(BinaryRow::EmptyRow(), /*bucket=*/0, base_path_,
                                   std::move(files));
    builder.WithSnapshot(kSnapshotId + 1).IsStreaming(false).RawConvertible(true);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> split, builder.Build());
    ASSERT_NOK(PrimaryKeySortedIndexScan::CreatePlan(kSnapshotId, {split}, definitions_,
                                                     MakeEntries(payload)));
}

}  // namespace paimon::test

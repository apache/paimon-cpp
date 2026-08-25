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

#include "paimon/core/table/format/lazy_concat_batch_reader.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

/// A reader over a fixed number of one-row batches that records when it was closed.
class OneColumnBatchReader : public BatchReader {
 public:
    OneColumnBatchReader(int32_t value, int32_t batches, bool* closed_flag)
        : value_(value),
          remaining_(batches),
          closed_flag_(closed_flag),
          metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        if (remaining_-- <= 0) {
            return BatchReader::MakeEofBatch();
        }
        arrow::Int32Builder builder;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(value_));
        std::shared_ptr<arrow::Array> column;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&column));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> struct_array,
            arrow::StructArray::Make({column}, std::vector<std::string>{"id"}));
        auto c_array = std::make_unique<ArrowArray>();
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportArray(*struct_array, c_array.get(), c_schema.get()));
        return std::make_pair(std::move(c_array), std::move(c_schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        if (closed_flag_ != nullptr) {
            *closed_flag_ = true;
        }
    }

 private:
    int32_t value_;
    int32_t remaining_;
    bool* closed_flag_;
    std::shared_ptr<Metrics> metrics_;
};

/// A reader that fails on its first batch, standing for a file that opens and then turns out to
/// be unreadable - a truncated record, a value no type can hold.
class FailingBatchReader : public BatchReader {
 public:
    explicit FailingBatchReader(Status failure)
        : failure_(std::move(failure)), metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        return failure_;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {}

 private:
    Status failure_;
    std::shared_ptr<Metrics> metrics_;
};

/// Names factories "file-0", "file-1", ... A source carries the name of the file it reads so a
/// failure can point at one; which name it is does not matter to these tests.
std::vector<LazyConcatBatchReader::Source> SourcesOf(
    std::vector<LazyConcatBatchReader::ReaderFactory>&& factories) {
    std::vector<LazyConcatBatchReader::Source> sources;
    sources.reserve(factories.size());
    for (size_t i = 0; i < factories.size(); i++) {
        sources.push_back({"file-" + std::to_string(i), std::move(factories[i])});
    }
    return sources;
}

int64_t DrainRows(BatchReader* reader) {
    int64_t rows = 0;
    while (true) {
        Result<BatchReader::ReadBatch> batch = reader->NextBatch();
        if (!batch.ok() || BatchReader::IsEofBatch(batch.value())) {
            return rows;
        }
        rows += batch.value().first->length;
        ReaderUtils::ReleaseReadBatch(std::move(batch).value());
    }
}

}  // namespace

TEST(LazyConcatBatchReaderTest, TestAReaderIsOpenedOnlyWhenItIsReached) {
    // A split holds a whole partition's files, and a reader keeps its file open. Building them all
    // up front would hold one descriptor per file before a single row came back.
    int32_t opened = 0;
    std::vector<LazyConcatBatchReader::ReaderFactory> factories;
    for (int32_t i = 0; i < 3; i++) {
        factories.push_back([&opened, i]() -> Result<std::unique_ptr<BatchReader>> {
            opened++;
            return std::make_unique<OneColumnBatchReader>(i, /*batches=*/1, nullptr);
        });
    }
    LazyConcatBatchReader reader(SourcesOf(std::move(factories)), GetDefaultPool());
    ASSERT_EQ(opened, 0);

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch first, reader.NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(first));
    ReaderUtils::ReleaseReadBatch(std::move(first));
    ASSERT_EQ(opened, 1);
    ASSERT_EQ(DrainRows(&reader), 2);
    ASSERT_EQ(opened, 3);
}

TEST(LazyConcatBatchReaderTest, TestAReaderIsClosedAsSoonAsItRunsOut) {
    bool first_closed = false;
    bool second_closed = false;
    std::vector<LazyConcatBatchReader::ReaderFactory> factories;
    factories.push_back([&first_closed]() -> Result<std::unique_ptr<BatchReader>> {
        return std::make_unique<OneColumnBatchReader>(0, /*batches=*/1, &first_closed);
    });
    factories.push_back([&second_closed]() -> Result<std::unique_ptr<BatchReader>> {
        return std::make_unique<OneColumnBatchReader>(1, /*batches=*/1, &second_closed);
    });
    LazyConcatBatchReader reader(SourcesOf(std::move(factories)), GetDefaultPool());

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch first, reader.NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(first));
    ReaderUtils::ReleaseReadBatch(std::move(first));
    ASSERT_FALSE(first_closed);
    // Reaching the second file's rows means the first file is done with, so it is let go at once
    // rather than at the end of the split.
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch second, reader.NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(second));
    ReaderUtils::ReleaseReadBatch(std::move(second));
    ASSERT_TRUE(first_closed);
    ASSERT_FALSE(second_closed);
}

TEST(LazyConcatBatchReaderTest, TestClosingDoesNotOpenWhatWasNeverReached) {
    int32_t opened = 0;
    std::vector<LazyConcatBatchReader::ReaderFactory> factories;
    for (int32_t i = 0; i < 3; i++) {
        factories.push_back([&opened, i]() -> Result<std::unique_ptr<BatchReader>> {
            opened++;
            return std::make_unique<OneColumnBatchReader>(i, /*batches=*/1, nullptr);
        });
    }
    LazyConcatBatchReader reader(SourcesOf(std::move(factories)), GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch first, reader.NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(first));
    ReaderUtils::ReleaseReadBatch(std::move(first));
    reader.Close();
    ASSERT_EQ(opened, 1);
    // And nothing is opened afterwards either.
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof, reader.NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof));
    ASSERT_EQ(opened, 1);
}

TEST(LazyConcatBatchReaderTest, TestAFailureIsTerminal) {
    // `BatchReader` says a failure must not be retried: the reader keeps answering with the same
    // error. Moving on to the next file would let a caller that ignored the error read a
    // partition with a file silently missing from it.
    int32_t opened = 0;
    std::vector<LazyConcatBatchReader::ReaderFactory> factories;
    factories.push_back([&opened]() -> Result<std::unique_ptr<BatchReader>> {
        opened++;
        return Status::IOError("cannot open the file");
    });
    factories.push_back([&opened]() -> Result<std::unique_ptr<BatchReader>> {
        opened++;
        return std::make_unique<OneColumnBatchReader>(1, /*batches=*/1, nullptr);
    });
    LazyConcatBatchReader reader(SourcesOf(std::move(factories)), GetDefaultPool());

    Result<BatchReader::ReadBatch> first = reader.NextBatch();
    ASSERT_FALSE(first.ok());
    ASSERT_NOK_WITH_MSG(first, "cannot read file-0");
    // The second call answers with the same failure and never reaches the second file.
    Result<BatchReader::ReadBatch> second = reader.NextBatch();
    ASSERT_FALSE(second.ok());
    ASSERT_NOK_WITH_MSG(second, "cannot read file-0");
    ASSERT_TRUE(second.status().IsIOError());
    ASSERT_EQ(opened, 1);
}

TEST(LazyConcatBatchReaderTest, TestAFailureNamesTheFileItCameFrom) {
    // A split holds a whole partition, so "the file ended inside a quoted value" says nothing a
    // caller could act on until it says which file. Both halves are covered: the file that would
    // not open, and the file that opened and then failed part way through.
    std::vector<LazyConcatBatchReader::ReaderFactory> failing_open;
    failing_open.push_back([]() -> Result<std::unique_ptr<BatchReader>> {
        return Status::NotImplemented("the compression cannot be decoded");
    });
    LazyConcatBatchReader open_reader(SourcesOf(std::move(failing_open)), GetDefaultPool());
    Result<BatchReader::ReadBatch> not_opened = open_reader.NextBatch();
    ASSERT_FALSE(not_opened.ok());
    ASSERT_NOK_WITH_MSG(not_opened, "cannot read file-0");
    ASSERT_NOK_WITH_MSG(not_opened, "the compression cannot be decoded");
    // The original code survives, or a caller that tells "not supported" from "bad input" would
    // stop being able to.
    ASSERT_TRUE(not_opened.status().IsNotImplemented());

    std::vector<LazyConcatBatchReader::ReaderFactory> failing_read;
    failing_read.push_back([]() -> Result<std::unique_ptr<BatchReader>> {
        return std::make_unique<OneColumnBatchReader>(0, /*batches=*/1, nullptr);
    });
    failing_read.push_back([]() -> Result<std::unique_ptr<BatchReader>> {
        return std::make_unique<FailingBatchReader>(Status::Invalid("the file ended mid-record"));
    });
    LazyConcatBatchReader read_reader(SourcesOf(std::move(failing_read)), GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch first, read_reader.NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(first));
    ReaderUtils::ReleaseReadBatch(std::move(first));
    Result<BatchReader::ReadBatch> failed = read_reader.NextBatch();
    ASSERT_FALSE(failed.ok());
    // The second file, not the first: the name follows the reader in hand.
    ASSERT_NOK_WITH_MSG(failed, "cannot read file-1");
    ASSERT_NOK_WITH_MSG(failed, "the file ended mid-record");
    ASSERT_TRUE(failed.status().IsInvalid());
}

TEST(LazyConcatBatchReaderTest, TestAFactoryMayDeclineToOpenAnything) {
    // A factory that returns no reader stands for a file with nothing to read; the next one is
    // reached instead of the read ending there.
    std::vector<LazyConcatBatchReader::ReaderFactory> factories;
    factories.push_back(
        []() -> Result<std::unique_ptr<BatchReader>> { return std::unique_ptr<BatchReader>(); });
    factories.push_back([]() -> Result<std::unique_ptr<BatchReader>> {
        return std::make_unique<OneColumnBatchReader>(7, /*batches=*/2, nullptr);
    });
    LazyConcatBatchReader reader(SourcesOf(std::move(factories)), GetDefaultPool());
    ASSERT_EQ(DrainRows(&reader), 2);
}

TEST(LazyConcatBatchReaderTest, TestMetricsCoverReadersThatAreAlreadyClosed) {
    std::vector<LazyConcatBatchReader::ReaderFactory> factories;
    factories.push_back([]() -> Result<std::unique_ptr<BatchReader>> {
        return std::make_unique<OneColumnBatchReader>(0, /*batches=*/1, nullptr);
    });
    LazyConcatBatchReader reader(SourcesOf(std::move(factories)), GetDefaultPool());
    ASSERT_EQ(DrainRows(&reader), 1);
    // The totals have to survive the reader they were taken from, or everything read through a
    // file would vanish from them the moment that file is closed.
    ASSERT_NE(reader.GetReaderMetrics(), nullptr);
}

}  // namespace paimon::test

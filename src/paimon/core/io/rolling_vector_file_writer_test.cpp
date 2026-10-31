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

#include "paimon/core/io/rolling_vector_file_writer.h"

#include <limits>
#include <tuple>
#include <utility>

#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

struct WriterState {
    bool fail_create = false;
    bool fail_write = false;
    bool fail_close = false;
    bool fail_size = false;
    bool fail_result = false;
    int64_t row_count_delta = 0;
    int64_t file_number = 0;
    std::vector<std::string> paths;
};

class TestVectorWriter : public RollingVectorFileWriter::Writer {
 public:
    TestVectorWriter(const std::shared_ptr<FileSystem>& fs, const std::string& path,
                     const std::shared_ptr<WriterState>& state)
        : SingleFileWriter("none", {}), state_(state) {
        fs_ = fs;
        path_ = path;
    }

    Status Write(::ArrowArray* record) override {
        ScopeGuard guard([&]() { ArrowArrayRelease(record); });
        if (state_->fail_write) {
            return Status::IOError("injected write failure");
        }
        rows_ += record->length;
        return Status::OK();
    }

    Status Close() override {
        if (state_->fail_close) {
            return Status::IOError("injected close failure");
        }
        closed_ = true;
        return Status::OK();
    }

    Result<bool> ReachTargetSize(bool, int64_t target_size) override {
        if (state_->fail_size) {
            return Status::IOError("injected size check failure");
        }
        return rows_ >= target_size;
    }

    Result<std::shared_ptr<DataFileMeta>> GetResult() override {
        if (state_->fail_result) {
            return Status::IOError("injected result failure");
        }
        return DataFileMeta::ForAppend(PathUtil::GetName(path_), 0, rows_ + state_->row_count_delta,
                                       SimpleStats::EmptyStats(), 0, rows_ - 1, 0,
                                       FileSource::Append(), std::nullopt, std::nullopt,
                                       std::nullopt, std::nullopt);
    }

 private:
    std::shared_ptr<WriterState> state_;
    int64_t rows_ = 0;
};

class TestVectorWriterFactory : public RollingVectorFileWriter::WriterFactory {
 public:
    TestVectorWriterFactory(std::shared_ptr<FileSystem> fs, std::string prefix,
                            std::shared_ptr<WriterState> state)
        : fs_(std::move(fs)), prefix_(std::move(prefix)), state_(std::move(state)) {}

    Result<std::unique_ptr<RollingVectorFileWriter::Writer>> CreateWriter() const override {
        if (state_->fail_create) {
            return Status::IOError("injected create failure");
        }
        std::string path = fmt::format("{}{}", prefix_, state_->file_number++);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> stream,
                               fs_->Create(path, /*overwrite=*/false));
        PAIMON_RETURN_NOT_OK(stream->Close());
        state_->paths.push_back(path);
        return std::make_unique<TestVectorWriter>(fs_, path, state_);
    }

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string prefix_;
    std::shared_ptr<WriterState> state_;
};

}  // namespace

class RollingVectorFileWriterTest : public ::testing::Test {
 protected:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create("local");
        ASSERT_NE(dir_, nullptr);
        fs_ = dir_->GetFileSystem();
        main_state_ = std::make_shared<WriterState>();
        vector_state_ = std::make_shared<WriterState>();
        schema_ = arrow::schema({arrow::field("id", arrow::int32()),
                                 arrow::field("v", arrow::fixed_size_list(arrow::float32(), 2))});
    }

    std::unique_ptr<RollingVectorFileWriter> NewWriter(
        int64_t main_size = 1024, int64_t main_rows = std::numeric_limits<int64_t>::max(),
        int64_t vector_size = 1) const {
        auto main_factory = std::make_shared<TestVectorWriterFactory>(
            fs_, PathUtil::JoinPath(dir_->Str(), "main"), main_state_);
        auto vector_factory = std::make_shared<TestVectorWriterFactory>(
            fs_, PathUtil::JoinPath(dir_->Str(), "vector"), vector_state_);
        return std::make_unique<RollingVectorFileWriter>(main_size, main_rows, vector_size,
                                                         main_factory, vector_factory, schema_, 1);
    }

    Status WriteRows(RollingVectorFileWriter* writer, const std::string& json) const {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema_->fields()), json));
        ::ArrowArray batch = {};
        ScopeGuard guard([&]() { ArrowArrayRelease(&batch); });
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &batch));
        return writer->Write(&batch);
    }

    void CheckFilesRemoved() const {
        for (const auto& state : {main_state_, vector_state_}) {
            for (const auto& path : state->paths) {
                ASSERT_OK_AND_ASSIGN(bool exists, fs_->Exists(path));
                ASSERT_FALSE(exists) << path;
            }
        }
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<WriterState> main_state_;
    std::shared_ptr<WriterState> vector_state_;
    std::shared_ptr<arrow::Schema> schema_;
};

TEST_F(RollingVectorFileWriterTest, VectorTargetRollsIndependently) {
    auto writer = NewWriter(1024, std::numeric_limits<int64_t>::max(), 2);
    ASSERT_OK(WriteRows(writer.get(), R"([[1, [1, 2]], [2, null]])"));
    ASSERT_OK(WriteRows(writer.get(), R"([[3, null], [4, [3, 4]]])"));
    ASSERT_EQ(main_state_->paths.size(), 1);
    ASSERT_EQ(vector_state_->paths.size(), 2);
    ASSERT_OK(WriteRows(writer.get(), R"([[5, null]])"));
    ASSERT_NOK(writer->GetResult());
    ASSERT_OK(writer->Close());
    ASSERT_OK(writer->Close());
    ASSERT_EQ(writer->RecordCount(), 5);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataFileMeta>> results, writer->GetResult());
    ASSERT_EQ(results.size(), 4);
    ASSERT_EQ(results[0]->file_name, "main0");
    ASSERT_EQ(results[0]->row_count, 5);
    for (int32_t i = 0; i < 3; ++i) {
        ASSERT_EQ(results[i + 1]->file_name, fmt::format("vector{}", i));
        ASSERT_EQ(results[i + 1]->row_count, i == 2 ? 1 : 2);
    }
    ASSERT_EQ(main_state_->paths.size(), 1);
    ASSERT_EQ(vector_state_->paths.size(), 3);
    writer->Abort();
    CheckFilesRemoved();
    ASSERT_NOK(writer->GetResult());
}

TEST_F(RollingVectorFileWriterTest, MainSizeTargetClosesCurrentVector) {
    auto writer = NewWriter(2, std::numeric_limits<int64_t>::max(), 1024);
    ASSERT_OK(WriteRows(writer.get(), R"([[1, [1, 2]], [2, null]])"));
    ASSERT_OK(WriteRows(writer.get(), R"([[3, null]])"));
    ASSERT_OK(writer->Close());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataFileMeta>> results, writer->GetResult());
    ASSERT_EQ(results.size(), 4);
    ASSERT_EQ(results[0]->file_name, "main0");
    ASSERT_EQ(results[1]->file_name, "vector0");
    ASSERT_EQ(results[0]->row_count, 2);
    ASSERT_EQ(results[1]->row_count, 2);
    ASSERT_EQ(results[2]->file_name, "main1");
    ASSERT_EQ(results[3]->file_name, "vector1");
    ASSERT_EQ(results[2]->row_count, 1);
    ASSERT_EQ(results[3]->row_count, 1);
}

TEST_F(RollingVectorFileWriterTest, MainRowLimitPreservesVectorOrderAcrossGroups) {
    auto writer = NewWriter(1024, 3, 2);
    for (int32_t i = 0; i < 7; ++i) {
        ASSERT_OK(WriteRows(writer.get(), R"([[1, null]])"));
    }
    ASSERT_OK(writer->Close());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataFileMeta>> results, writer->GetResult());
    const std::vector<std::string> names = {"main0",   "vector0", "vector1", "main1",
                                            "vector2", "vector3", "main2",   "vector4"};
    const std::vector<int64_t> counts = {3, 2, 1, 3, 2, 1, 1, 1};
    ASSERT_EQ(results.size(), names.size());
    for (size_t i = 0; i < results.size(); ++i) {
        ASSERT_EQ(results[i]->file_name, names[i]);
        ASSERT_EQ(results[i]->row_count, counts[i]);
    }
}

TEST_F(RollingVectorFileWriterTest, EmptyBatchesDoNotCreateFiles) {
    auto writer = NewWriter();
    ASSERT_OK(WriteRows(writer.get(), "[]"));
    ASSERT_OK(writer->Close());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataFileMeta>> results, writer->GetResult());
    ASSERT_TRUE(results.empty());
    ASSERT_TRUE(main_state_->paths.empty());
    ASSERT_TRUE(vector_state_->paths.empty());
}

TEST_F(RollingVectorFileWriterTest, ExactVectorBoundaryDoesNotCreateEmptyTail) {
    auto writer = NewWriter();
    ASSERT_OK(WriteRows(writer.get(), R"([[1, null]])"));
    ASSERT_OK(WriteRows(writer.get(), "[]"));
    ASSERT_OK(writer->Close());
    ASSERT_EQ(main_state_->paths.size(), 1);
    ASSERT_EQ(vector_state_->paths.size(), 1);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataFileMeta>> results, writer->GetResult());
    ASSERT_EQ(results.size(), 2);
    ASSERT_EQ(results[0]->row_count, 1);
    ASSERT_EQ(results[1]->row_count, 1);
}

TEST_F(RollingVectorFileWriterTest, AbortRemovesPendingVectorFiles) {
    auto writer = NewWriter();
    ASSERT_OK(WriteRows(writer.get(), R"([[1, null]])"));
    ASSERT_OK(WriteRows(writer.get(), R"([[2, [1, 2]]])"));
    writer->Abort();
    CheckFilesRemoved();
    ASSERT_NOK(writer->Close());
    ASSERT_NOK(writer->GetResult());
}

TEST_F(RollingVectorFileWriterTest, RejectsMismatchedTotalRows) {
    auto writer = NewWriter();
    vector_state_->row_count_delta = 1;
    ASSERT_OK(WriteRows(writer.get(), R"([[1, null]])"));
    ASSERT_NOK_WITH_MSG(writer->Close(), "same total number of rows");
    CheckFilesRemoved();
    ASSERT_NOK(writer->GetResult());
}

class RollingVectorFailureTest : public RollingVectorFileWriterTest,
                                 public ::testing::WithParamInterface<std::tuple<bool, int32_t>> {};

TEST_P(RollingVectorFailureTest, RemovesEarlierGroupsAndPendingVectors) {
    const auto [main_failure, failure] = GetParam();
    auto writer = NewWriter(2);
    ASSERT_OK(WriteRows(writer.get(), R"([[1, null]])"));
    ASSERT_OK(WriteRows(writer.get(), R"([[2, [1, 2]]])"));
    // Keep a main file open with an already closed VECTOR file, except when testing main create.
    if (!main_failure || failure != 0) {
        ASSERT_OK(WriteRows(writer.get(), R"([[3, null]])"));
    }
    const auto& state = main_failure ? main_state_ : vector_state_;
    state->fail_create = failure == 0;
    state->fail_write = failure == 1;
    state->fail_close = failure == 2;
    state->fail_size = failure == 3;
    state->fail_result = failure == 4;
    if (main_failure && (failure == 2 || failure == 4)) {
        ASSERT_NOK(writer->Close());
    } else {
        ASSERT_NOK(WriteRows(writer.get(), R"([[4, [3, 4]]])"));
    }
    ASSERT_NOK(writer->GetResult());
    ASSERT_NOK(writer->Close());
    CheckFilesRemoved();
}

INSTANTIATE_TEST_SUITE_P(MainOrVector, RollingVectorFailureTest,
                         ::testing::Combine(::testing::Bool(), ::testing::Range(0, 5)));

}  // namespace paimon::test

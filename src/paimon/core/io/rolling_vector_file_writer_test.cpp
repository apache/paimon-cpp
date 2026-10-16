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
        return rows_ >= target_size;
    }

    Result<std::shared_ptr<DataFileMeta>> GetResult() override {
        return DataFileMeta::ForAppend(
            PathUtil::GetName(path_), 0, rows_, SimpleStats::EmptyStats(), 0, rows_ - 1, 0,
            FileSource::Append(), std::nullopt, std::nullopt, std::nullopt, std::nullopt);
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

TEST(RollingVectorFileWriterTest, VectorTargetRollsBothFiles) {
    auto dir = UniqueTestDirectory::Create("local");
    ASSERT_NE(dir, nullptr);
    std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
    auto main_state = std::make_shared<WriterState>();
    auto vector_state = std::make_shared<WriterState>();
    auto main_factory = std::make_shared<TestVectorWriterFactory>(
        fs, PathUtil::JoinPath(dir->Str(), "main"), main_state);
    auto vector_factory = std::make_shared<TestVectorWriterFactory>(
        fs, PathUtil::JoinPath(dir->Str(), "vector"), vector_state);
    auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                 arrow::field("v", arrow::fixed_size_list(arrow::float32(), 2))});
    RollingVectorFileWriter writer(1024, std::numeric_limits<int64_t>::max(), 1, main_factory,
                                   vector_factory, schema, 1);
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()),
                                                           R"([[1, [1, 2]], [2, null]])")
                     .ValueOrDie();
    for (int32_t i = 0; i < 2; ++i) {
        ::ArrowArray batch = {};
        ASSERT_TRUE(arrow::ExportArray(*array, &batch).ok());
        ASSERT_OK(writer.Write(&batch));
    }
    ASSERT_OK(writer.Close());
    ASSERT_OK(writer.Close());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataFileMeta>> results, writer.GetResult());
    ASSERT_EQ(results.size(), 4);
    for (const auto& file : results) {
        ASSERT_EQ(file->row_count, 2);
    }
    writer.Abort();
    for (const auto& state : {main_state, vector_state}) {
        ASSERT_EQ(state->paths.size(), 2);
        for (const auto& path : state->paths) {
            ASSERT_OK_AND_ASSIGN(bool exists, fs->Exists(path));
            ASSERT_FALSE(exists);
        }
    }
    ASSERT_NOK(writer.GetResult());
}

TEST(RollingVectorFileWriterTest, FailureRemovesBothFilesAndEarlierPairs) {
    for (int32_t failure = 0; failure < 6; ++failure) {
        SCOPED_TRACE(failure);
        auto dir = UniqueTestDirectory::Create("local");
        ASSERT_NE(dir, nullptr);
        std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
        auto main_state = std::make_shared<WriterState>();
        auto vector_state = std::make_shared<WriterState>();
        auto main_factory = std::make_shared<TestVectorWriterFactory>(
            fs, PathUtil::JoinPath(dir->Str(), "main"), main_state);
        auto vector_factory = std::make_shared<TestVectorWriterFactory>(
            fs, PathUtil::JoinPath(dir->Str(), "vector"), vector_state);
        auto schema =
            arrow::schema({arrow::field("id", arrow::int32()),
                           arrow::field("v", arrow::fixed_size_list(arrow::float32(), 2))});
        RollingVectorFileWriter writer(1024, 1, 1024, main_factory, vector_factory, schema, 1);
        auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()),
                                                               R"([[1, [1, 2]]])")
                         .ValueOrDie();
        ::ArrowArray batch = {};
        ASSERT_TRUE(arrow::ExportArray(*array, &batch).ok());
        ASSERT_OK(writer.Write(&batch));
        const auto& failing_state = failure < 3 ? main_state : vector_state;
        failing_state->fail_create = failure % 3 == 0;
        failing_state->fail_write = failure % 3 == 1;
        failing_state->fail_close = failure % 3 == 2;
        ASSERT_TRUE(arrow::ExportArray(*array, &batch).ok());
        ASSERT_NOK(writer.Write(&batch));
        ASSERT_NOK(writer.Close());
        ASSERT_NOK(writer.GetResult());
        for (const auto& state : {main_state, vector_state}) {
            for (const auto& path : state->paths) {
                ASSERT_OK_AND_ASSIGN(bool exists, fs->Exists(path));
                ASSERT_FALSE(exists);
            }
        }
    }
}

}  // namespace paimon::test

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

#include "paimon/testing/utils/blocking_file_system.h"

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(BlockingFileSystemTest, TestOpenFiltersPathAndBlocksOnlyOnce) {
    auto fs = std::make_shared<BlockingFileSystem>(std::make_shared<MockFileSystem>());
    auto gate = fs->BlockNext(BlockingFileSystem::Operation::OPEN, "blocked");
    std::future<Result<std::unique_ptr<InputStream>>> opening;
    ScopeGuard finish([&]() {
        gate->Release();
        if (opening.valid()) {
            opening.wait();
        }
    });
    opening = std::async(std::launch::async, [&]() { return fs->Open(FileStatus("blocked", 0)); });
    ASSERT_TRUE(gate->WaitForArrival());
    ASSERT_EQ("blocked", gate->MatchedPath());
    ASSERT_EQ(std::future_status::timeout, opening.wait_for(std::chrono::milliseconds(20)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> other, fs->Open("other"));
    ASSERT_OK(other->Close());
    gate->Release();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> stream, opening.get());
    ASSERT_OK(stream->Close());
    ASSERT_OK_AND_ASSIGN(stream, fs->Open("blocked"));
    ASSERT_OK(stream->Close());
}

TEST(BlockingFileSystemTest, TestReleaseInjectsOneShotErrorAndGateCanBeRearmed) {
    auto fs = std::make_shared<BlockingFileSystem>(std::make_shared<MockFileSystem>());
    auto first = fs->BlockNext(BlockingFileSystem::Operation::OPEN, "file");
    first->Release(Status::IOError("first open failed"));
    ASSERT_NOK_WITH_MSG(fs->Open("file"), "first open failed");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> stream, fs->Open("file"));
    ASSERT_OK(stream->Close());
    auto second = fs->BlockNext(BlockingFileSystem::Operation::OPEN, "file");
    second->Release(Status::IOError("second open failed"));
    ASSERT_NOK_WITH_MSG(fs->Open(FileStatus("file", 0)), "second open failed");
    ASSERT_OK_AND_ASSIGN(stream, fs->Open("file"));
    ASSERT_OK(stream->Close());
}

TEST(BlockingFileSystemTest, TestCreateAndCloseKeepControlsAlive) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_NE(nullptr, directory);
    const std::string path = PathUtil::JoinPath(directory->Str(), "output");
    auto fs = std::make_shared<BlockingFileSystem>(directory->GetFileSystem());
    auto create_gate = fs->BlockNext(BlockingFileSystem::Operation::CREATE, /*path=*/"");
    auto close_gate = fs->BlockNext(BlockingFileSystem::Operation::CLOSE, path);
    std::unique_ptr<OutputStream> stream;
    std::future<Result<std::unique_ptr<OutputStream>>> creating;
    std::future<Status> closing;
    ScopeGuard finish([&]() {
        create_gate->Release();
        close_gate->Release();
        if (creating.valid()) {
            creating.wait();
        }
        if (closing.valid()) {
            closing.wait();
        }
    });
    creating =
        std::async(std::launch::async, [&]() { return fs->Create(path, /*overwrite=*/false); });
    ASSERT_TRUE(create_gate->WaitForArrival());
    ASSERT_EQ(path, create_gate->MatchedPath());
    ASSERT_OK_AND_ASSIGN(bool exists, fs->Exists(path));
    ASSERT_FALSE(exists);
    create_gate->Release();
    ASSERT_OK_AND_ASSIGN(stream, creating.get());
    ASSERT_OK_AND_ASSIGN(exists, fs->Exists(path));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(int64_t written, stream->Write("data", 4));
    ASSERT_EQ(4, written);
    ASSERT_OK(stream->Flush());
    ASSERT_OK_AND_ASSIGN(int64_t position, stream->GetPos());
    ASSERT_EQ(4, position);
    closing = std::async(std::launch::async, [&]() { return stream->Close(); });
    ASSERT_TRUE(close_gate->WaitForArrival());
    fs.reset();
    close_gate->Release(Status::IOError("injected close failure"));
    ASSERT_NOK_WITH_MSG(closing.get(), "injected close failure");
    std::string content;
    ASSERT_OK(directory->GetFileSystem()->ReadFile(path, &content));
    ASSERT_EQ("data", content);
}

TEST(BlockingFileSystemTest, TestBarrierRequiresConcurrentArrivals) {
    auto fs = std::make_shared<BlockingFileSystem>(std::make_shared<MockFileSystem>());
    ASSERT_NOK_WITH_MSG(fs->Barrier(BlockingFileSystem::Operation::OPEN, "", 0),
                        "requires positive arrivals");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlockingFileSystem::Gate> barrier,
                         fs->Barrier(BlockingFileSystem::Operation::OPEN, "", 3));
    std::vector<std::future<Result<std::unique_ptr<InputStream>>>> openings;
    ScopeGuard finish([&]() {
        barrier->Release();
        for (auto& opening : openings) {
            if (opening.valid()) {
                opening.wait();
            }
        }
    });
    openings.push_back(std::async(std::launch::async, [&]() { return fs->Open("first"); }));
    ASSERT_TRUE(barrier->WaitForArrival());
    ASSERT_EQ(std::future_status::timeout, openings[0].wait_for(std::chrono::milliseconds(20)));
    for (int32_t i = 0; i < 2; ++i) {
        openings.push_back(std::async(std::launch::async, [&]() { return fs->Open("other"); }));
    }
    for (auto& opening : openings) {
        ASSERT_EQ(std::future_status::ready, opening.wait_for(std::chrono::seconds(5)));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> stream, opening.get());
        ASSERT_OK(stream->Close());
    }
}

}  // namespace paimon::test

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
#include "paimon/core/deletionvectors/deletion_vector_index_file_writer.h"

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/core/deletionvectors/bitmap64_deletion_vector.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/mock/mock_index_path_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

class FailingDeletionVector : public DeletionVector {
 public:
    Status Delete(int64_t) override {
        return Status::Invalid("injected delete failure");
    }

    Result<bool> CheckedDelete(int64_t) override {
        return Status::Invalid("injected checked-delete failure");
    }

    Result<bool> IsDeleted(int64_t) const override {
        return Status::Invalid("injected is-deleted failure");
    }

    bool IsEmpty() const override {
        return true;
    }

    Result<int64_t> GetCardinality() const override {
        return 0;
    }

    Result<int32_t> SerializeTo(const std::shared_ptr<MemoryPool>&, DataOutputStream*) override {
        return Status::Invalid("injected serialize failure");
    }

    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeToBytes(const std::shared_ptr<MemoryPool>&) override {
        return Status::Invalid("injected serialize failure");
    }

    Status Merge(const std::shared_ptr<DeletionVector>&) override {
        return Status::Invalid("injected merge failure");
    }

    Status ForEachDeletedPosition(const std::function<Status(int64_t)>&) const override {
        return Status::Invalid("injected for-each-deleted-position failure");
    }
};

}  // namespace

TEST(DeletionVectorIndexFileWriterTest, WriteSingleFileRoundTrip) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    DeletionVectorIndexFileWriter writer(fs, path_factory, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    RoaringBitmap32 roaring_1;
    roaring_1.Add(1);
    roaring_1.Add(2);
    input["data-a"] = std::make_shared<BitmapDeletionVector>(roaring_1);

    RoaringBitmap32 roaring_2;
    roaring_2.Add(10);
    input["data-b"] = std::make_shared<BitmapDeletionVector>(roaring_2);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> meta, writer.WriteSingleFile(input));
    ASSERT_EQ(meta->IndexType(), DeletionVectorsIndexFile::DELETION_VECTORS_INDEX);
    ASSERT_EQ(meta->FileName(), "index-0");
    ASSERT_EQ(meta->RowCount(), 2);

    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);
    ASSERT_OK_AND_ASSIGN(auto read_back, index_file.ReadAllDeletionVectors(meta));
    ASSERT_EQ(read_back.size(), 2);

    ASSERT_OK_AND_ASSIGN(bool is_deleted, read_back.at("data-a")->IsDeleted(1));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("data-a")->IsDeleted(3));
    ASSERT_FALSE(is_deleted);

    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("data-b")->IsDeleted(10));
    ASSERT_TRUE(is_deleted);
}

TEST(DeletionVectorIndexFileWriterTest, WriteWithRollingSplitsWhenTargetSizeIsExceeded) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorIndexFileWriter writer(fs, path_factory, pool);
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);

    // Four one-position vectors, with a target small enough that every one of them overruns it.
    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    for (int32_t i = 0; i < 4; i++) {
        RoaringBitmap32 roaring;
        roaring.Add(i);
        input[fmt::format("data-{}", i)] = std::make_shared<BitmapDeletionVector>(roaring);
    }
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<IndexFileMeta>> metas,
                         writer.WriteWithRolling(input, /*target_size=*/1));
    ASSERT_EQ(metas.size(), 4);

    // Every rolled file has to be a complete, readable index file on its own, holding exactly
    // the vector it was given.
    for (const std::shared_ptr<IndexFileMeta>& meta : metas) {
        ASSERT_EQ(meta->IndexType(), DeletionVectorsIndexFile::DELETION_VECTORS_INDEX);
        ASSERT_EQ(meta->RowCount(), 1);
        std::map<std::string, std::shared_ptr<DeletionVector>> read_back;
        ASSERT_OK_AND_ASSIGN(read_back, index_file.ReadAllDeletionVectors(meta));
        ASSERT_EQ(read_back.size(), 1);
        const std::string& data_file = read_back.begin()->first;
        ASSERT_EQ(input.count(data_file), 1U);
        ASSERT_OK_AND_ASSIGN(int64_t expected, input.at(data_file)->GetCardinality());
        ASSERT_OK_AND_ASSIGN(int64_t actual, read_back.begin()->second->GetCardinality());
        ASSERT_EQ(actual, expected);
    }

    // The file names differ, so nothing was overwritten.
    std::set<std::string> file_names;
    for (const std::shared_ptr<IndexFileMeta>& meta : metas) {
        file_names.insert(meta->FileName());
    }
    ASSERT_EQ(file_names.size(), 4U);
}

TEST(DeletionVectorIndexFileWriterTest, WriteWithRollingKeepsAFileThatReachesTheTargetExactly) {
    // The boundary itself: rolling happens on `written > target`, so a written size landing
    // exactly on the target must not roll. One byte less must.
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorIndexFileWriter writer(fs, path_factory, pool);

    auto make_input = []() {
        std::map<std::string, std::shared_ptr<DeletionVector>> input;
        RoaringBitmap32 roaring_a;
        roaring_a.Add(1);
        input["data-a"] = std::make_shared<BitmapDeletionVector>(roaring_a);
        RoaringBitmap32 roaring_b;
        roaring_b.Add(2);
        input["data-b"] = std::make_shared<BitmapDeletionVector>(roaring_b);
        return input;
    };

    // The writer's position right after the first vector equals the size of a file holding only
    // that vector, so measuring one gives the exact boundary to aim at.
    std::map<std::string, std::shared_ptr<DeletionVector>> first_only;
    first_only["data-a"] = make_input().at("data-a");
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<IndexFileMeta>> single,
                         writer.WriteWithRolling(first_only, /*target_size=*/1024 * 1024));
    ASSERT_EQ(single.size(), 1);
    int64_t after_first_write = single[0]->FileSize();
    ASSERT_GT(after_first_write, 1);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<IndexFileMeta>> at_boundary,
                         writer.WriteWithRolling(make_input(), after_first_write));
    ASSERT_EQ(at_boundary.size(), 1);
    ASSERT_EQ(at_boundary[0]->RowCount(), 2);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<IndexFileMeta>> past_boundary,
                         writer.WriteWithRolling(make_input(), after_first_write - 1));
    ASSERT_EQ(past_boundary.size(), 2);
}

TEST(DeletionVectorIndexFileWriterTest, WriteWithRollingKeepsOneFileBelowTheTargetSize) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorIndexFileWriter writer(fs, path_factory, pool);
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    RoaringBitmap32 roaring_a;
    roaring_a.Add(1);
    input["data-a"] = std::make_shared<BitmapDeletionVector>(roaring_a);
    // A 64 bit vector alongside a 32 bit one: the two frame their records differently, and both
    // have to survive the same rolled file.
    auto dv64 = std::make_shared<Bitmap64DeletionVector>();
    ASSERT_OK(dv64->Delete(1L << 33));
    input["data-b"] = dv64;

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<IndexFileMeta>> metas,
                         writer.WriteWithRolling(input, /*target_size=*/1024 * 1024));
    ASSERT_EQ(metas.size(), 1);
    ASSERT_EQ(metas[0]->RowCount(), 2);
    std::map<std::string, std::shared_ptr<DeletionVector>> read_back;
    ASSERT_OK_AND_ASSIGN(read_back, index_file.ReadAllDeletionVectors(metas[0]));
    ASSERT_EQ(read_back.size(), 2);
    ASSERT_OK_AND_ASSIGN(bool is_deleted, read_back.at("data-a")->IsDeleted(1));
    ASSERT_TRUE(is_deleted);
    ASSERT_TRUE(dynamic_cast<Bitmap64DeletionVector*>(read_back.at("data-b").get()) != nullptr);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("data-b")->IsDeleted(1L << 33));
    ASSERT_TRUE(is_deleted);
}

TEST(DeletionVectorIndexFileWriterTest, WriteWithRollingKeepsAnOversizedVectorInItsOwnFile) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorIndexFileWriter writer(fs, path_factory, pool);
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);

    // One vector on its own already exceeds the target. Rolling happens after the write, so it
    // still lands in a file rather than failing.
    RoaringBitmap32 roaring;
    for (int32_t position = 0; position < 5000; position += 2) {
        roaring.Add(position);
    }
    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    input["data-big"] = std::make_shared<BitmapDeletionVector>(roaring);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<IndexFileMeta>> metas,
                         writer.WriteWithRolling(input, /*target_size=*/8));
    ASSERT_EQ(metas.size(), 1);
    std::map<std::string, std::shared_ptr<DeletionVector>> read_back;
    ASSERT_OK_AND_ASSIGN(read_back, index_file.ReadAllDeletionVectors(metas[0]));
    ASSERT_EQ(read_back.size(), 1);
    ASSERT_OK_AND_ASSIGN(int64_t cardinality, read_back.at("data-big")->GetCardinality());
    ASSERT_EQ(cardinality, 2500);
}

TEST(DeletionVectorIndexFileWriterTest, WriteWithRollingRejectsANonPositiveTargetSize) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorIndexFileWriter writer(fs, path_factory, pool);

    RoaringBitmap32 roaring;
    roaring.Add(1);
    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    input["data-a"] = std::make_shared<BitmapDeletionVector>(roaring);

    ASSERT_NOK_WITH_MSG(writer.WriteWithRolling(input, /*target_size=*/0),
                        "target size must be positive");
    ASSERT_NOK_WITH_MSG(writer.WriteWithRolling(input, /*target_size=*/-1),
                        "target size must be positive");

    // An empty input writes nothing, whatever the target is.
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<IndexFileMeta>> metas,
                         writer.WriteWithRolling({}, /*target_size=*/1024));
    ASSERT_TRUE(metas.empty());
}

TEST(DeletionVectorIndexFileWriterTest, WriteWithRollingShouldReturnSerializeError) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorIndexFileWriter writer(fs, path_factory, pool);

    // The failing vector comes second, so a file is already open when the write fails: the
    // writer has to close it instead of leaking its output stream.
    RoaringBitmap32 roaring;
    roaring.Add(1);
    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    input["data-a"] = std::make_shared<BitmapDeletionVector>(roaring);
    input["data-b"] = std::make_shared<FailingDeletionVector>();

    ASSERT_NOK_WITH_MSG(writer.WriteWithRolling(input, /*target_size=*/1024 * 1024),
                        "injected serialize failure");
}

TEST(DeletionVectorIndexFileWriterTest, WriteSingleFileShouldReturnSerializeError) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    DeletionVectorIndexFileWriter writer(fs, path_factory, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    input["bad"] = std::make_shared<FailingDeletionVector>();

    ASSERT_NOK_WITH_MSG(writer.WriteSingleFile(input), "injected serialize failure");
}

}  // namespace paimon::test

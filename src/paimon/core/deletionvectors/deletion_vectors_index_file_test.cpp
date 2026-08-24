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
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/mock/mock_index_path_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(DeletionVectorsIndexFileTest, Basic) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    auto index_file =
        std::make_shared<DeletionVectorsIndexFile>(fs, path_factory, /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    RoaringBitmap32 roaring_1;
    for (int32_t i = 0; i < 10; ++i) {
        roaring_1.Add(i);
    }
    input["dv1"] = std::make_shared<BitmapDeletionVector>(roaring_1);
    RoaringBitmap32 roaring_2;
    for (int32_t i = 100; i < 110; ++i) {
        roaring_2.Add(i);
    }
    input["dv2"] = std::make_shared<BitmapDeletionVector>(roaring_2);

    ASSERT_FALSE(index_file->Bitmap64());
    ASSERT_OK_AND_ASSIGN(auto meta, index_file->WriteSingleFile(input));
    ASSERT_GT(meta->FileSize(), 0);
    ASSERT_OK_AND_ASSIGN(auto size, index_file->FileSize(meta));
    ASSERT_EQ(meta->FileSize(), size);
    ASSERT_EQ(meta->IndexType(), DeletionVectorsIndexFile::DELETION_VECTORS_INDEX);
    ASSERT_EQ(meta->FileName(), "index-0");
    ASSERT_FALSE(index_file->IsExternalPath());
    ASSERT_EQ(meta->ExternalPath(), std::nullopt);

    // Round trip: write then read all deletion vectors from index file.
    ASSERT_OK_AND_ASSIGN(auto read_back, index_file->ReadAllDeletionVectors(meta));
    ASSERT_EQ(read_back.size(), input.size());
    ASSERT_EQ(read_back.at("dv1")->GetCardinality().value(), 10);
    ASSERT_EQ(read_back.at("dv2")->GetCardinality().value(), 10);

    ASSERT_OK_AND_ASSIGN(bool is_deleted, read_back.at("dv1")->IsDeleted(0));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("dv1")->IsDeleted(10));
    ASSERT_FALSE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("dv2")->IsDeleted(100));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("dv2")->IsDeleted(99));
    ASSERT_FALSE(is_deleted);
}

TEST(DeletionVectorsIndexFileTest, ExternalPathAndIndexFileMeta) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    path_factory->SetExternal(true);
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(fs, path_factory,
                                        /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    RoaringBitmap32 roaring;
    for (int32_t i = 0; i < 5; ++i) {
        roaring.Add(i);
    }
    input["dv_ext"] = std::make_shared<BitmapDeletionVector>(roaring);

    ASSERT_OK_AND_ASSIGN(auto meta, index_file.WriteSingleFile(input));
    ASSERT_EQ(meta->ExternalPath().value(), PathUtil::JoinPath(dir->Str(), "index-0"));

    // Round trip for external path index file.
    ASSERT_OK_AND_ASSIGN(auto read_back, index_file.ReadAllDeletionVectors(meta));
    ASSERT_EQ(read_back.size(), 1);
    ASSERT_EQ(read_back.at("dv_ext")->GetCardinality().value(), 5);
    ASSERT_OK_AND_ASSIGN(bool is_deleted, read_back.at("dv_ext")->IsDeleted(0));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("dv_ext")->IsDeleted(5));
    ASSERT_FALSE(is_deleted);
}

TEST(DeletionVectorsIndexFileTest, RoundTripEmptyInput) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    ASSERT_OK_AND_ASSIGN(auto meta, index_file.WriteSingleFile(input));
    ASSERT_EQ(meta->RowCount(), 0);
    ASSERT_OK_AND_ASSIGN(auto read_back, index_file.ReadAllDeletionVectors(meta));
    ASSERT_TRUE(read_back.empty());
}

TEST(DeletionVectorsIndexFileTest, RoundTripMultipleIndexFilesMerge) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input1;
    RoaringBitmap32 roaring_1;
    roaring_1.Add(1);
    roaring_1.Add(3);
    input1["dv_a"] = std::make_shared<BitmapDeletionVector>(roaring_1);
    ASSERT_OK_AND_ASSIGN(auto meta1, index_file.WriteSingleFile(input1));

    std::map<std::string, std::shared_ptr<DeletionVector>> input2;
    RoaringBitmap32 roaring_2;
    roaring_2.Add(8);
    input2["dv_b"] = std::make_shared<BitmapDeletionVector>(roaring_2);
    ASSERT_OK_AND_ASSIGN(auto meta2, index_file.WriteSingleFile(input2));

    ASSERT_OK_AND_ASSIGN(auto read_back,
                         index_file.ReadAllDeletionVectors(
                             std::vector<std::shared_ptr<IndexFileMeta>>{meta1, meta2}));
    ASSERT_EQ(read_back.size(), 2);

    ASSERT_OK_AND_ASSIGN(bool is_deleted, read_back.at("dv_a")->IsDeleted(1));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("dv_b")->IsDeleted(8));
    ASSERT_TRUE(is_deleted);
}

TEST(DeletionVectorsIndexFileTest, ReadDeletionVectorsSelectsASubsetOfOneIndexFile) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    for (int32_t i = 0; i < 4; i++) {
        RoaringBitmap32 roaring;
        roaring.Add(i);
        input[fmt::format("data-{}", i)] = std::make_shared<BitmapDeletionVector>(roaring);
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> meta, index_file.WriteSingleFile(input));
    const std::optional<LinkedHashMap<std::string, DeletionVectorMeta>>& dv_ranges =
        meta->DvRanges();
    ASSERT_TRUE(dv_ranges.has_value());
    std::string index_file_path = path_factory->ToPath(meta);

    // Two of the four vectors, addressed by their recorded positions. The two that are not asked
    // for must not be returned, which is what lets a rewrite keep an untouched vector's bytes.
    std::map<std::string, DeletionFile> selected;
    for (const std::string& data_file : {std::string("data-1"), std::string("data-3")}) {
        auto dv_meta = dv_ranges.value().find(data_file);
        ASSERT_TRUE(dv_meta != dv_ranges.value().end());
        selected.emplace(
            data_file, DeletionFile(index_file_path, dv_meta->second.GetOffset(),
                                    dv_meta->second.GetLength(), dv_meta->second.GetCardinality()));
    }
    std::map<std::string, std::shared_ptr<DeletionVector>> read_back;
    ASSERT_OK_AND_ASSIGN(read_back, index_file.ReadDeletionVectors(selected));
    ASSERT_EQ(read_back.size(), 2);
    ASSERT_OK_AND_ASSIGN(bool is_deleted, read_back.at("data-1")->IsDeleted(1));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("data-3")->IsDeleted(3));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, read_back.at("data-3")->IsDeleted(1));
    ASSERT_FALSE(is_deleted);

    // The single-vector overload reads the same bytes.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DeletionVector> single,
                         index_file.ReadDeletionVector(selected.at("data-1")));
    ASSERT_OK_AND_ASSIGN(is_deleted, single->IsDeleted(1));
    ASSERT_TRUE(is_deleted);

    ASSERT_OK_AND_ASSIGN(read_back, index_file.ReadDeletionVectors({}));
    ASSERT_TRUE(read_back.empty());
}

TEST(DeletionVectorsIndexFileTest, ReadDeletionVectorsRejectsMixedIndexFiles) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);

    RoaringBitmap32 roaring;
    roaring.Add(1);
    std::map<std::string, std::shared_ptr<DeletionVector>> input;
    input["data-a"] = std::make_shared<BitmapDeletionVector>(roaring);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> meta, index_file.WriteSingleFile(input));
    const std::optional<LinkedHashMap<std::string, DeletionVectorMeta>>& dv_ranges =
        meta->DvRanges();
    ASSERT_TRUE(dv_ranges.has_value());
    auto dv_meta_iter = dv_ranges.value().find("data-a");
    ASSERT_TRUE(dv_meta_iter != dv_ranges.value().end());
    const DeletionVectorMeta& dv_meta = dv_meta_iter->second;

    // A batch that spans two index files cannot be served by one stream, so it is a caller bug
    // rather than something to silently read from the wrong file.
    std::map<std::string, DeletionFile> mixed;
    mixed.emplace("data-a", DeletionFile(path_factory->ToPath(meta), dv_meta.GetOffset(),
                                         dv_meta.GetLength(), dv_meta.GetCardinality()));
    mixed.emplace("data-b", DeletionFile(path_factory->ToPath("other-index"), dv_meta.GetOffset(),
                                         dv_meta.GetLength(), dv_meta.GetCardinality()));
    ASSERT_NOK_WITH_MSG(index_file.ReadDeletionVectors(mixed), "live in different index files");
}

TEST(DeletionVectorsIndexFileTest, ReadDeletionVectorsChecksTheIndexFileVersion) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);

    // An index file whose leading version byte is not V1. Seeking straight to a recorded offset
    // would parse its payload as if it were V1; the read has to reject the file instead.
    std::string path = path_factory->ToPath("index-future-version");
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out, fs->Create(path,
                                                                           /*overwrite=*/true));
        DataOutputStream output_stream(out);
        ASSERT_OK(output_stream.WriteValue<int8_t>(DeletionVectorsIndexFile::VERSION_ID_V1 + 1));
        for (int32_t i = 0; i < 32; i++) {
            ASSERT_OK(output_stream.WriteValue<int8_t>(0));
        }
        ASSERT_OK(out->Close());
    }

    DeletionFile deletion_file(path, /*offset=*/1, /*length=*/16, /*cardinality=*/1);
    ASSERT_NOK_WITH_MSG(index_file.ReadDeletionVector(deletion_file), "Version not match");
    std::map<std::string, DeletionFile> batch;
    batch.emplace("data-a", deletion_file);
    ASSERT_NOK_WITH_MSG(index_file.ReadDeletionVectors(batch), "Version not match");
}

TEST(DeletionVectorsIndexFileTest, RoundTripMultipleIndexFilesLastWriteWinsOnSameKey) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();
    DeletionVectorsIndexFile index_file(fs, path_factory, /*bitmap64=*/false, pool);

    std::map<std::string, std::shared_ptr<DeletionVector>> input1;
    RoaringBitmap32 roaring_old;
    roaring_old.Add(2);
    input1["same_dv"] = std::make_shared<BitmapDeletionVector>(roaring_old);
    ASSERT_OK_AND_ASSIGN(auto meta1, index_file.WriteSingleFile(input1));

    std::map<std::string, std::shared_ptr<DeletionVector>> input2;
    RoaringBitmap32 roaring_new;
    roaring_new.Add(9);
    input2["same_dv"] = std::make_shared<BitmapDeletionVector>(roaring_new);
    ASSERT_OK_AND_ASSIGN(auto meta2, index_file.WriteSingleFile(input2));

    ASSERT_OK_AND_ASSIGN(auto read_back,
                         index_file.ReadAllDeletionVectors(
                             std::vector<std::shared_ptr<IndexFileMeta>>{meta1, meta2}));
    ASSERT_EQ(read_back.size(), 1);

    ASSERT_OK_AND_ASSIGN(bool is_deleted_old, read_back.at("same_dv")->IsDeleted(2));
    ASSERT_FALSE(is_deleted_old);
    ASSERT_OK_AND_ASSIGN(bool is_deleted_new, read_back.at("same_dv")->IsDeleted(9));
    ASSERT_TRUE(is_deleted_new);
}

}  // namespace paimon::test

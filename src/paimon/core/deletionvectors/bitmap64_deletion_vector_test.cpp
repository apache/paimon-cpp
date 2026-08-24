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
#include "paimon/core/deletionvectors/bitmap64_deletion_vector.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/utils/math.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(Bitmap64DeletionVectorTest, BasicOperations) {
    Bitmap64DeletionVector dv;
    ASSERT_TRUE(dv.IsEmpty());
    ASSERT_OK(dv.Delete(5));
    // A position beyond what RoaringBitmap32 can index, which is the reason this kind exists.
    ASSERT_OK(dv.Delete(1L << 33));
    ASSERT_FALSE(dv.IsEmpty());
    ASSERT_TRUE(dv.IsDeleted(5).value());
    ASSERT_TRUE(dv.IsDeleted(1L << 33).value());
    ASSERT_FALSE(dv.IsDeleted(6).value());
    ASSERT_EQ(dv.GetCardinality().value(), 2);

    ASSERT_TRUE(dv.CheckedDelete(7).value());
    ASSERT_FALSE(dv.CheckedDelete(7).value());

    ASSERT_NOK_WITH_MSG(dv.Delete(-1), "supports positions in");
    ASSERT_NOK_WITH_MSG(dv.Delete(Bitmap64DeletionVector::MAX_VALUE + 1), "supports positions in");
}

TEST(Bitmap64DeletionVectorTest, ForEachDeletedPositionVisitsPositionsInOrder) {
    Bitmap64DeletionVector dv;
    ASSERT_OK(dv.Delete(1L << 33));
    ASSERT_OK(dv.Delete(3));
    ASSERT_OK(dv.Delete(1L << 32));

    std::vector<int64_t> visited;
    ASSERT_OK(dv.ForEachDeletedPosition([&visited](int64_t position) -> Status {
        visited.push_back(position);
        return Status::OK();
    }));
    ASSERT_EQ(visited, (std::vector<int64_t>{3, 1L << 32, 1L << 33}));

    Bitmap64DeletionVector empty_dv;
    visited.clear();
    ASSERT_OK(empty_dv.ForEachDeletedPosition([&visited](int64_t position) -> Status {
        visited.push_back(position);
        return Status::OK();
    }));
    ASSERT_TRUE(visited.empty());
}

TEST(Bitmap64DeletionVectorTest, ForEachDeletedPositionStopsOnConsumerFailure) {
    // The rewriter re-keys a vector position by position, so a failure halfway through has to
    // surface instead of leaving the remaining positions silently applied.
    Bitmap64DeletionVector dv;
    ASSERT_OK(dv.Delete(1));
    ASSERT_OK(dv.Delete(2));
    ASSERT_OK(dv.Delete(1L << 33));

    int32_t calls = 0;
    Status status = dv.ForEachDeletedPosition([&calls](int64_t) -> Status {
        calls++;
        return Status::Invalid("stop here");
    });
    ASSERT_NOK_WITH_MSG(status, "stop here");
    ASSERT_EQ(calls, 1);
}

TEST(Bitmap64DeletionVectorTest, MergeRejectsTheOtherVectorKind) {
    auto dv64 = std::make_shared<Bitmap64DeletionVector>();
    ASSERT_OK(dv64->Delete(1L << 33));
    auto other64 = std::make_shared<Bitmap64DeletionVector>();
    ASSERT_OK(other64->Delete(4));
    ASSERT_OK(dv64->Merge(other64));
    ASSERT_EQ(dv64->GetCardinality().value(), 2);

    // The two kinds serialize differently, so mixing them has to fail rather than silently
    // dropping deletions.
    RoaringBitmap32 roaring;
    roaring.Add(9);
    auto dv32 = std::make_shared<BitmapDeletionVector>(roaring);
    ASSERT_NOK_WITH_MSG(dv64->Merge(dv32), "Cannot merge a non-Bitmap64DeletionVector");
    ASSERT_NOK_WITH_MSG(dv32->Merge(dv64), "Cannot merge a non-BitmapDeletionVector");

    // An empty vector of the other kind is a no-op rather than an error, matching bitmap32.
    auto empty32 = std::make_shared<BitmapDeletionVector>(RoaringBitmap32());
    ASSERT_OK(dv64->Merge(empty32));
}

TEST(Bitmap64DeletionVectorTest, SerializeToStoresTheMagicLittleEndian) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    std::string path = PathUtil::JoinPath(dir->Str(), "dv64");

    Bitmap64DeletionVector dv;
    ASSERT_OK(dv.Delete(1));
    ASSERT_OK(dv.Delete(1L << 33));

    int32_t serialized_length = 0;
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out, fs->Create(path, true));
        DataOutputStream data_out(out);
        ASSERT_OK_AND_ASSIGN(serialized_length, dv.SerializeTo(GetDefaultPool(), &data_out));
        ASSERT_OK(out->Close());
    }

    // A bitmap64 record records its whole framed length, unlike a bitmap32 one.
    ASSERT_OK_AND_ASSIGN(FileStatus status, fs->GetFileStatus(path));
    ASSERT_EQ(serialized_length, status.GetLen());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(path));
    auto data_in = std::make_shared<DataInputStream>(in);
    ASSERT_OK_AND_ASSIGN(int32_t bitmap_length, data_in->ReadValue<int32_t>());
    ASSERT_EQ(bitmap_length, serialized_length - Bitmap64DeletionVector::LENGTH_SIZE_BYTES -
                                 Bitmap64DeletionVector::CRC_SIZE_BYTES);
    // Read big-endian, the magic only matches after a swap: that asymmetry against
    // BitmapDeletionVector is what tells the two kinds apart on disk.
    ASSERT_OK_AND_ASSIGN(int32_t magic, data_in->ReadValue<int32_t>());
    ASSERT_NE(magic, Bitmap64DeletionVector::MAGIC_NUMBER);
    ASSERT_EQ(EndianSwapValue(magic), Bitmap64DeletionVector::MAGIC_NUMBER);
}

TEST(Bitmap64DeletionVectorTest, ReadsDenseBucketArrayCompatibleWithPaimonJava) {
    // A vector may be serialized as a *dense* bucket array: the bucket count is the length of
    // the bitmap array and every key from 0 up is written, empty buckets included. CRoaring
    // writes only the buckets in use, so the two forms differ in bytes for a vector spanning
    // several 32 bit key ranges even though both follow the portable format. Reading has to
    // accept the dense form, which is what this assembles by hand: keys 0 and 2 in use with an
    // empty key 1 between them. Note the bytes are built here rather than captured from a
    // writer that produces them, so this pins the documented layout rather than a golden
    // fixture; the reverse direction, another engine reading the sparse form CRoaring writes,
    // is not covered by any test here either.
    auto pool = GetDefaultPool();
    auto portable_bitmap = [&pool](const std::vector<int32_t>& positions) {
        RoaringBitmap32 roaring;
        for (int32_t position : positions) {
            roaring.Add(position);
        }
        return roaring.Serialize(pool.get());
    };
    PAIMON_UNIQUE_PTR<Bytes> bucket_0 = portable_bitmap({5});
    PAIMON_UNIQUE_PTR<Bytes> bucket_1 = portable_bitmap({});
    PAIMON_UNIQUE_PTR<Bytes> bucket_2 = portable_bitmap({7});

    // The bitmap payload is little-endian throughout, as the portable format demands.
    std::string payload;
    auto append_little_endian = [&payload](uint64_t value, size_t width) {
        for (size_t i = 0; i < width; i++) {
            payload.push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
        }
    };
    append_little_endian(3, 8);  // bucket count, including the empty one
    append_little_endian(0, 4);
    payload.append(bucket_0->data(), bucket_0->size());
    append_little_endian(1, 4);
    payload.append(bucket_1->data(), bucket_1->size());
    append_little_endian(2, 4);
    payload.append(bucket_2->data(), bucket_2->size());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    std::string path = PathUtil::JoinPath(dir->Str(), "dv64-java-dense");
    int32_t bitmap_data_length =
        Bitmap64DeletionVector::MAGIC_NUMBER_SIZE_BYTES + static_cast<int32_t>(payload.size());
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out, fs->Create(path,
                                                                           /*overwrite=*/true));
        DataOutputStream data_out(out);
        // Length big-endian, magic little-endian: the framing asymmetry a reader tells the two
        // vector kinds apart by.
        ASSERT_OK(data_out.WriteValue<int32_t>(bitmap_data_length));
        ASSERT_OK(
            data_out.WriteValue<int32_t>(EndianSwapValue(Bitmap64DeletionVector::MAGIC_NUMBER)));
        ASSERT_OK_AND_ASSIGN(int64_t written,
                             out->Write(payload.data(), static_cast<int64_t>(payload.size())));
        ASSERT_EQ(written, static_cast<int64_t>(payload.size()));
        // The checksum is not validated on read, so any value round-trips the framing.
        ASSERT_OK(data_out.WriteValue<int32_t>(0));
        ASSERT_OK(out->Close());
    }

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(path));
    auto data_in = std::make_shared<DataInputStream>(in);
    int64_t record_length = Bitmap64DeletionVector::LENGTH_SIZE_BYTES + bitmap_data_length +
                            Bitmap64DeletionVector::CRC_SIZE_BYTES;
    ASSERT_OK_AND_ASSIGN(PAIMON_UNIQUE_PTR<DeletionVector> read_back,
                         DeletionVector::Read(data_in.get(), record_length, pool.get()));
    ASSERT_TRUE(dynamic_cast<Bitmap64DeletionVector*>(read_back.get()) != nullptr);
    ASSERT_EQ(read_back->GetCardinality().value(), 2);
    ASSERT_TRUE(read_back->IsDeleted(5).value());
    // Key 2 with low bits 7, i.e. the empty middle bucket did not shift the keys.
    ASSERT_TRUE(read_back->IsDeleted((2LL << 32) | 7).value());
    ASSERT_FALSE(read_back->IsDeleted((1LL << 32) | 7).value());
}

TEST(Bitmap64DeletionVectorTest, ReadRoundTripThroughDeletionVectorRead) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    std::string path = PathUtil::JoinPath(dir->Str(), "dv64-roundtrip");

    Bitmap64DeletionVector dv;
    ASSERT_OK(dv.Delete(0));
    ASSERT_OK(dv.Delete(7));
    ASSERT_OK(dv.Delete(1L << 34));

    int32_t serialized_length = 0;
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out, fs->Create(path, true));
        DataOutputStream data_out(out);
        ASSERT_OK_AND_ASSIGN(serialized_length, dv.SerializeTo(GetDefaultPool(), &data_out));
        ASSERT_OK(out->Close());
    }

    // DeletionVector::Read dispatches on the magic and has to pick the bitmap64 branch.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(path));
    auto data_in = std::make_shared<DataInputStream>(in);
    ASSERT_OK_AND_ASSIGN(
        PAIMON_UNIQUE_PTR<DeletionVector> read_back,
        DeletionVector::Read(data_in.get(), serialized_length, GetDefaultPool().get()));
    ASSERT_TRUE(dynamic_cast<Bitmap64DeletionVector*>(read_back.get()) != nullptr);
    ASSERT_EQ(read_back->GetCardinality().value(), 3);
    ASSERT_TRUE(read_back->IsDeleted(0).value());
    ASSERT_TRUE(read_back->IsDeleted(7).value());
    ASSERT_TRUE(read_back->IsDeleted(1L << 34).value());
    ASSERT_FALSE(read_back->IsDeleted(1).value());
}

}  // namespace paimon::test

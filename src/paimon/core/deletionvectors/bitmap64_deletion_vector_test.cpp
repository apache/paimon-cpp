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

#include <set>

#include "gtest/gtest.h"
#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(Bitmap64DeletionVectorTest, TestBasicOperations) {
    Bitmap64DeletionVector dv;
    ASSERT_TRUE(dv.IsEmpty());
    std::vector<int64_t> expected;
    for (const auto& base : {0LL, 1LL << 31, 1LL << 32, 2LL << 32}) {
        for (int64_t i = 0; i < 2000; i += 2) {
            ASSERT_OK(dv.Delete(base + i));
            expected.push_back(base + i);
        }
        for (int64_t i = 0; i < 2000; ++i) {
            ASSERT_EQ(dv.IsDeleted(base + i).value(), i % 2 == 0);
        }
    }
    ASSERT_FALSE(dv.IsEmpty());
    ASSERT_EQ(dv.GetCardinality().value(), expected.size());
    std::vector<int64_t> actual;
    ASSERT_OK(dv.ForEachDeletedPosition([&](int64_t p) { actual.push_back(p); }));
    ASSERT_EQ(actual, expected);
}

TEST(Bitmap64DeletionVectorTest, TestCheckedDelete) {
    Bitmap64DeletionVector dv;
    std::vector<int64_t> positions = {
        0, 42, (1LL << 31) - 1, 1LL << 31, (1LL << 32) - 1, 1LL << 32, (2LL << 32) + 42};
    for (const auto& position : positions) {
        ASSERT_TRUE(dv.CheckedDelete(position).value());
        ASSERT_FALSE(dv.CheckedDelete(position).value());
        ASSERT_TRUE(dv.IsDeleted(position).value());
    }
    ASSERT_EQ(dv.GetCardinality().value(), positions.size());
}

TEST(Bitmap64DeletionVectorTest, TestGetCardinality) {
    for (const auto& step : {1, 10}) {
        Bitmap64DeletionVector dv;
        ASSERT_EQ(dv.GetCardinality().value(), 0);
        for (const auto& base : {0LL, 1LL << 32}) {
            for (int64_t i = 0; i < 100; ++i) {
                ASSERT_OK(dv.Delete(base + i * step));
            }
        }
        ASSERT_EQ(dv.GetCardinality().value(), 200);
        ASSERT_OK(dv.Delete(0));
        ASSERT_EQ(dv.GetCardinality().value(), 200);
        ASSERT_OK(dv.Delete((2LL << 32) + 1));
        ASSERT_EQ(dv.GetCardinality().value(), 201);
    }
}

TEST(Bitmap64DeletionVectorTest, TestFromBitmapDeletionVector) {
    for (const auto& positions :
         {std::vector<int32_t>{}, std::vector<int32_t>{0, 1, 3, RoaringBitmap32::MAX_VALUE}}) {
        RoaringBitmap32 bitmap;
        for (const auto& position : positions) {
            bitmap.Add(position);
        }
        BitmapDeletionVector dv32(bitmap);
        auto dv64 = Bitmap64DeletionVector::FromBitmapDeletionVector(dv32);
        ASSERT_EQ(dv64->IsEmpty(), positions.empty());
        ASSERT_EQ(dv64->GetCardinality().value(), positions.size());
        std::vector<int64_t> actual;
        ASSERT_OK(dv64->ForEachDeletedPosition([&](int64_t p) { actual.push_back(p); }));
        ASSERT_EQ(actual, std::vector<int64_t>(positions.begin(), positions.end()));
        ASSERT_OK(dv64->Delete(7));
        ASSERT_OK(dv64->Delete((1LL << 32) + 7));
        ASSERT_FALSE(dv32.IsDeleted(7).value());
        ASSERT_EQ(dv32.GetCardinality().value(), positions.size());
        ASSERT_OK(dv32.Delete(8));
        ASSERT_FALSE(dv64->IsDeleted(8).value());
    }
}

TEST(Bitmap64DeletionVectorTest, TestPositionOutOfRangeShouldFail) {
    Bitmap64DeletionVector dv;
    for (const auto& position : std::vector<int64_t>{-1, OptimizedRoaringBitmap64::kMaxValue + 1}) {
        ASSERT_NOK_WITH_MSG(dv.Delete(position),
                            "OptimizedRoaringBitmap64 supports positions that are >= 0 and <=");
        ASSERT_NOK_WITH_MSG(dv.CheckedDelete(position),
                            "OptimizedRoaringBitmap64 supports positions that are >= 0 and <=");
        ASSERT_NOK_WITH_MSG(dv.IsDeleted(position),
                            "OptimizedRoaringBitmap64 supports positions that are >= 0 and <=");
    }
    ASSERT_TRUE(dv.IsEmpty());
    ASSERT_EQ(dv.GetCardinality().value(), 0);
    ASSERT_FALSE(dv.IsDeleted(OptimizedRoaringBitmap64::kMaxValue).value());
}

TEST(Bitmap64DeletionVectorTest, TestMerge) {
    std::vector<int64_t> left = {1, 3, (1LL << 32) + 5};
    std::vector<int64_t> right = {2, (1LL << 32) + 5, (2LL << 32) + 4};
    for (const auto& left_empty : {false, true}) {
        for (const auto& right_empty : {false, true}) {
            auto dv = std::make_shared<Bitmap64DeletionVector>();
            auto other = std::make_shared<Bitmap64DeletionVector>();
            std::set<int64_t> expected;
            if (!left_empty) {
                for (const auto& p : left) {
                    ASSERT_OK(dv->Delete(p));
                    expected.insert(p);
                }
            }
            if (!right_empty) {
                for (const auto& p : right) {
                    ASSERT_OK(other->Delete(p));
                    expected.insert(p);
                }
            }
            ASSERT_OK(dv->Merge(other));
            ASSERT_NOK_WITH_MSG(dv->Merge(nullptr), "Cannot merge a non-Bitmap64DeletionVector");
            ASSERT_OK(dv->Merge(other));
            for (const auto& positions : {std::vector<int32_t>{}, std::vector<int32_t>{7}}) {
                RoaringBitmap32 bitmap;
                for (const auto& position : positions) {
                    bitmap.Add(position);
                }
                ASSERT_NOK_WITH_MSG(dv->Merge(std::make_shared<BitmapDeletionVector>(bitmap)),
                                    "Cannot merge a non-Bitmap64DeletionVector");
            }
            ASSERT_EQ(dv->IsEmpty(), expected.empty());
            ASSERT_EQ(dv->GetCardinality().value(), expected.size());
            std::vector<int64_t> actual;
            ASSERT_OK(dv->ForEachDeletedPosition([&](int64_t p) { actual.push_back(p); }));
            ASSERT_EQ(actual, std::vector<int64_t>(expected.begin(), expected.end()));
            for (const auto& p : expected) {
                ASSERT_TRUE(dv->IsDeleted(p).value());
            }
        }
    }
}

TEST(Bitmap64DeletionVectorTest, JavaCompatibleFramedRecord) {
    // Generated with Java Paimon Bitmap64DeletionVector.serializeTo(), including CRC.
    std::vector<uint8_t> java_bytes = {
        0,   0,   0, 110, 209, 211, 57, 100, 3,   0,   0, 0, 0,   0,   0,   0,   0,   0,   0, 0,
        58,  48,  0, 0,   4,   0,   0,  0,   0,   0,   1, 0, 255, 127, 0,   0,   0,   128, 0, 0,
        255, 255, 0, 0,   40,  0,   0,  0,   44,  0,   0, 0, 46,  0,   0,   0,   48,  0,   0, 0,
        0,   0,   1, 0,   255, 255, 0,  0,   255, 255, 1, 0, 0,   0,   58,  48,  0,   0,   1, 0,
        0,   0,   0, 0,   0,   0,   16, 0,   0,   0,   0, 0, 2,   0,   0,   0,   58,  48,  0, 0,
        1,   0,   0, 0,   0,   0,   0,  0,   16,  0,   0, 0, 7,   0,   206, 177, 171, 208};
    std::vector<int64_t> positions = {
        0, 1, 2147483647LL, 2147483648LL, 4294967295LL, 4294967296LL, 8589934599LL};
    auto pool = GetDefaultPool();
    for (const auto& with_length : {false, true}) {
        auto input = std::make_shared<ByteArrayInputStream>(
            reinterpret_cast<const char*>(java_bytes.data()), java_bytes.size());
        DataInputStream in(input);
        ASSERT_OK_AND_ASSIGN(
            auto decoded,
            DeletionVector::Read(
                &in, with_length ? std::make_optional<int64_t>(java_bytes.size()) : std::nullopt,
                pool.get()));
        ASSERT_TRUE(dynamic_cast<Bitmap64DeletionVector*>(decoded.get()));
        std::vector<int64_t> actual;
        ASSERT_OK(decoded->ForEachDeletedPosition([&](int64_t p) { actual.push_back(p); }));
        ASSERT_EQ(actual, positions);
        ASSERT_EQ(input->GetPos().value(), java_bytes.size());
    }
    Bitmap64DeletionVector dv;
    for (const auto& p : positions) {
        ASSERT_OK(dv.Delete(p));
    }
    auto output = std::make_shared<ByteArrayOutputStream>(
        std::make_unique<MemorySegmentOutputStream>(1024, pool));
    DataOutputStream out(output);
    ASSERT_OK_AND_ASSIGN(int32_t metadata_length, dv.SerializeTo(pool, &out));
    ASSERT_OK_AND_ASSIGN(auto bytes, output->Finish(pool.get()));
    ASSERT_EQ(metadata_length, java_bytes.size());
    ASSERT_EQ(std::vector<uint8_t>(bytes->data(), bytes->data() + bytes->size()), java_bytes);
}

TEST(Bitmap64DeletionVectorTest, SerializeAndDeserialize) {
    Bitmap64DeletionVector dv;
    std::vector<int64_t> positions = {1, 7, 1024, (1LL << 32) + 7, (2LL << 32) + 1};
    for (const auto& position : positions) {
        ASSERT_OK(dv.Delete(position));
    }
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(auto bytes, dv.SerializeToBytes(pool));
    ASSERT_OK_AND_ASSIGN(
        auto decoded,
        Bitmap64DeletionVector::DeserializeWithoutMagicNumber(
            bytes->data() + Bitmap64DeletionVector::MAGIC_NUMBER_SIZE_BYTES,
            bytes->size() - Bitmap64DeletionVector::MAGIC_NUMBER_SIZE_BYTES, pool.get()));
    ASSERT_EQ(decoded->GetCardinality().value(), positions.size());
    for (const auto& position : positions) {
        ASSERT_TRUE(decoded->IsDeleted(position).value());
    }
    ASSERT_FALSE(decoded->IsDeleted(8).value());
    ASSERT_FALSE(decoded->IsDeleted((1LL << 32) + 8).value());
}

TEST(Bitmap64DeletionVectorTest, SerializeToOutputStream) {
    Bitmap64DeletionVector dv;
    for (int64_t p = 0; p < 50; p += 2) {
        ASSERT_OK(dv.Delete((1LL << 32) + p));
    }
    auto pool = GetDefaultPool();
    auto dir = UniqueTestDirectory::Create();
    auto path = PathUtil::JoinPath(dir->Str(), "dv64");
    ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", path, {}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> output, fs->Create(path, false));
    DataOutputStream out(output);
    ASSERT_OK_AND_ASSIGN(int32_t length, dv.SerializeTo(pool, &out));
    ASSERT_OK(output->Flush());
    ASSERT_OK(output->Close());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, fs->Open(path));
    DataInputStream in(input);
    ASSERT_OK_AND_ASSIGN(int32_t bitmap_length, in.ReadValue<int32_t>());
    ASSERT_EQ(length, bitmap_length + Bitmap64DeletionVector::LENGTH_SIZE_BYTES +
                          Bitmap64DeletionVector::CRC_SIZE_BYTES);
    ASSERT_EQ(in.Length().value(), length);
    ASSERT_OK(in.Seek(0));
    ASSERT_OK_AND_ASSIGN(auto decoded, DeletionVector::Read(&in, length, pool.get()));
    ASSERT_EQ(in.GetPos().value(), length);
    ASSERT_EQ(decoded->GetCardinality().value(), 25);
    for (int64_t p = 0; p <= 50; ++p) {
        ASSERT_EQ(decoded->IsDeleted((1LL << 32) + p).value(), p < 50 && p % 2 == 0);
    }
}

TEST(Bitmap64DeletionVectorTest, RejectMalformedRecords) {
    auto pool = GetDefaultPool();
    Bitmap64DeletionVector dv;
    ASSERT_OK(dv.Delete(1LL << 32));
    auto output = std::make_shared<ByteArrayOutputStream>(
        std::make_unique<MemorySegmentOutputStream>(1024, pool));
    DataOutputStream out(output);
    ASSERT_OK_AND_ASSIGN(int32_t length, dv.SerializeTo(pool, &out));
    ASSERT_OK_AND_ASSIGN(auto bytes, output->Finish(pool.get()));
    auto input = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    DataInputStream in(input);
    ASSERT_NOK_WITH_MSG(DeletionVector::Read(&in, length - 8, pool.get()), "Size not match");
    for (const auto& cut : {0, 1, 3, 4, 7, length - 5, length - 1}) {
        auto truncated = std::make_shared<ByteArrayInputStream>(bytes->data(), cut);
        DataInputStream truncated_in(truncated);
        ASSERT_NOK_WITH_MSG(DeletionVector::Read(&truncated_in, std::nullopt, pool.get()),
                            "DataInputStream boundary check failed");
    }
    // Keep the frame intact and corrupt only its magic number.
    bytes->data()[Bitmap64DeletionVector::LENGTH_SIZE_BYTES] ^= 1;
    auto invalid_magic_input = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    DataInputStream invalid_magic_in(invalid_magic_input);
    ASSERT_NOK_WITH_MSG(DeletionVector::Read(&invalid_magic_in, length, pool.get()),
                        "Invalid magic number");
    // A complete frame containing only magic and CRC has no bitmap payload.
    char magic_only[] = {0, 0, 0, 4, static_cast<char>(0xD1), static_cast<char>(0xD3), 0x39, 0x64,
                         0, 0, 0, 0};
    auto magic_only_input = std::make_shared<ByteArrayInputStream>(magic_only, sizeof(magic_only));
    DataInputStream magic_only_in(magic_only_input);
    ASSERT_NOK_WITH_MSG(DeletionVector::Read(&magic_only_in, sizeof(magic_only), pool.get()),
                        "Invalid bitmap64 payload length");
}

}  // namespace paimon::test

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

#include "paimon/common/utils/optimized_roaring_bitmap64.h"

#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

constexpr int64_t kTwoTo32 = int64_t{1} << 32;

void AssertContains(const OptimizedRoaringBitmap64& bitmap, int64_t position) {
    ASSERT_OK_AND_ASSIGN(bool contains, bitmap.Contains(position));
    ASSERT_TRUE(contains);
}

void AssertNotContains(const OptimizedRoaringBitmap64& bitmap, int64_t position) {
    ASSERT_OK_AND_ASSIGN(bool contains, bitmap.Contains(position));
    ASSERT_FALSE(contains);
}

void AssertContent(const OptimizedRoaringBitmap64& bitmap, const std::set<int64_t>& expected) {
    ASSERT_EQ(static_cast<int64_t>(expected.size()), bitmap.Cardinality());
    for (int64_t position : expected) {
        AssertContains(bitmap, position);
    }

    std::vector<int64_t> actual;
    bitmap.ForEach([&actual](int64_t position) { actual.push_back(position); });
    ASSERT_EQ(std::vector<int64_t>(expected.begin(), expected.end()), actual);
}

void AssertRoundTrip(const OptimizedRoaringBitmap64& bitmap, const std::set<int64_t>& expected) {
    PAIMON_UNIQUE_PTR<Bytes> bytes = bitmap.Serialize(GetDefaultPool().get());
    ASSERT_EQ(bitmap.GetSizeInBytes(), bytes->size());

    OptimizedRoaringBitmap64 deserialized;
    ASSERT_OK(deserialized.Deserialize(bytes->data(), bytes->size()));
    AssertContent(deserialized, expected);
    ASSERT_EQ(bitmap, deserialized);
}

}  // namespace

TEST(OptimizedRoaringBitmap64Test, TestHighAndLowBits) {
    OptimizedRoaringBitmap64 bitmap;
    ASSERT_TRUE(bitmap.IsEmpty());

    const std::vector<int64_t> positions = {
        0, 42, int64_t{1} << 31, kTwoTo32 - 1, kTwoTo32, kTwoTo32 + 5, 2 * kTwoTo32 + 9,
    };
    for (int64_t position : positions) {
        ASSERT_OK(bitmap.Add(position));
    }
    ASSERT_OK(bitmap.Add(42));

    ASSERT_EQ(3, bitmap.GetAllocatedBitmapCount());
    ASSERT_EQ(positions.size(), bitmap.Cardinality());
    for (int64_t position : positions) {
        AssertContains(bitmap, position);
    }
    AssertNotContains(bitmap, kTwoTo32 + 6);

    std::vector<int64_t> actual;
    bitmap.ForEach([&actual](int64_t position) { actual.push_back(position); });
    ASSERT_EQ(positions, actual);

    ASSERT_NOK_WITH_MSG(bitmap.Add(-1), "positions that are >= 0");
    ASSERT_NOK_WITH_MSG(bitmap.Add(OptimizedRoaringBitmap64::kMaxValue + 1),
                        "positions that are >= 0");
    ASSERT_NOK_WITH_MSG(bitmap.Contains(-1), "positions that are >= 0");
    AssertNotContains(bitmap, OptimizedRoaringBitmap64::kMaxValue);
}

TEST(OptimizedRoaringBitmap64Test, TestRangeUnionAndConversion) {
    OptimizedRoaringBitmap64 bitmap;
    ASSERT_OK(bitmap.AddRange(kTwoTo32 - 2, kTwoTo32 + 2));
    ASSERT_EQ(2, bitmap.GetAllocatedBitmapCount());

    OptimizedRoaringBitmap64 other;
    ASSERT_OK(other.Add(kTwoTo32 + 10));
    bitmap |= other;
    ASSERT_EQ(5, bitmap.Cardinality());
    AssertContains(bitmap, kTwoTo32 - 2);
    AssertContains(bitmap, kTwoTo32 - 1);
    AssertContains(bitmap, kTwoTo32);
    AssertContains(bitmap, kTwoTo32 + 1);
    AssertContains(bitmap, kTwoTo32 + 10);

    RoaringBitmap32 bitmap32;
    bitmap32.Add(1);
    OptimizedRoaringBitmap64 converted = OptimizedRoaringBitmap64::FromRoaringBitmap32(bitmap32);
    ASSERT_EQ(1, converted.GetAllocatedBitmapCount());
    AssertContains(converted, 1);
}

TEST(OptimizedRoaringBitmap64Test, TestCompatibleWithJava) {
    // This payload was written by Java Paimon's OptimizedRoaringBitmap64 for positions {2, 3}.
    const std::vector<uint8_t> java_bytes = {
        1,  0,  0, 0, 0, 0, 0, 0,  // number of inner bitmaps
        0,  0,  0, 0,              // high-32-bit key
        58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 16, 0, 0, 0, 2, 0, 3, 0,
    };

    OptimizedRoaringBitmap64 bitmap;
    ASSERT_OK(
        bitmap.Deserialize(reinterpret_cast<const char*>(java_bytes.data()), java_bytes.size()));
    ASSERT_EQ(2, bitmap.Cardinality());
    AssertContains(bitmap, 2);
    AssertContains(bitmap, 3);

    PAIMON_UNIQUE_PTR<Bytes> serialized = bitmap.Serialize(GetDefaultPool().get());
    std::vector<uint8_t> actual(serialized->data(), serialized->data() + serialized->size());
    ASSERT_EQ(java_bytes, actual);

    OptimizedRoaringBitmap64 cpp_bitmap;
    ASSERT_OK(cpp_bitmap.Add(2));
    ASSERT_OK(cpp_bitmap.Add(3));
    cpp_bitmap.RunLengthEncode();
    PAIMON_UNIQUE_PTR<Bytes> cpp_bytes = cpp_bitmap.Serialize(GetDefaultPool().get());
    std::vector<uint8_t> cpp_actual(cpp_bytes->data(), cpp_bytes->data() + cpp_bytes->size());
    ASSERT_EQ(java_bytes, cpp_actual);
}

TEST(OptimizedRoaringBitmap64Test, TestCompatibleWithJavaLargeContinuousData) {
    const int64_t start = kTwoTo32 - 50000;
    const int64_t end = kTwoTo32 + 50000;
    OptimizedRoaringBitmap64 bitmap;
    ASSERT_OK(bitmap.AddRange(start, end));
    ASSERT_TRUE(bitmap.RunLengthEncode());

    // Generated by Java Paimon's OptimizedRoaringBitmap64 after runLengthEncode().
    const std::vector<uint8_t> java_bytes = {
        2,  0,  0, 0, 0, 0,   0,   0,  // number of inner bitmaps
        0,  0,  0, 0,                  // high-32-bit key 0
        59, 48, 0, 0, 1, 255, 255, 79, 195, 1, 0, 176, 60, 79, 195,
        1,  0,  0, 0,  // high-32-bit key 1
        59, 48, 0, 0, 1, 0,   0,   79, 195, 1, 0, 0,   0,  79, 195,
    };

    PAIMON_UNIQUE_PTR<Bytes> bytes = bitmap.Serialize(GetDefaultPool().get());
    ASSERT_EQ(java_bytes, std::vector<uint8_t>(bytes->data(), bytes->data() + bytes->size()));

    OptimizedRoaringBitmap64 deserialized;
    ASSERT_OK(deserialized.Deserialize(reinterpret_cast<const char*>(java_bytes.data()),
                                       java_bytes.size()));
    ASSERT_EQ(bitmap, deserialized);
    ASSERT_EQ(end - start, deserialized.Cardinality());
    ASSERT_EQ(2, deserialized.GetAllocatedBitmapCount());
    AssertContains(deserialized, start);
    AssertContains(deserialized, kTwoTo32 - 1);
    AssertContains(deserialized, kTwoTo32);
    AssertContains(deserialized, end - 1);
    AssertNotContains(deserialized, start - 1);
    AssertNotContains(deserialized, end);
}

TEST(OptimizedRoaringBitmap64Test, TestCompatibleWithJavaLargeSparseData) {
    OptimizedRoaringBitmap64 bitmap;
    for (int64_t position = 5000; position < 10000; position += 17) {
        ASSERT_OK(bitmap.Add(position));
    }
    bitmap.RunLengthEncode();

    // Generated by Java Paimon's OptimizedRoaringBitmap64 after runLengthEncode().
    const std::vector<uint8_t> java_bytes = {
        1,   0,  0,   0,  0,   0,  0,   0,  0,   0,  0,   0,  58,  48, 0,   0,  1,   0,  0,   0,
        0,   0,  38,  1,  16,  0,  0,   0,  136, 19, 153, 19, 170, 19, 187, 19, 204, 19, 221, 19,
        238, 19, 255, 19, 16,  20, 33,  20, 50,  20, 67,  20, 84,  20, 101, 20, 118, 20, 135, 20,
        152, 20, 169, 20, 186, 20, 203, 20, 220, 20, 237, 20, 254, 20, 15,  21, 32,  21, 49,  21,
        66,  21, 83,  21, 100, 21, 117, 21, 134, 21, 151, 21, 168, 21, 185, 21, 202, 21, 219, 21,
        236, 21, 253, 21, 14,  22, 31,  22, 48,  22, 65,  22, 82,  22, 99,  22, 116, 22, 133, 22,
        150, 22, 167, 22, 184, 22, 201, 22, 218, 22, 235, 22, 252, 22, 13,  23, 30,  23, 47,  23,
        64,  23, 81,  23, 98,  23, 115, 23, 132, 23, 149, 23, 166, 23, 183, 23, 200, 23, 217, 23,
        234, 23, 251, 23, 12,  24, 29,  24, 46,  24, 63,  24, 80,  24, 97,  24, 114, 24, 131, 24,
        148, 24, 165, 24, 182, 24, 199, 24, 216, 24, 233, 24, 250, 24, 11,  25, 28,  25, 45,  25,
        62,  25, 79,  25, 96,  25, 113, 25, 130, 25, 147, 25, 164, 25, 181, 25, 198, 25, 215, 25,
        232, 25, 249, 25, 10,  26, 27,  26, 44,  26, 61,  26, 78,  26, 95,  26, 112, 26, 129, 26,
        146, 26, 163, 26, 180, 26, 197, 26, 214, 26, 231, 26, 248, 26, 9,   27, 26,  27, 43,  27,
        60,  27, 77,  27, 94,  27, 111, 27, 128, 27, 145, 27, 162, 27, 179, 27, 196, 27, 213, 27,
        230, 27, 247, 27, 8,   28, 25,  28, 42,  28, 59,  28, 76,  28, 93,  28, 110, 28, 127, 28,
        144, 28, 161, 28, 178, 28, 195, 28, 212, 28, 229, 28, 246, 28, 7,   29, 24,  29, 41,  29,
        58,  29, 75,  29, 92,  29, 109, 29, 126, 29, 143, 29, 160, 29, 177, 29, 194, 29, 211, 29,
        228, 29, 245, 29, 6,   30, 23,  30, 40,  30, 57,  30, 74,  30, 91,  30, 108, 30, 125, 30,
        142, 30, 159, 30, 176, 30, 193, 30, 210, 30, 227, 30, 244, 30, 5,   31, 22,  31, 39,  31,
        56,  31, 73,  31, 90,  31, 107, 31, 124, 31, 141, 31, 158, 31, 175, 31, 192, 31, 209, 31,
        226, 31, 243, 31, 4,   32, 21,  32, 38,  32, 55,  32, 72,  32, 89,  32, 106, 32, 123, 32,
        140, 32, 157, 32, 174, 32, 191, 32, 208, 32, 225, 32, 242, 32, 3,   33, 20,  33, 37,  33,
        54,  33, 71,  33, 88,  33, 105, 33, 122, 33, 139, 33, 156, 33, 173, 33, 190, 33, 207, 33,
        224, 33, 241, 33, 2,   34, 19,  34, 36,  34, 53,  34, 70,  34, 87,  34, 104, 34, 121, 34,
        138, 34, 155, 34, 172, 34, 189, 34, 206, 34, 223, 34, 240, 34, 1,   35, 18,  35, 35,  35,
        52,  35, 69,  35, 86,  35, 103, 35, 120, 35, 137, 35, 154, 35, 171, 35, 188, 35, 205, 35,
        222, 35, 239, 35, 0,   36, 17,  36, 34,  36, 51,  36, 68,  36, 85,  36, 102, 36, 119, 36,
        136, 36, 153, 36, 170, 36, 187, 36, 204, 36, 221, 36, 238, 36, 255, 36, 16,  37, 33,  37,
        50,  37, 67,  37, 84,  37, 101, 37, 118, 37, 135, 37, 152, 37, 169, 37, 186, 37, 203, 37,
        220, 37, 237, 37, 254, 37, 15,  38, 32,  38, 49,  38, 66,  38, 83,  38, 100, 38, 117, 38,
        134, 38, 151, 38, 168, 38, 185, 38, 202, 38, 219, 38, 236, 38, 253, 38, 14,  39,
    };

    PAIMON_UNIQUE_PTR<Bytes> bytes = bitmap.Serialize(GetDefaultPool().get());
    ASSERT_EQ(java_bytes, std::vector<uint8_t>(bytes->data(), bytes->data() + bytes->size()));

    OptimizedRoaringBitmap64 deserialized;
    ASSERT_OK(deserialized.Deserialize(reinterpret_cast<const char*>(java_bytes.data()),
                                       java_bytes.size()));
    ASSERT_EQ(bitmap, deserialized);
    for (int64_t position = 5000; position < 10000; position += 17) {
        AssertContains(deserialized, position);
    }
}

TEST(OptimizedRoaringBitmap64Test, TestAddAndCardinality) {
    OptimizedRoaringBitmap64 bitmap;
    ASSERT_EQ(0, bitmap.Cardinality());

    ASSERT_OK(bitmap.Add(0));
    ASSERT_OK(bitmap.Add(10));
    ASSERT_OK(bitmap.Add(10));
    ASSERT_OK(bitmap.Add(kTwoTo32 - 1));
    ASSERT_OK(bitmap.Add(kTwoTo32));
    ASSERT_OK(bitmap.Add((int64_t{100} << 32) + 7));

    AssertContent(bitmap, {0, 10, kTwoTo32 - 1, kTwoTo32, (int64_t{100} << 32) + 7});
    ASSERT_EQ(101, bitmap.GetAllocatedBitmapCount());
    AssertNotContains(bitmap, 9);
    AssertNotContains(bitmap, kTwoTo32 + 1);
}

TEST(OptimizedRoaringBitmap64Test, TestAddRange) {
    OptimizedRoaringBitmap64 bitmap;

    ASSERT_OK(bitmap.AddRange(10, 20));
    ASSERT_OK(bitmap.AddRange(42, 43));
    ASSERT_OK(bitmap.AddRange(kTwoTo32 - 3, kTwoTo32 + 3));
    ASSERT_OK(bitmap.AddRange(100, 100));
    ASSERT_OK(bitmap.AddRange(200, 100));

    std::set<int64_t> expected;
    for (int64_t position = 10; position < 20; ++position) {
        expected.insert(position);
    }
    expected.insert(42);
    for (int64_t position = kTwoTo32 - 3; position < kTwoTo32 + 3; ++position) {
        expected.insert(position);
    }
    AssertContent(bitmap, expected);
    AssertNotContains(bitmap, 9);
    AssertNotContains(bitmap, 20);
    AssertNotContains(bitmap, 41);
    AssertNotContains(bitmap, 43);
    AssertNotContains(bitmap, kTwoTo32 - 4);
    AssertNotContains(bitmap, kTwoTo32 + 3);

    OptimizedRoaringBitmap64 invalid;
    ASSERT_NOK_WITH_MSG(invalid.AddRange(-1, 1), "positions that are >= 0");
    ASSERT_TRUE(invalid.IsEmpty());
    ASSERT_NOK_WITH_MSG(invalid.AddRange(OptimizedRoaringBitmap64::kMaxValue + 1,
                                         OptimizedRoaringBitmap64::kMaxValue + 2),
                        "positions that are >= 0");
    ASSERT_TRUE(invalid.IsEmpty());
}

TEST(OptimizedRoaringBitmap64Test, TestUnion) {
    OptimizedRoaringBitmap64 lhs;
    ASSERT_OK(lhs.Add(10));
    ASSERT_OK(lhs.Add(20));
    ASSERT_OK(lhs.Add(kTwoTo32 + 30));

    OptimizedRoaringBitmap64 rhs;
    ASSERT_OK(rhs.Add(20));
    ASSERT_OK(rhs.Add(40));
    ASSERT_OK(rhs.Add((int64_t{3} << 32) + 50));

    lhs |= rhs;
    AssertContent(lhs, {10, 20, 40, kTwoTo32 + 30, (int64_t{3} << 32) + 50});
    AssertContent(rhs, {20, 40, (int64_t{3} << 32) + 50});

    OptimizedRoaringBitmap64 empty;
    lhs |= empty;
    AssertContent(lhs, {10, 20, 40, kTwoTo32 + 30, (int64_t{3} << 32) + 50});

    empty |= rhs;
    AssertContent(empty, {20, 40, (int64_t{3} << 32) + 50});

    lhs |= lhs;
    AssertContent(lhs, {10, 20, 40, kTwoTo32 + 30, (int64_t{3} << 32) + 50});
}

TEST(OptimizedRoaringBitmap64Test, TestCopyAndMove) {
    const std::set<int64_t> expected = {1, 2, kTwoTo32 + 3, (int64_t{2} << 32) + 4};
    OptimizedRoaringBitmap64 original;
    for (int64_t position : expected) {
        ASSERT_OK(original.Add(position));
    }

    OptimizedRoaringBitmap64 copy(original);
    ASSERT_EQ(original, copy);
    ASSERT_OK(copy.Add(99));
    AssertNotContains(original, 99);

    OptimizedRoaringBitmap64 assigned;
    ASSERT_OK(assigned.Add(1000));
    assigned = original;
    ASSERT_EQ(original, assigned);
    AssertNotContains(assigned, 1000);

    OptimizedRoaringBitmap64 moved(std::move(original));
    AssertContent(moved, expected);
    original = moved;  // A moved-from object can be restored by assignment.
    AssertContent(original, expected);

    OptimizedRoaringBitmap64 move_assigned;
    ASSERT_OK(move_assigned.Add(2000));
    move_assigned = std::move(assigned);
    AssertContent(move_assigned, expected);
    assigned = move_assigned;
    AssertContent(assigned, expected);
}

TEST(OptimizedRoaringBitmap64Test, TestFromRoaringBitmap32CopiesContent) {
    RoaringBitmap32 bitmap32;
    bitmap32.Add(1);
    bitmap32.Add(10);
    bitmap32.Add(std::numeric_limits<int32_t>::min());
    bitmap32.Add(-1);

    OptimizedRoaringBitmap64 bitmap = OptimizedRoaringBitmap64::FromRoaringBitmap32(bitmap32);
    AssertContent(bitmap, {1, 10, int64_t{1} << 31, static_cast<int64_t>(UINT32_MAX)});
}

TEST(OptimizedRoaringBitmap64Test, TestEqualityChecksBitmapArrayShape) {
    OptimizedRoaringBitmap64 empty;
    OptimizedRoaringBitmap64 another_empty;
    ASSERT_EQ(empty, another_empty);

    RoaringBitmap32 empty32;
    OptimizedRoaringBitmap64 empty_with_allocated_bitmap =
        OptimizedRoaringBitmap64::FromRoaringBitmap32(empty32);
    ASSERT_TRUE(empty_with_allocated_bitmap.IsEmpty());
    ASSERT_FALSE(empty == empty_with_allocated_bitmap);

    OptimizedRoaringBitmap64 lhs;
    OptimizedRoaringBitmap64 rhs;
    ASSERT_OK(lhs.Add(1));
    ASSERT_OK(rhs.Add(1));
    ASSERT_EQ(lhs, rhs);
    ASSERT_OK(rhs.Add(kTwoTo32 + 1));
    ASSERT_FALSE(lhs == rhs);
}

TEST(OptimizedRoaringBitmap64Test, TestRunLengthEncode) {
    OptimizedRoaringBitmap64 bitmap;
    std::set<int64_t> expected;
    for (int64_t position = 0; position < 10000; ++position) {
        ASSERT_OK(bitmap.Add(position));
        expected.insert(position);
    }
    for (int64_t position = kTwoTo32; position < kTwoTo32 + 5000; ++position) {
        ASSERT_OK(bitmap.Add(position));
        expected.insert(position);
    }

    const size_t size_before = bitmap.GetSizeInBytes();
    ASSERT_TRUE(bitmap.RunLengthEncode());
    const size_t size_after = bitmap.GetSizeInBytes();
    ASSERT_GT(size_before, size_after);
    AssertContent(bitmap, expected);
    AssertRoundTrip(bitmap, expected);
}

TEST(OptimizedRoaringBitmap64Test, TestSerializeDeserializeRoundTrip) {
    OptimizedRoaringBitmap64 bitmap;
    const std::set<int64_t> expected = {
        0,
        1,
        (int64_t{1} << 16) - 1,
        int64_t{1} << 16,
        (int64_t{1} << 31) - 1,
        int64_t{1} << 31,
        kTwoTo32 - 1,
        kTwoTo32,
        kTwoTo32 + 1,
        (int64_t{5} << 32) + 12345,
    };
    for (int64_t position : expected) {
        ASSERT_OK(bitmap.Add(position));
    }
    AssertRoundTrip(bitmap, expected);

    PAIMON_UNIQUE_PTR<Bytes> default_pool_bytes = bitmap.Serialize(nullptr);
    ASSERT_EQ(bitmap.GetSizeInBytes(), default_pool_bytes->size());

    OptimizedRoaringBitmap64 deserialized;
    ASSERT_OK(deserialized.Deserialize(default_pool_bytes->data(), default_pool_bytes->size()));
    ASSERT_EQ(bitmap, deserialized);
}

TEST(OptimizedRoaringBitmap64Test, TestSerializeDeserializeEmpty) {
    OptimizedRoaringBitmap64 bitmap;
    PAIMON_UNIQUE_PTR<Bytes> bytes = bitmap.Serialize(GetDefaultPool().get());
    ASSERT_EQ(8, bytes->size());
    ASSERT_EQ(std::vector<uint8_t>({0, 0, 0, 0, 0, 0, 0, 0}),
              std::vector<uint8_t>(bytes->data(), bytes->data() + bytes->size()));

    OptimizedRoaringBitmap64 deserialized;
    ASSERT_OK(deserialized.Deserialize(bytes->data(), bytes->size()));
    ASSERT_TRUE(deserialized.IsEmpty());
    ASSERT_EQ(0, deserialized.GetAllocatedBitmapCount());
    ASSERT_EQ(bitmap, deserialized);
}

TEST(OptimizedRoaringBitmap64Test, TestDeserializeFromInputStream) {
    OptimizedRoaringBitmap64 first;
    ASSERT_OK(first.Add(10));
    ASSERT_OK(first.Add(kTwoTo32 + 20));

    OptimizedRoaringBitmap64 second;
    ASSERT_OK(second.Add(30));
    ASSERT_OK(second.Add((int64_t{2} << 32) + 40));

    PAIMON_UNIQUE_PTR<Bytes> first_bytes = first.Serialize(GetDefaultPool().get());
    PAIMON_UNIQUE_PTR<Bytes> second_bytes = second.Serialize(GetDefaultPool().get());
    std::vector<char> concatenated(first_bytes->size() + second_bytes->size());
    std::memcpy(concatenated.data(), first_bytes->data(), first_bytes->size());
    std::memcpy(concatenated.data() + first_bytes->size(), second_bytes->data(),
                second_bytes->size());

    ByteArrayInputStream input(concatenated.data(), concatenated.size());
    OptimizedRoaringBitmap64 actual_first;
    ASSERT_OK(actual_first.Deserialize(&input));
    ASSERT_EQ(first, actual_first);
    ASSERT_OK_AND_ASSIGN(int64_t first_position, input.GetPos());
    ASSERT_EQ(first_bytes->size(), first_position);

    OptimizedRoaringBitmap64 actual_second;
    ASSERT_OK(actual_second.Deserialize(&input));
    ASSERT_EQ(second, actual_second);
    ASSERT_OK_AND_ASSIGN(int64_t second_position, input.GetPos());
    ASSERT_EQ(concatenated.size(), second_position);
}

TEST(OptimizedRoaringBitmap64Test, TestDeserializeRejectsInvalidInput) {
    OptimizedRoaringBitmap64 bitmap;
    ASSERT_OK(bitmap.Add(7));

    const std::vector<uint8_t> truncated_count = {1, 0, 0, 0};
    ASSERT_NOK_WITH_MSG(bitmap.Deserialize(reinterpret_cast<const char*>(truncated_count.data()),
                                           truncated_count.size()),
                        "boundary");
    ASSERT_NOK_WITH_MSG(bitmap.Deserialize(nullptr, 0), "null buffer");
    ASSERT_NOK_WITH_MSG(bitmap.Deserialize(static_cast<ByteArrayInputStream*>(nullptr)),
                        "null stream");

    const std::vector<uint8_t> negative_count = {
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    };
    ASSERT_NOK_WITH_MSG(bitmap.Deserialize(reinterpret_cast<const char*>(negative_count.data()),
                                           negative_count.size()),
                        "bitmap count");

    const std::vector<uint8_t> too_large_count = {
        0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00,
    };
    ASSERT_NOK_WITH_MSG(bitmap.Deserialize(reinterpret_cast<const char*>(too_large_count.data()),
                                           too_large_count.size()),
                        "bitmap count");

    const std::vector<uint8_t> truncated_key = {
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    ASSERT_NOK_WITH_MSG(bitmap.Deserialize(reinterpret_cast<const char*>(truncated_key.data()),
                                           truncated_key.size()),
                        "boundary");

    const std::vector<uint8_t> negative_key = {
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
    };
    ASSERT_NOK_WITH_MSG(
        bitmap.Deserialize(reinterpret_cast<const char*>(negative_key.data()), negative_key.size()),
        "Invalid unsigned key");

    const std::vector<uint8_t> too_large_key = {
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0x7F,
    };
    ASSERT_NOK_WITH_MSG(bitmap.Deserialize(reinterpret_cast<const char*>(too_large_key.data()),
                                           too_large_key.size()),
                        "Key is too large");

    const std::vector<uint8_t> malformed_bitmap = {
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    ASSERT_NOK_WITH_MSG(bitmap.Deserialize(reinterpret_cast<const char*>(malformed_bitmap.data()),
                                           malformed_bitmap.size()),
                        "Failed to deserialize inner RoaringBitmap32");
    AssertContains(bitmap, 7);
}

TEST(OptimizedRoaringBitmap64Test, TestDeserializeRejectsDuplicateKeys) {
    OptimizedRoaringBitmap64 single;
    ASSERT_OK(single.Add(1));
    PAIMON_UNIQUE_PTR<Bytes> bytes = single.Serialize(GetDefaultPool().get());

    std::vector<char> duplicate(bytes->data(), bytes->data() + bytes->size());
    duplicate[0] = 2;  // Two bitmap entries in little-endian encoding.
    duplicate.insert(duplicate.end(), bytes->data() + sizeof(int64_t),
                     bytes->data() + bytes->size());

    OptimizedRoaringBitmap64 bitmap;
    ASSERT_NOK_WITH_MSG(bitmap.Deserialize(duplicate.data(), duplicate.size()),
                        "Keys must be sorted");
}

}  // namespace paimon::test

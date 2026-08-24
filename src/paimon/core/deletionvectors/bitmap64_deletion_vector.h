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
#pragma once

#include <functional>
#include <limits>
#include <memory>

#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

/// A `DeletionVector` based on `RoaringBitmap64`, for files whose row count exceeds what
/// `BitmapDeletionVector` can index.
///
/// On-disk layout, byte-compatible with the shared Paimon format. Note that it is *not* the
/// layout `BitmapDeletionVector` uses, in two ways every reader has to special case:
///
///   length   int32 big-endian    = magic + bitmap byte count
///   magic    int32 LITTLE-endian = 1681511377   -- checksum covers from here ...
///   bitmap   RoaringBitmap64, little-endian                       -- ... to here
///   checksum int32 big-endian    = CRC32 over the magic and the bitmap
///
/// The magic is stored little-endian while `BitmapDeletionVector` stores its own big-endian,
/// which is what lets a reader tell the two apart from the same four bytes (see
/// `DeletionVector::Read`). The length recorded for a vector also differs: `DeletionVector`
/// metadata stores only the checksum-covered region for a bitmap32 vector, but the whole
/// record — length prefix and checksum included — for a bitmap64 one.
class Bitmap64DeletionVector : public DeletionVector {
 public:
    static constexpr int32_t MAGIC_NUMBER = 1681511377;
    static constexpr int32_t MAGIC_NUMBER_SIZE_BYTES = 4;
    static constexpr int32_t LENGTH_SIZE_BYTES = 4;
    static constexpr int32_t CRC_SIZE_BYTES = 4;

    /// Largest position this vector can hold. The underlying `RoaringBitmap64` would take
    /// more, but a larger position could not be read back by other Paimon engines.
    static constexpr int64_t MAX_VALUE =
        (static_cast<int64_t>(std::numeric_limits<int32_t>::max() - 1) << 32) |
        static_cast<int64_t>(static_cast<uint32_t>(std::numeric_limits<int32_t>::min()));

    Bitmap64DeletionVector() = default;

    explicit Bitmap64DeletionVector(const RoaringBitmap64& roaring_bitmap)
        : roaring_bitmap_(roaring_bitmap) {}

    Status Delete(int64_t position) override {
        PAIMON_RETURN_NOT_OK(CheckPosition(position));
        roaring_bitmap_.Add(position);
        return Status::OK();
    }

    Result<bool> CheckedDelete(int64_t position) override {
        PAIMON_RETURN_NOT_OK(CheckPosition(position));
        return roaring_bitmap_.CheckedAdd(position);
    }

    Result<bool> IsDeleted(int64_t position) const override {
        PAIMON_RETURN_NOT_OK(CheckPosition(position));
        return roaring_bitmap_.Contains(position);
    }

    bool IsEmpty() const override {
        return roaring_bitmap_.IsEmpty();
    }

    Result<int64_t> GetCardinality() const override {
        return roaring_bitmap_.Cardinality();
    }

    Status ForEachDeletedPosition(const std::function<Status(int64_t)>& consumer) const override {
        for (auto it = roaring_bitmap_.Begin(); it != roaring_bitmap_.End(); ++it) {
            PAIMON_RETURN_NOT_OK(consumer(*it));
        }
        return Status::OK();
    }

    Result<int32_t> SerializeTo(const std::shared_ptr<MemoryPool>& pool,
                                DataOutputStream* out) override;

    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeToBytes(
        const std::shared_ptr<MemoryPool>& pool) override;

    Status Merge(const std::shared_ptr<DeletionVector>& deletion_vector) override;

    const RoaringBitmap64* GetBitmap() const {
        return &roaring_bitmap_;
    }

    /// Deserializes the bitmap of a record whose magic number the caller already consumed.
    static Result<PAIMON_UNIQUE_PTR<DeletionVector>> DeserializeWithoutMagicNumber(
        const char* buffer, int32_t length, MemoryPool* pool);

 private:
    Status CheckPosition(int64_t position) const;

    RoaringBitmap64 roaring_bitmap_;
};

}  // namespace paimon

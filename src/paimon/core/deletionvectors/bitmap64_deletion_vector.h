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

#include "paimon/common/utils/optimized_roaring_bitmap64.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vector.h"

namespace paimon {

/// A `DeletionVector` based on `OptimizedRoaringBitmap64`, it only supports files with
/// row count not exceeding `OptimizedRoaringBitmap64::kMaxValue`.
///
/// Mostly copied from iceberg.
class Bitmap64DeletionVector : public DeletionVector {
 public:
    static constexpr int32_t MAGIC_NUMBER = 1681511377;
    static constexpr int32_t MAGIC_NUMBER_SIZE_BYTES = 4;
    static constexpr int32_t LENGTH_SIZE_BYTES = 4;
    static constexpr int32_t CRC_SIZE_BYTES = 4;

    static std::unique_ptr<Bitmap64DeletionVector> FromBitmapDeletionVector(
        const BitmapDeletionVector& dv);

    Status Delete(int64_t position) override {
        return roaring_bitmap_.Add(position);
    }

    Result<bool> CheckedDelete(int64_t position) override {
        PAIMON_ASSIGN_OR_RAISE(bool deleted, IsDeleted(position));
        if (deleted) {
            return false;
        }
        PAIMON_RETURN_NOT_OK(Delete(position));
        return true;
    }

    Result<bool> IsDeleted(int64_t position) const override {
        return roaring_bitmap_.Contains(position);
    }

    bool IsEmpty() const override {
        return roaring_bitmap_.IsEmpty();
    }

    Result<int64_t> GetCardinality() const override {
        return roaring_bitmap_.Cardinality();
    }

    Status ForEachDeletedPosition(const std::function<void(int64_t)>& consumer) const override {
        roaring_bitmap_.ForEach(consumer);
        return Status::OK();
    }

    Status Merge(const std::shared_ptr<DeletionVector>& deletion_vector) override;

    Result<int32_t> SerializeTo(const std::shared_ptr<MemoryPool>& pool,
                                DataOutputStream* out) override;

    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeToBytes(
        const std::shared_ptr<MemoryPool>& pool) override;

    static Result<PAIMON_UNIQUE_PTR<DeletionVector>> DeserializeWithoutMagicNumber(
        const char* buffer, int32_t length, MemoryPool* pool);

 private:
    OptimizedRoaringBitmap64 roaring_bitmap_;
};

}  // namespace paimon

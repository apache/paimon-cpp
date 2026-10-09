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

#include <cstring>
#include <limits>

#include "arrow/util/crc32.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/io/byte_order.h"

namespace paimon {

std::unique_ptr<Bitmap64DeletionVector> Bitmap64DeletionVector::FromBitmapDeletionVector(
    const BitmapDeletionVector& dv) {
    auto result = std::make_unique<Bitmap64DeletionVector>();
    result->roaring_bitmap_ = OptimizedRoaringBitmap64::FromRoaringBitmap32(*dv.GetBitmap());
    return result;
}

Status Bitmap64DeletionVector::Merge(const std::shared_ptr<DeletionVector>& deletion_vector) {
    auto* other = dynamic_cast<Bitmap64DeletionVector*>(deletion_vector.get());
    if (!other) {
        return Status::Invalid(
            "Cannot merge a non-Bitmap64DeletionVector into a Bitmap64DeletionVector");
    }
    roaring_bitmap_ |= other->roaring_bitmap_;
    return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<Bytes>> Bitmap64DeletionVector::SerializeToBytes(
    const std::shared_ptr<MemoryPool>& pool) {
    roaring_bitmap_.RunLengthEncode();
    size_t bitmap_size = roaring_bitmap_.GetSizeInBytes();
    constexpr int32_t overhead = MAGIC_NUMBER_SIZE_BYTES + LENGTH_SIZE_BYTES + CRC_SIZE_BYTES;
    if (bitmap_size > static_cast<size_t>(std::numeric_limits<int32_t>::max() - overhead)) {
        return Status::Invalid("Cannot serialize deletion vector > 2GB");
    }
    auto bitmap = roaring_bitmap_.Serialize(pool.get());
    auto bytes = Bytes::AllocateBytes(MAGIC_NUMBER_SIZE_BYTES + bitmap_size, pool.get());
    int32_t magic = ToLittleEndian(MAGIC_NUMBER);
    std::memcpy(bytes->data(), &magic, MAGIC_NUMBER_SIZE_BYTES);
    std::memcpy(bytes->data() + MAGIC_NUMBER_SIZE_BYTES, bitmap->data(), bitmap_size);
    return bytes;
}

Result<int32_t> Bitmap64DeletionVector::SerializeTo(const std::shared_ptr<MemoryPool>& pool,
                                                    DataOutputStream* out) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, SerializeToBytes(pool));
    int32_t length = static_cast<int32_t>(bytes->size());
    PAIMON_RETURN_NOT_OK(out->WriteValue<int32_t>(length));
    PAIMON_RETURN_NOT_OK(out->WriteBytes(bytes));
    uint32_t crc = arrow::internal::crc32(0, bytes->data(), bytes->size());
    PAIMON_RETURN_NOT_OK(out->WriteValue<int32_t>(static_cast<int32_t>(crc)));
    // Unlike DV32, Java DV64 metadata includes the length and CRC fields.
    return length + LENGTH_SIZE_BYTES + CRC_SIZE_BYTES;
}

Result<PAIMON_UNIQUE_PTR<DeletionVector>> Bitmap64DeletionVector::DeserializeWithoutMagicNumber(
    const char* buffer, int32_t length, MemoryPool* pool) {
    // Even an empty portable bitmap has an eight-byte bucket count.
    if (length < static_cast<int32_t>(sizeof(int64_t))) {
        return Status::Invalid("Invalid bitmap64 payload length");
    }
    auto result = pool->AllocateUnique<Bitmap64DeletionVector>();
    PAIMON_RETURN_NOT_OK(result->roaring_bitmap_.Deserialize(buffer, length));
    return result;
}

}  // namespace paimon

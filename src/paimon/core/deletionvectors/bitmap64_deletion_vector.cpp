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

#include <limits>

#include "arrow/util/crc32.h"
#include "fmt/format.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/utils/math.h"

namespace paimon {

Result<PAIMON_UNIQUE_PTR<Bytes>> Bitmap64DeletionVector::SerializeToBytes(
    const std::shared_ptr<MemoryPool>& pool) {
    // Serialize() run-length encodes the bitmap first, as the on-disk format requires.
    std::shared_ptr<Bytes> bitmap_bytes = roaring_bitmap_.Serialize(pool.get());
    if (bitmap_bytes == nullptr) {
        return Status::Invalid("roaring bitmap 64 serialize failed");
    }
    MemorySegmentOutputStream output(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    // The stream writes big-endian, so the swapped value lands on disk little-endian, which is
    // the byte order this magic is stored in and the reader tells the two vector kinds by.
    output.WriteValue<int32_t>(EndianSwapValue(MAGIC_NUMBER));
    output.WriteBytes(bitmap_bytes);
    return MemorySegmentUtils::CopyToBytes(output.Segments(), /*offset=*/0, output.CurrentSize(),
                                           pool.get());
}

Result<int32_t> Bitmap64DeletionVector::SerializeTo(const std::shared_ptr<MemoryPool>& pool,
                                                    DataOutputStream* out) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> data, SerializeToBytes(pool));
    size_t bitmap_data_length = data->size();
    // The record has to stay addressable by the int32 offset and length a deletion file meta
    // records, so the whole framed record is bounded, not just the bitmap.
    if (bitmap_data_length > static_cast<size_t>(std::numeric_limits<int32_t>::max() -
                                                 LENGTH_SIZE_BYTES - CRC_SIZE_BYTES)) {
        return Status::Invalid(
            fmt::format("Bitmap64 deletion vector of {} bytes does not fit a deletion file "
                        "record.",
                        bitmap_data_length));
    }
    PAIMON_RETURN_NOT_OK(out->WriteValue<int32_t>(static_cast<int32_t>(bitmap_data_length)));
    PAIMON_RETURN_NOT_OK(out->WriteBytes(data));
    uint32_t crc32 = 0;
    crc32 = arrow::internal::crc32(crc32, data->data(), bitmap_data_length);
    PAIMON_RETURN_NOT_OK(out->WriteValue<int32_t>(static_cast<int32_t>(crc32)));
    // Unlike a bitmap32 vector, whose recorded length covers only the checksummed region, a
    // bitmap64 record is recorded whole. `DeletionVector::Read` relies on the difference.
    return static_cast<int32_t>(LENGTH_SIZE_BYTES + bitmap_data_length + CRC_SIZE_BYTES);
}

Status Bitmap64DeletionVector::CheckPosition(int64_t position) const {
    if (position < 0 || position > MAX_VALUE) {
        return Status::Invalid(
            fmt::format("Bitmap64 deletion vector supports positions in [0, {}], but got {}.",
                        MAX_VALUE, position));
    }
    return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<DeletionVector>> Bitmap64DeletionVector::DeserializeWithoutMagicNumber(
    const char* buffer, int32_t length, MemoryPool* pool) {
    RoaringBitmap64 roaring_bitmap;
    PAIMON_RETURN_NOT_OK(roaring_bitmap.Deserialize(buffer, length));
    return pool->AllocateUnique<Bitmap64DeletionVector>(roaring_bitmap);
}

Status Bitmap64DeletionVector::Merge(const std::shared_ptr<DeletionVector>& deletion_vector) {
    if (!deletion_vector || deletion_vector->IsEmpty()) {
        return Status::OK();
    }
    auto* other = dynamic_cast<Bitmap64DeletionVector*>(deletion_vector.get());
    if (other == nullptr) {
        return Status::Invalid(
            "Cannot merge a non-Bitmap64DeletionVector into a Bitmap64DeletionVector");
    }
    roaring_bitmap_ |= other->roaring_bitmap_;
    return Status::OK();
}

}  // namespace paimon

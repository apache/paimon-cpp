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

#include "paimon/common/global_index/btree/btree_index_meta.h"

#include <cstddef>
#include <limits>

#include "fmt/format.h"
#include "paimon/common/memory/memory_slice_output.h"

namespace paimon {
namespace {

Result<std::shared_ptr<Bytes>> ReadKey(MemorySliceInput* input, int32_t key_length,
                                       int32_t required_remaining, const char* key_name,
                                       MemoryPool* pool) {
    if (key_length < 0) {
        return Status::Invalid(
            fmt::format("BTree index metadata has a negative {} length {}.", key_name, key_length));
    }
    if (input->Available() < required_remaining ||
        key_length > input->Available() - required_remaining) {
        return Status::Invalid(
            fmt::format("BTree index metadata {} length {} exceeds the available payload bytes.",
                        key_name, key_length));
    }
    if (key_length == 0) {
        return std::make_shared<Bytes>(0, pool);
    }
    return input->ReadSliceView(key_length).CopyBytes(pool);
}

}  // namespace

Result<std::shared_ptr<BTreeIndexMeta>> BTreeIndexMeta::Deserialize(
    const std::shared_ptr<Bytes>& meta, paimon::MemoryPool* pool) {
    if (meta == nullptr) {
        return Status::Invalid("Cannot deserialize BTree index metadata from a null buffer.");
    }
    if (pool == nullptr) {
        return Status::Invalid("Cannot deserialize BTree index metadata with a null memory pool.");
    }
    // Legacy metadata contains two int32 lengths and one has-nulls byte.
    constexpr size_t kMinimumMetadataSize = 2 * sizeof(int32_t) + sizeof(int8_t);
    if (meta->size() < kMinimumMetadataSize) {
        return Status::Invalid(fmt::format(
            "BTree index metadata is truncated: expected at least {} bytes, but found {}.",
            kMinimumMetadataSize, meta->size()));
    }
    if (meta->size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        return Status::Invalid(
            fmt::format("BTree index metadata size {} exceeds the supported maximum {}.",
                        meta->size(), std::numeric_limits<int32_t>::max()));
    }

    MemorySlice slice = MemorySlice::Wrap(meta);
    MemorySliceInput input = slice.ToInput();
    int32_t first_key_len = input.ReadInt();
    constexpr int32_t kRequiredAfterFirstKey = sizeof(int32_t) + sizeof(int8_t);
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<Bytes> first_key,
        ReadKey(&input, first_key_len, kRequiredAfterFirstKey, "first key", pool));
    int32_t last_key_len = input.ReadInt();
    constexpr int32_t kRequiredAfterLastKey = sizeof(int8_t);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> last_key,
                           ReadKey(&input, last_key_len, kRequiredAfterLastKey, "last key", pool));
    int8_t has_nulls_byte = input.ReadByte();
    if (has_nulls_byte != 0 && has_nulls_byte != 1) {
        return Status::Invalid(
            fmt::format("BTree index metadata has invalid has-nulls value {}.", has_nulls_byte));
    }
    bool has_nulls = has_nulls_byte == 1;

    if (input.Available() == 2) {
        int8_t format_version = input.ReadByte();
        if (format_version != kFormatVersionWithNullFlags) {
            return Status::Invalid(
                fmt::format("Unsupported BTree index metadata version {}.", format_version));
        }
        int8_t null_key_flags = input.ReadByte();
        constexpr int8_t kKnownNullFlags = kFirstKeyIsNull | kLastKeyIsNull;
        if ((null_key_flags & ~kKnownNullFlags) != 0) {
            return Status::Invalid(
                fmt::format("BTree index metadata has invalid null-key flags {}.", null_key_flags));
        }
        if ((null_key_flags & kFirstKeyIsNull) != 0) {
            if (first_key_len != 0) {
                return Status::Invalid("BTree index metadata marks a non-empty first key as null.");
            }
            first_key.reset();
        }
        if ((null_key_flags & kLastKeyIsNull) != 0) {
            if (last_key_len != 0) {
                return Status::Invalid("BTree index metadata marks a non-empty last key as null.");
            }
            last_key.reset();
        }
    } else if (input.Available() == 0 && first_key_len == 0 && last_key_len == 0 && has_nulls) {
        // Legacy metadata used zero length for null keys. Both empty boundaries plus a null bitmap
        // identify an all-null file; a single empty boundary remains a valid serialized key.
        first_key.reset();
        last_key.reset();
    } else if (input.Available() != 0) {
        return Status::Invalid(fmt::format("BTree index metadata has {} unexpected trailing bytes.",
                                           input.Available()));
    }
    return std::make_shared<BTreeIndexMeta>(first_key, last_key, has_nulls);
}

std::shared_ptr<Bytes> BTreeIndexMeta::Serialize(paimon::MemoryPool* pool) const {
    int32_t first_key_size = first_key_ ? first_key_->size() : 0;
    int32_t last_key_size = last_key_ ? last_key_->size() : 0;
    int32_t total_size = Size();
    MemorySliceOutput output(total_size, pool);
    int8_t null_key_flags = 0;

    output.WriteValue(first_key_size);
    if (first_key_) {
        output.WriteBytes(first_key_);
    } else {
        null_key_flags |= kFirstKeyIsNull;
    }

    output.WriteValue(last_key_size);
    if (last_key_) {
        output.WriteBytes(last_key_);
    } else {
        null_key_flags |= kLastKeyIsNull;
    }

    output.WriteValue(static_cast<int8_t>(has_nulls_ ? 1 : 0));
    output.WriteValue(kFormatVersionWithNullFlags);
    output.WriteValue(null_key_flags);

    return output.ToSlice().GetOrCreateHeapMemory(pool);
}

}  // namespace paimon

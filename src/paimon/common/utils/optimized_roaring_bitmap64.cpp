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

/* This file is based on source code from the Iceberg Project (http://iceberg.apache.org/),
 * licensed by the Apache Software Foundation (ASF) under the Apache License, Version 2.0. See the
 * NOTICE file distributed with this work for additional information regarding copyright
 * ownership. */

#include "paimon/common/utils/optimized_roaring_bitmap64.h"

#include <cstring>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/utils/math.h"
#include "paimon/result.h"
#include "roaring.hh"  // NOLINT(build/include_subdir)

namespace paimon {
namespace {

constexpr size_t kBitmapCountSizeBytes = sizeof(int64_t);
constexpr size_t kBitmapKeySizeBytes = sizeof(int32_t);

template <typename T>
void WriteLittleEndian(T value, char** output) {
    T little_endian = ToLittleEndian(value);
    std::memcpy(*output, &little_endian, sizeof(T));
    *output += sizeof(T);
}

template <typename T>
Result<T> ReadLittleEndian(ByteArrayInputStream* input_stream) {
    T little_endian = 0;
    PAIMON_ASSIGN_OR_RAISE(int64_t read_size,
                           input_stream->Read(reinterpret_cast<char*>(&little_endian), sizeof(T)));
    if (read_size != sizeof(T)) {
        return Status::Invalid(
            fmt::format("Failed to read {} bytes, only read {}", sizeof(T), read_size));
    }
    return FromLittleEndian(little_endian);
}

}  // namespace

class OptimizedRoaringBitmap64::Impl {
 public:
    std::vector<roaring::Roaring> bitmaps;
};

OptimizedRoaringBitmap64::OptimizedRoaringBitmap64() : impl_(std::make_unique<Impl>()) {}

OptimizedRoaringBitmap64::~OptimizedRoaringBitmap64() = default;

OptimizedRoaringBitmap64::OptimizedRoaringBitmap64(const OptimizedRoaringBitmap64& other)
    : impl_(std::make_unique<Impl>(*other.impl_)) {}

OptimizedRoaringBitmap64& OptimizedRoaringBitmap64::operator=(
    const OptimizedRoaringBitmap64& other) {
    if (this != &other) {
        if (impl_ == nullptr) {
            impl_ = std::make_unique<Impl>(*other.impl_);
        } else {
            *impl_ = *other.impl_;
        }
    }
    return *this;
}

OptimizedRoaringBitmap64::OptimizedRoaringBitmap64(OptimizedRoaringBitmap64&& other) noexcept
    : impl_(std::move(other.impl_)) {}

OptimizedRoaringBitmap64& OptimizedRoaringBitmap64::operator=(
    OptimizedRoaringBitmap64&& other) noexcept {
    if (this != &other) {
        impl_ = std::move(other.impl_);
    }
    return *this;
}

OptimizedRoaringBitmap64 OptimizedRoaringBitmap64::FromRoaringBitmap32(
    const RoaringBitmap32& bitmap) {
    OptimizedRoaringBitmap64 result;
    const auto* roaring_bitmap = static_cast<const roaring::Roaring*>(bitmap.roaring_bitmap_);
    result.impl_->bitmaps.push_back(*roaring_bitmap);
    return result;
}

Status OptimizedRoaringBitmap64::Add(int64_t position) {
    PAIMON_RETURN_NOT_OK(CheckPosition(position));
    const int32_t key = static_cast<int32_t>(position >> 32);
    const uint32_t position32 = static_cast<uint32_t>(position);
    AllocateBitmapsIfNeeded(static_cast<size_t>(key) + 1);
    impl_->bitmaps[key].add(position32);
    return Status::OK();
}

Status OptimizedRoaringBitmap64::AddRange(int64_t start, int64_t end) {
    for (int64_t position = start; position < end; ++position) {
        PAIMON_RETURN_NOT_OK(Add(position));
    }
    return Status::OK();
}

OptimizedRoaringBitmap64& OptimizedRoaringBitmap64::operator|=(
    const OptimizedRoaringBitmap64& other) {
    AllocateBitmapsIfNeeded(other.impl_->bitmaps.size());
    for (size_t key = 0; key < other.impl_->bitmaps.size(); ++key) {
        impl_->bitmaps[key] |= other.impl_->bitmaps[key];
    }
    return *this;
}

Result<bool> OptimizedRoaringBitmap64::Contains(int64_t position) const {
    PAIMON_RETURN_NOT_OK(CheckPosition(position));
    const int32_t key = static_cast<int32_t>(position >> 32);
    const uint32_t position32 = static_cast<uint32_t>(position);
    return static_cast<size_t>(key) < impl_->bitmaps.size() &&
           impl_->bitmaps[key].contains(position32);
}

bool OptimizedRoaringBitmap64::IsEmpty() const {
    return Cardinality() == 0;
}

int64_t OptimizedRoaringBitmap64::Cardinality() const {
    int64_t cardinality = 0;
    for (const roaring::Roaring& bitmap : impl_->bitmaps) {
        cardinality += static_cast<int64_t>(bitmap.cardinality());
    }
    return cardinality;
}

bool OptimizedRoaringBitmap64::RunLengthEncode() {
    bool changed = false;
    for (roaring::Roaring& bitmap : impl_->bitmaps) {
        changed |= bitmap.runOptimize();
    }
    return changed;
}

void OptimizedRoaringBitmap64::ForEach(const std::function<void(int64_t)>& consumer) const {
    for (size_t key = 0; key < impl_->bitmaps.size(); ++key) {
        for (uint32_t position32 : impl_->bitmaps[key]) {
            const uint64_t position = (static_cast<uint64_t>(key) << 32) | position32;
            consumer(static_cast<int64_t>(position));
        }
    }
}

size_t OptimizedRoaringBitmap64::GetAllocatedBitmapCount() const {
    return impl_->bitmaps.size();
}

size_t OptimizedRoaringBitmap64::GetSizeInBytes() const {
    size_t size = kBitmapCountSizeBytes;
    for (const roaring::Roaring& bitmap : impl_->bitmaps) {
        size += kBitmapKeySizeBytes + bitmap.getSizeInBytes();
    }
    return size;
}

PAIMON_UNIQUE_PTR<Bytes> OptimizedRoaringBitmap64::Serialize(MemoryPool* pool) const {
    if (pool == nullptr) {
        pool = GetDefaultPool().get();
    }
    PAIMON_UNIQUE_PTR<Bytes> bytes = Bytes::AllocateBytes(GetSizeInBytes(), pool);
    char* output = bytes->data();
    WriteLittleEndian(static_cast<int64_t>(impl_->bitmaps.size()), &output);
    for (size_t key = 0; key < impl_->bitmaps.size(); ++key) {
        WriteLittleEndian(static_cast<int32_t>(key), &output);
        output += impl_->bitmaps[key].write(output);
    }
    return bytes;
}

Status OptimizedRoaringBitmap64::Deserialize(ByteArrayInputStream* input_stream) {
    if (input_stream == nullptr) {
        return Status::Invalid("Cannot deserialize OptimizedRoaringBitmap64 from a null stream");
    }

    PAIMON_ASSIGN_OR_RAISE(int64_t bitmap_count, ReadLittleEndian<int64_t>(input_stream));
    if (bitmap_count < 0 || bitmap_count > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid(fmt::format("Invalid bitmap count: {}", bitmap_count));
    }

    std::vector<roaring::Roaring> bitmaps;
    bitmaps.reserve(static_cast<size_t>(bitmap_count));
    int32_t last_key = -1;
    for (int64_t i = 0; i < bitmap_count; ++i) {
        PAIMON_ASSIGN_OR_RAISE(int32_t key, ReadLittleEndian<int32_t>(input_stream));
        if (key < 0) {
            return Status::Invalid(fmt::format("Invalid unsigned key: {}", key));
        }
        if (key > std::numeric_limits<int32_t>::max() - 1) {
            return Status::Invalid(fmt::format("Key is too large: {}", key));
        }
        if (key <= last_key) {
            return Status::Invalid("Keys must be sorted in ascending order");
        }

        bitmaps.resize(static_cast<size_t>(key));
        PAIMON_ASSIGN_OR_RAISE(int64_t position, input_stream->GetPos());
        PAIMON_ASSIGN_OR_RAISE(int64_t length, input_stream->Length());
        roaring::Roaring bitmap;
        try {
            bitmap = roaring::Roaring::readSafe(input_stream->GetRawData(), length - position);
        } catch (...) {
            return Status::Invalid("Failed to deserialize inner RoaringBitmap32");
        }
        const size_t bitmap_size = bitmap.getSizeInBytes();
        PAIMON_RETURN_NOT_OK(
            input_stream->Seek(static_cast<int64_t>(bitmap_size), SeekOrigin::FS_SEEK_CUR));
        bitmaps.push_back(std::move(bitmap));
        last_key = key;
    }

    impl_->bitmaps = std::move(bitmaps);
    return Status::OK();
}

Status OptimizedRoaringBitmap64::Deserialize(const char* begin, size_t length) {
    if (begin == nullptr) {
        return Status::Invalid("Cannot deserialize OptimizedRoaringBitmap64 from a null buffer");
    }
    ByteArrayInputStream input_stream(begin, static_cast<int64_t>(length));
    return Deserialize(&input_stream);
}

bool OptimizedRoaringBitmap64::operator==(const OptimizedRoaringBitmap64& other) const noexcept {
    if (this == &other) {
        return true;
    }
    return impl_->bitmaps == other.impl_->bitmaps;
}

Status OptimizedRoaringBitmap64::CheckPosition(int64_t position) {
    if (position < 0 || position > kMaxValue) {
        return Status::Invalid(
            fmt::format("OptimizedRoaringBitmap64 supports positions that are >= 0 and <= {}: {}",
                        kMaxValue, position));
    }
    return Status::OK();
}

void OptimizedRoaringBitmap64::AllocateBitmapsIfNeeded(size_t required_length) {
    if (impl_->bitmaps.size() < required_length) {
        impl_->bitmaps.resize(required_length);
    }
}

}  // namespace paimon

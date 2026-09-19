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

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>

#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {

/// A bitmap for non-negative 64-bit positions that uses the high 32 bits as an array index
/// and stores the low 32 bits in a 32-bit Roaring bitmap.
///
/// This layout is compatible with Java Paimon's `OptimizedRoaringBitmap64` and is optimized for
/// positions whose high 32 bits are small, in particular file-local row positions.
class OptimizedRoaringBitmap64 {
 public:
    static constexpr int64_t kMaxValue =
        (static_cast<int64_t>(std::numeric_limits<int32_t>::max() - 1) << 32) |
        static_cast<uint32_t>(std::numeric_limits<int32_t>::min());

    OptimizedRoaringBitmap64();
    ~OptimizedRoaringBitmap64();

    OptimizedRoaringBitmap64(const OptimizedRoaringBitmap64& other);
    OptimizedRoaringBitmap64& operator=(const OptimizedRoaringBitmap64& other);

    OptimizedRoaringBitmap64(OptimizedRoaringBitmap64&& other) noexcept;
    OptimizedRoaringBitmap64& operator=(OptimizedRoaringBitmap64&& other) noexcept;

    /// Create an optimized 64-bit bitmap containing all positions from a 32-bit bitmap.
    static OptimizedRoaringBitmap64 FromRoaringBitmap32(const RoaringBitmap32& bitmap);

    /// Add a position.
    Status Add(int64_t position);

    /// Add all positions in the half-open interval [start, end).
    Status AddRange(int64_t start, int64_t end);

    /// Union another bitmap into this bitmap.
    OptimizedRoaringBitmap64& operator|=(const OptimizedRoaringBitmap64& other);

    /// Return whether the bitmap contains a position.
    Result<bool> Contains(int64_t position) const;

    /// Return whether the bitmap is empty.
    bool IsEmpty() const;

    /// Return the number of positions in the bitmap.
    int64_t Cardinality() const;

    /// Apply run-length encoding to inner bitmaps when it is more space efficient.
    bool RunLengthEncode();

    /// Visit positions in ascending order.
    void ForEach(const std::function<void(int64_t)>& consumer) const;

    /// Return the number of allocated inner 32-bit bitmaps.
    size_t GetAllocatedBitmapCount() const;

    /// Return the number of bytes required by the portable serialized form.
    size_t GetSizeInBytes() const;

    /// Serialize using the Java-compatible little-endian portable format.
    PAIMON_UNIQUE_PTR<Bytes> Serialize(MemoryPool* pool) const;

    /// Deserialize from the current position of an input stream.
    Status Deserialize(ByteArrayInputStream* input_stream);

    /// Deserialize from a buffer.
    Status Deserialize(const char* begin, size_t length);

    bool operator==(const OptimizedRoaringBitmap64& other) const noexcept;

 private:
    class Impl;

    static Status CheckPosition(int64_t position);
    void AllocateBitmapsIfNeeded(size_t required_length);

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon

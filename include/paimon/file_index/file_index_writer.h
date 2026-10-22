/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "paimon/memory/bytes.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowArray;

namespace paimon {
class OutputStream;

/// Interface for writing file-level index data from Arrow batches.
class PAIMON_EXPORT FileIndexWriter {
 public:
    virtual ~FileIndexWriter() = default;

    /// Adds a batch of data to the index writer.
    ///
    /// @param batch Pointer to a C ArrowArray derived from arrow struct array contain specified
    ///              indexed field.
    /// @return `Status::OK()` on success; otherwise, an error indicating failure (e.g., schema
    ///         mismatch).
    virtual Status AddBatch(::ArrowArray* batch) = 0;

    /// Serializes the built index into a byte buffer.
    ///
    /// @note This method returns the complete serialized form of the index after all batches
    /// have been added. It can be called only once and typically assumes no further calls to
    /// `AddBatch()` will occur afterward.
    ///
    /// Streaming-only writers may override WriteTo() and use the default NotImplemented result
    /// from this method.
    ///
    /// @return A unique pointer to a byte array containing the serialized index data,
    ///         or an error if serialization fails.
    virtual Result<PAIMON_UNIQUE_PTR<Bytes>> SerializedBytes() const;

    /// Writes one index payload to the caller-owned stream without closing it.
    /// The stream is borrowed for this call and must not be retained by the writer.
    /// The default implementation writes SerializedBytes(); streaming writers can override this
    /// method to avoid materializing the complete payload. Write failures must be propagated.
    /// @return true if a non-empty index payload was produced, false if there is no index, or an
    /// error. Returning false must not write any bytes. The default implementation treats null or
    /// zero-length SerializedBytes() results as no index.
    virtual Result<bool> WriteTo(OutputStream* output) const;
};

}  // namespace paimon

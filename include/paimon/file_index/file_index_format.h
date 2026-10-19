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
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/file_index/file_index_reader.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {
class InputStream;
class MemoryPool;
class Bytes;
class OutputStream;

/// Defines the on-disk format and versioning for Paimon file-level indexes.
/// Supports V1 and V2 containers. V2 uses int64 offsets and lengths and supports streaming index
/// payloads directly to the output; avoiding full payload buffering requires a streaming WriteTo()
/// implementation in the index plugin.
/// See file_index_format_v1.h and file_index_format_v2.h for the serialized layouts.
class PAIMON_EXPORT FileIndexFormat {
 public:
    class Reader;
    class Writer;

    /// Serialized file indexes grouped as column name -> index type -> index bytes. A null bytes
    /// pointer represents an empty index for that column and index type.
    /// For example, indexes["col1"]["bsi"] = <bytes>;
    using ColumnIndexes = std::map<std::string, std::map<std::string, std::shared_ptr<Bytes>>>;

    /// Creates a `Reader` to parse a index file (may contain multiple indexes) from the given input
    /// stream.
    /// @param input_stream Input stream containing serialized index data.
    /// @param pool Memory pool for temporary allocations during reading.
    /// @param options Options used to configure File Index backends, expressed in file field names.
    /// @return A unique pointer to a `Reader` on success, or an error if the stream is invalid
    ///         (e.g., wrong magic, unsupported version, or corrupted data).
    static Result<std::unique_ptr<Reader>> CreateReader(
        const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool,
        const std::map<std::string, std::string>& options);

    /// Creates a `Writer` for file index container.
    ///
    /// @param output_stream Destination stream for serialized index data.
    /// @param pool Memory pool for writer-side allocations.
    /// @param version Container format version; defaults to V1.
    /// @return A unique pointer to a `Writer` on success.
    static Result<std::unique_ptr<Writer>> CreateWriter(
        const std::shared_ptr<OutputStream>& output_stream, const std::shared_ptr<MemoryPool>& pool,
        int32_t version = V_1);

 public:
    static const int64_t MAGIC;
    static const int32_t EMPTY_INDEX_FLAG;
    static const int32_t V_1;
    static const int32_t V_2;
};

/// Writer for file index file.
class PAIMON_EXPORT FileIndexFormat::Writer {
 public:
    /// Writes one payload without closing the stream. Returns false for an absent index
    /// (without writing bytes), or true after writing a non-empty payload.
    using PayloadWriter = std::function<Result<bool>(const std::shared_ptr<OutputStream>&)>;
    virtual ~Writer() = default;

    /// Flushes and closes the output stream supplied to `CreateWriter()`.
    virtual Status Close() = 0;

    /// Writes one index. V1 buffers payloads; V2 writes them directly to the destination.
    /// An empty callback or a false result denotes an empty index (-1, 0).
    /// A true result indicates that a non-empty payload was written.
    /// Call Finish() after the last index. Payload failures are terminal.
    virtual Status WriteIndex(const std::string& column_name, const std::string& index_type,
                              const PayloadWriter& payload) = 0;

    /// Completes the container: writes V1's header and buffered bodies, or V2's footer and trailer.
    virtual Status Finish() = 0;
};

/// Reader for file index file.
class PAIMON_EXPORT FileIndexFormat::Reader {
 public:
    virtual ~Reader() = default;
    /// Reads index data for a specific column from the index file.
    /// @param column_name Name of the column to retrieve index data for.
    /// @param arrow_schema Arrow schema that must contain a field corresponding to `column_name`.
    /// @return A vector of shared pointers to FileIndexReader objects, each corresponding to a
    ///         different index type; or an error if the column is not indexed or the index is
    ///         malformed.
    virtual Result<std::vector<std::shared_ptr<FileIndexReader>>> ReadColumnIndex(
        const std::string& column_name, ::ArrowSchema* arrow_schema) const = 0;
};

}  // namespace paimon

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
/// File index file format. Put all column and offset in the header.
///
/// <pre>
///  _______________________________________    _____________________
/// |     magic    | version | head length |
/// |--------------------------------------|
/// |            column number             |
/// |--------------------------------------|
/// |   column 1        |  index number    |
/// |--------------------------------------|
/// |  index name 1 | start pos | length   |
/// |--------------------------------------|
/// |  index name 2 | start pos | length   |
/// |--------------------------------------|
/// |  index name 3 | start pos | length   |
/// |--------------------------------------|            HEAD
/// |   column 2        |  index number    |
/// |--------------------------------------|
/// |  index name 1 | start pos | length   |
/// |--------------------------------------|
/// |  index name 2 | start pos | length   |
/// |--------------------------------------|
/// |  index name 3 | start pos | length   |
/// |--------------------------------------|
/// |                 ...                  |
/// |--------------------------------------|
/// |                 ...                  |
/// |--------------------------------------|
/// |  redundant length | redundant bytes  |
/// |--------------------------------------|    ---------------------
/// |                BODY                  |
/// |                BODY                  |
/// |                BODY                  |             BODY
/// |                BODY                  |
/// |______________________________________|    _____________________
///
/// magic:               8 bytes long
/// version:             4 bytes int
/// head length:         4 bytes int
/// column number:       4 bytes int
/// column x:            var bytes utf (length + bytes)
/// index number:        4 bytes int (how many column items below)
/// index name x:        var bytes utf
/// start pos:           4 bytes int
/// length:              4 bytes int
/// redundant length:    4 bytes int (for compatibility with later versions, in this version,
///                      content is zero)
/// redundant bytes:     var bytes (for compatibility with later version, in this version, is empty)
/// BODY:                column index bytes + column index bytes + column index bytes + .......
/// </pre>
///
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
    /// @return A unique pointer to a `Reader` on success, or an error if the stream is invalid
    ///         (e.g., wrong magic, unsupported version, or corrupted data).
    static Result<std::unique_ptr<Reader>> CreateReader(
        const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool);

    /// Creates a `Writer` which serializes a complete V1 file index container.
    static Result<std::unique_ptr<Writer>> CreateWriter(
        const std::shared_ptr<OutputStream>& output_stream,
        const std::shared_ptr<MemoryPool>& pool);

 public:
    static const int64_t MAGIC;
    static const int32_t EMPTY_INDEX_FLAG;
    static const int32_t V_1;
};

/// Writer for file index file.
class FileIndexFormat::Writer {
 public:
    virtual ~Writer() = default;

    /// Writes all column indexes. This is a terminal, one-shot operation.
    virtual Status WriteColumnIndexes(const FileIndexFormat::ColumnIndexes& indexes) = 0;

    virtual Status Close() = 0;
};

/// Reader for file index file.
class FileIndexFormat::Reader {
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

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

#include "paimon/common/file_index/file_index_format_utils.h"
#include "paimon/file_index/file_index_format.h"

namespace paimon {
/// V1 container layout and writer implementation.
/// - All numeric fields use big-endian byte order.
/// - Column metadata precedes the index payloads.
/// - Payload offsets and lengths are int32; an empty index uses (-1, 0).
/// - Incremental writes buffer payloads until Finish() can write the header and bodies.
/// - The complete container must fit within INT32_MAX bytes.
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
class FileIndexFormatV1 {
 public:
    FileIndexFormatV1() = delete;
    ~FileIndexFormatV1() = delete;

    class Writer;

    /// Reads index metadata from the header after the magic/version prefix has been consumed.
    static Result<FileIndexFormatUtils::FileIndexEntries> ReadIndexEntries(
        const std::shared_ptr<InputStream>& input, const std::shared_ptr<MemoryPool>& pool,
        int64_t file_length);
};

class FileIndexFormatV1::Writer : public FileIndexFormat::Writer {
 public:
    Writer(const std::shared_ptr<OutputStream>& output, const std::shared_ptr<MemoryPool>& pool);
    Status WriteIndex(const std::string& column, const std::string& type,
                      const PayloadWriter& payload_writer) override;
    Status Finish() override;
    Status Close() override;

 private:
    static constexpr int32_t kRedundantLength = 0;
    static Result<int32_t> CalculateHeadLength(const FileIndexFormat::ColumnIndexes& indexes);
    Status WriteIndexes(const FileIndexFormat::ColumnIndexes& indexes);
    Status WriteHead(const FileIndexFormat::ColumnIndexes& indexes);

    std::shared_ptr<OutputStream> output_stream_;
    std::shared_ptr<MemoryPool> pool_;
    FileIndexFormat::ColumnIndexes buffered_indexes_;
    bool failed_ = false;
    bool finished_ = false;
    bool closed_ = false;
};
}  // namespace paimon

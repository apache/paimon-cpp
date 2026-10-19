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
#include <utility>

#include "paimon/common/file_index/file_index_format_utils.h"
#include "paimon/file_index/file_index_format.h"

namespace paimon {
/// V2 container layout and writer implementation.
/// - All numeric fields use big-endian byte order.
/// - Column metadata follows the index payloads.
/// - Payload offsets and lengths are int64; an empty index uses (-1, 0).
/// - Incremental writes stream payloads directly; Finish() writes the footer and trailer.
/// - The trailer locates the footer, footer_length must fit within INT32_MAX bytes.
///
/// <pre>
///  _______________________________________    _____________________
/// |          magic          | version    |            PREFIX
/// |--------------------------------------|    ---------------------
/// |            index payload 1           |
/// |--------------------------------------|
/// |            index payload 2           |             BODY
/// |--------------------------------------|
/// |                 ...                  |
/// |--------------------------------------|    ---------------------
/// |            column number             |
/// |--------------------------------------|
/// |   column 1        |  index number    |
/// |--------------------------------------|
/// |  index name 1 | start pos | length   |
/// |--------------------------------------|
/// |  index name 2 | start pos | length   |
/// |--------------------------------------|
/// |                 ...                  |            FOOTER
/// |--------------------------------------|
/// |   column 2        |  index number    |
/// |--------------------------------------|
/// |  index name 1 | start pos | length   |
/// |--------------------------------------|
/// |                 ...                  |
/// |--------------------------------------|    ---------------------
/// |     footer length     | tail magic   |            TRAILER
/// |______________________________________|    _____________________
///
/// magic:               8 bytes long
/// version:             4 bytes int (2)
/// column number:       4 bytes int
/// column x:            var bytes utf (length + bytes)
/// index number:        4 bytes int (number of indexes for the column)
/// index name x:        var bytes utf (index type)
/// start pos:           8 bytes long (absolute payload offset)
/// length:              8 bytes long (payload length)
/// footer length:       4 bytes int (footer bytes only, excludes trailer)
/// tail magic:          8 bytes long (0x5041464944583032, "PAFIDX02")
/// BODY:                serialized index payloads
/// </pre>
///
class FileIndexFormatV2 {
 public:
    FileIndexFormatV2() = delete;
    ~FileIndexFormatV2() = delete;

    class Writer;

    /// Reads the directory after the magic/version prefix has been consumed.
    static Result<FileIndexEntries> ReadIndexEntries(const std::shared_ptr<InputStream>& input,
                                                     const std::shared_ptr<MemoryPool>& pool,
                                                     int64_t file_length);
};

class FileIndexFormatV2::Writer : public FileIndexFormat::Writer {
 public:
    static Result<std::unique_ptr<Writer>> Create(const std::shared_ptr<OutputStream>& output);
    Status Close() override;
    Status WriteIndex(const std::string& column, const std::string& type,
                      const PayloadWriter& payload) override;
    Status Finish() override;

 private:
    explicit Writer(const std::shared_ptr<OutputStream>& output);

    std::shared_ptr<OutputStream> output_;
    std::map<std::string, std::map<std::string, std::pair<int64_t, int64_t>>> entries_;
    bool failed_ = false;
    bool finished_ = false;
    bool closed_ = false;
};
}  // namespace paimon

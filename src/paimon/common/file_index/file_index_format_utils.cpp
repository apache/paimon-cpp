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

#include "paimon/common/file_index/file_index_format_utils.h"

#include "paimon/common/io/offset_input_stream.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/io/buffered_input_stream.h"
#include "paimon/io/data_input_stream.h"

namespace paimon {
Result<std::shared_ptr<InputStream>> FileIndexFormatUtils::CreateFileIndexDirectory(
    const std::shared_ptr<InputStream>& input, const std::shared_ptr<MemoryPool>& pool,
    int64_t start, int64_t length, int64_t file_length) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OffsetInputStream> bounded,
                           OffsetInputStream::Create(input, length, start, file_length));
    return std::make_shared<BufferedInputStream>(bounded, BufferedInputStream::DEFAULT_BUFFER_SIZE,
                                                 pool.get());
}

Result<FileIndexEntries> FileIndexFormatUtils::ReadFileIndexEntries(const DataInputStream& in,
                                                                    int32_t version) {
    PAIMON_ASSIGN_OR_RAISE(int32_t column_size, in.ReadValue<int32_t>());
    FileIndexEntries header;
    for (int32_t i = 0; i < column_size; ++i) {
        PAIMON_ASSIGN_OR_RAISE(std::string column, in.ReadString());
        PAIMON_ASSIGN_OR_RAISE(int32_t index_size, in.ReadValue<int32_t>());
        auto& entries = header[column];
        for (int32_t j = 0; j < index_size; ++j) {
            PAIMON_ASSIGN_OR_RAISE(std::string type, in.ReadString());
            int64_t offset;
            int64_t length;
            if (version == FileIndexFormat::V_1) {
                PAIMON_ASSIGN_OR_RAISE(offset, in.ReadValue<int32_t>());
                PAIMON_ASSIGN_OR_RAISE(length, in.ReadValue<int32_t>());
            } else {
                PAIMON_ASSIGN_OR_RAISE(offset, in.ReadValue<int64_t>());
                PAIMON_ASSIGN_OR_RAISE(length, in.ReadValue<int64_t>());
            }
            entries[type] = {offset, length};
        }
    }
    return header;
}

}  // namespace paimon

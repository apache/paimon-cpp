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

#include "paimon/file_index/file_index_writer.h"

#include "paimon/fs/file_system.h"
#include "paimon/status.h"

namespace paimon {
Result<PAIMON_UNIQUE_PTR<Bytes>> FileIndexWriter::SerializedBytes() const {
    return Status::NotImplemented("This file index writer requires streaming output");
}

Result<bool> FileIndexWriter::WriteTo(const std::shared_ptr<OutputStream>& output) const {
    if (!output) {
        return Status::Invalid("File index output stream cannot be null");
    }
    PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> bytes, SerializedBytes());
    if (!bytes || bytes->size() == 0) {
        return false;
    }
    auto length = static_cast<int64_t>(bytes->size());
    PAIMON_ASSIGN_OR_RAISE(int64_t written, output->Write(bytes->data(), length));
    if (written != length) {
        return Status::IOError("Short write for file index payload");
    }
    return true;
}
}  // namespace paimon

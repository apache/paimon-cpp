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

#include "paimon/common/io/offset_output_stream.h"

#include "paimon/macros.h"

namespace paimon {
Result<std::unique_ptr<OffsetOutputStream>> OffsetOutputStream::Create(OutputStream* output) {
    PAIMON_ASSIGN_OR_RAISE(int64_t start, output->GetPos());
    return std::unique_ptr<OffsetOutputStream>(new OffsetOutputStream(output, start));
}

OffsetOutputStream::OffsetOutputStream(OutputStream* output, int64_t start)
    : output_(output), start_(start) {}

Result<int64_t> OffsetOutputStream::Write(const char* buffer, int64_t size) {
    return output_->Write(buffer, size);
}

Result<int64_t> OffsetOutputStream::GetPos() const {
    PAIMON_ASSIGN_OR_RAISE(int64_t position, output_->GetPos());
    return position - start_;
}

Result<std::string> OffsetOutputStream::GetUri() const {
    return output_->GetUri();
}

Status OffsetOutputStream::Flush() {
    return output_->Flush();
}

Status OffsetOutputStream::Close() {
    return Status::OK();
}
}  // namespace paimon

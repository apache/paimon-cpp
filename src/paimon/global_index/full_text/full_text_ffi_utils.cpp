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

#include "paimon/global_index/full_text/full_text_ffi_utils.h"

#include "fmt/format.h"
#include "paimon/common/utils/string_utils.h"

namespace paimon::full_text {

namespace {
constexpr char kNativeIoErrorPrefix[] = "io error:";
}  // namespace

Status LastFtindexError(const std::string& action, const Status& callback_error) {
    if (!callback_error.ok()) {
        return callback_error.WithMessage("failed to ", action, ": ", callback_error.message());
    }
    const char* native_error = paimon_ftindex_last_error();
    std::string message = native_error != nullptr ? native_error : "unknown native error";
    std::string full_message = fmt::format("failed to {}: {}", action, message);
    if (StringUtils::StartsWith(message, kNativeIoErrorPrefix)) {
        return Status::IOError(full_message);
    }
    return Status::Invalid(full_message);
}

}  // namespace paimon::full_text

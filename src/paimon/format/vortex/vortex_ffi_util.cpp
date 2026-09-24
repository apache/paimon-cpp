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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/format/vortex/vortex_ffi_util.h"

namespace paimon::vortex {

Status VortexFfiError(const std::string& operation, vx_error* error) {
    if (error == nullptr) {
        return Status::OK();
    }
    const vx_string* message = vx_error_get_message(error);
    const char* ptr = message == nullptr ? nullptr : vx_string_ptr(message);
    const size_t len = message == nullptr ? 0 : vx_string_len(message);
    std::string text = ptr == nullptr ? std::string() : std::string(ptr, len);
    vx_error_free(error);
    return Status::Invalid(operation, ": ", text);
}

}  // namespace paimon::vortex

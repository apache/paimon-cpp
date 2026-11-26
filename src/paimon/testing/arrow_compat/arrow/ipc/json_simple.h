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

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/json/from_string.h"
#include "arrow/result.h"

// Keep existing JSON fixture calls working with Arrow 26's public JSON API.
namespace arrow::ipc::internal::json {

inline Result<std::shared_ptr<Array>> ArrayFromJSON(const std::shared_ptr<DataType>& type,
                                                    std::string_view json) {
    return ::arrow::json::ArrayFromJSONString(type, json);
}

inline Status ChunkedArrayFromJSON(const std::shared_ptr<DataType>& type,
                                   const std::vector<std::string>& chunks,
                                   std::shared_ptr<ChunkedArray>* out) {
    return ::arrow::json::ChunkedArrayFromJSONString(type, chunks).Value(out);
}

}  // namespace arrow::ipc::internal::json

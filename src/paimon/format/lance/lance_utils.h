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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "paimon/status.h"

namespace paimon::lance {

inline constexpr char kLanceStorageOptionPrefix[] = "lance.storage.";
inline constexpr char kLanceBatchReadahead[] = "lance.read.batch-readahead";
inline constexpr uint32_t kDefaultLanceBatchReadahead = 1;

using LanceStorageOptions = std::vector<std::pair<std::string, std::string>>;

LanceStorageOptions GetLanceStorageOptions(const std::map<std::string, std::string>& options,
                                           const std::string& uri);

Status LanceFfiError(const std::string& operation);

}  // namespace paimon::lance

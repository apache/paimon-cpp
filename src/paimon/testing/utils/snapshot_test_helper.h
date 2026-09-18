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
#include <optional>
#include <string>

#include "paimon/core/snapshot.h"

namespace paimon::test {

inline Snapshot BuildTestSnapshot(int64_t id,
                                  const std::optional<std::string>& uuid = std::nullopt) {
    return Snapshot(Snapshot::CURRENT_VERSION, id, 0,
                    "manifest-list-3879e56f-2f27-49ae-a2f3-3dcbb8eb0beb-0", 291,
                    "manifest-list-3879e56f-2f27-49ae-a2f3-3dcbb8eb0beb-1", 1342, std::nullopt,
                    std::nullopt, std::nullopt, "commit_user_1", 9223372036854775807,
                    Snapshot::CommitKind::Append(), 1758097357597, 5, 5, 0, std::nullopt,
                    std::nullopt, std::nullopt, 0, uuid);
}

}  // namespace paimon::test

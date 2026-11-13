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
#include <string>

#include "paimon/table/source/data_split.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "paimon/visibility.h"

namespace paimon {

/// Zero-based physical file positions, never renumbered after filtering.
struct PAIMON_EXPORT FileRowSelection {
    DataSplit::SimpleDataFileMeta file;
    /// String partition values; NULL uses partition.default-name.
    std::map<std::string, std::string> partition;
    int32_t bucket = 0;
    RoaringBitmap32 positions;
};

}  // namespace paimon

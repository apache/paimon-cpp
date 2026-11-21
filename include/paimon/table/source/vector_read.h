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

#pragma once

#include <memory>

#include "paimon/global_index/global_index_result.h"
#include "paimon/result.h"
#include "paimon/table/source/vector_scan.h"
#include "paimon/visibility.h"

namespace paimon {
/// Searches a vector plan from the same table, branch and field. The plan supplies the snapshot
/// and filters. Each read supplies its query, limit, oversampling and refinement settings.
class PAIMON_EXPORT VectorRead {
 public:
    virtual ~VectorRead() = default;
    virtual Result<std::shared_ptr<ScoredGlobalIndexResult>> Read(
        const VectorScan::Plan& plan) const = 0;
};

}  // namespace paimon

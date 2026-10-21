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

#include "paimon/table/source/table_scan.h"

#include "paimon/common/metrics/metrics_impl.h"

namespace paimon {

std::shared_ptr<Metrics> TableScan::GetMetrics() const {
    return std::make_shared<MetricsImpl>();
}

Result<std::vector<std::map<std::string, std::string>>> TableScan::ListPartitions() const {
    // A managed table's partitions live in its manifests, and reading them is a scan of its own
    // rather than a listing, so only the format table path answers this for now.
    return Status::NotImplemented("this table does not list its partitions");
}

}  // namespace paimon

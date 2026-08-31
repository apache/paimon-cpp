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

#include "paimon/visibility.h"

namespace paimon {

/// Metric names for scan planning operations.
class PAIMON_EXPORT ScanMetrics {
 public:
    static constexpr char LAST_SCAN_DURATION[] = "lastScanDuration";
    // Histogram metric for scan plan duration (milliseconds).
    static constexpr char SCAN_DURATION[] = "scanDuration";
    static constexpr char LAST_SCANNED_SNAPSHOT_ID[] = "lastScannedSnapshotId";
    static constexpr char LAST_SCANNED_MANIFESTS[] = "lastScannedManifests";
    static constexpr char LAST_SCAN_SKIPPED_TABLE_FILES[] = "lastScanSkippedTableFiles";
    static constexpr char LAST_SCAN_RESULTED_TABLE_FILES[] = "lastScanResultedTableFiles";

    // The metrics below are C++-only and do not have counterparts in Java ScanMetrics.
    static constexpr char LAST_MANIFEST_READ_DURATION[] = "lastManifestReadDuration";
    // Histogram metric for manifest-list and manifest-entry read duration (milliseconds).
    static constexpr char MANIFEST_READ_DURATION[] = "manifestReadDuration";
    static constexpr char LAST_SNAPSHOT_CACHE_ENABLED[] = "lastSnapshotCacheEnabled";
    static constexpr char LAST_SNAPSHOT_CACHE_HIT[] = "lastSnapshotCacheHit";
    static constexpr char SNAPSHOT_CACHE_HITS[] = "snapshotCacheHits";
    static constexpr char SNAPSHOT_CACHE_MISSES[] = "snapshotCacheMisses";
    static constexpr char LAST_SNAPSHOT_CACHE_LOAD_DURATION[] = "lastSnapshotCacheLoadDuration";
    static constexpr char SNAPSHOT_CACHE_LOAD_DURATION[] = "snapshotCacheLoadDuration";
    static constexpr char LAST_SNAPSHOT_CACHE_STORE_DURATION[] = "lastSnapshotCacheStoreDuration";
    static constexpr char SNAPSHOT_CACHE_STORE_DURATION[] = "snapshotCacheStoreDuration";
    // Candidate manifest-entry rows inspected by lazy scan filtering.
    static constexpr char LAST_LAZY_DECODE_SCANNED_ROWS[] = "lastLazyDecodeScannedRows";
    // Full manifest entries retained after lazy scan filtering.
    static constexpr char LAST_LAZY_DECODE_MATERIALIZED_ROWS[] = "lastLazyDecodeMaterializedRows";
};

}  // namespace paimon

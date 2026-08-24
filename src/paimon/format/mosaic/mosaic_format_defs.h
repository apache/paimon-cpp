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

#pragma once

namespace paimon::mosaic {

static inline const char MOSAIC_NUM_BUCKETS[] = "mosaic.num-buckets";

/// Max dict size per column.
static inline const char MOSAIC_MAX_DICT_TOTAL_BYTES[] = "mosaic.max-dict-total-bytes";

/// Max dict entries per column.
static inline const char MOSAIC_MAX_DICT_ENTRIES[] = "mosaic.max-dict-entries";

/// Min avg column page size to enable paged mode.
static inline const char MOSAIC_PAGE_SIZE_THRESHOLD[] = "mosaic.page-size-threshold";

/// Comma-separated list of column names to collect statistics for. Empty means no statistics
/// collection.
static inline const char MOSAIC_STATS_COLUMNS[] = "mosaic.stats-columns";

}  // namespace paimon::mosaic

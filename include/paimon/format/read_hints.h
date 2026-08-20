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

/// Runtime state of the framework read path, passed to format layers via
/// `ReaderBuilder::WithReadHints` so each format can adapt its internal behavior
/// (e.g. whether parquet enables its own pre-buffering).
struct PAIMON_EXPORT ReadHints {
    /// Whether framework-level prefetch is enabled for this read.
    bool prefetch_enabled = false;
    /// Whether the shared read-ahead cache is enabled for this read.
    bool read_ahead_cache_enabled = false;
};

}  // namespace paimon

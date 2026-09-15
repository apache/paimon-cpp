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

#include <cstdint>
#include <memory>

#include "paimon/executor.h"

namespace paimon {

/// Number of data file readers built in parallel within one split, and the size of the dedicated
/// reader build pool (clamped to hardware concurrency). Building a reader mostly waits on remote
/// I/O, so a small fixed pool overlaps those waits without unbounded thread growth.
inline constexpr uint32_t kReaderBuildMaxParallelNum = 4;

/// Returns the process-wide thread pool dedicated to building data file readers.
///
/// This pool is kept separate from the executor of a read context on purpose. That executor is
/// handed to the readers this pool builds, and they use it for async prefetch once reading
/// starts; running the per-file builds on a dedicated pool keeps the blocking build phase from
/// contending with it, and keeps the pool a single reused instance instead of a fresh one per
/// split.
///
/// The pool is created once, on first use, sized to `kReaderBuildMaxParallelNum` clamped to
/// hardware concurrency. Callers that build readers serially never call this and therefore never
/// pay for any thread.
std::shared_ptr<Executor> GetReaderBuildExecutor();

}  // namespace paimon

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

/// Returns the process-wide thread pool dedicated to building data file readers.
///
/// This pool must stay separate from the executor of a read context: building one reader
/// submits sub-reader tasks to the read context executor and blocks until they finish, so
/// reusing that same pool for the outer per-file tasks would starve the inner tasks and
/// deadlock. With a dedicated pool the wait graph is one way, reader building waits on the
/// read context executor and never the other way around.
///
/// The pool is created on first use, hence the first caller decides its size:
/// `thread_count` clamped to [1, hardware concurrency]. Callers that build readers
/// serially never call this and therefore never pay for any thread.
std::shared_ptr<Executor> GetReaderBuildExecutor(uint32_t thread_count);

}  // namespace paimon

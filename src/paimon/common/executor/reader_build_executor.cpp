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

#include "paimon/common/executor/reader_build_executor.h"

#include <algorithm>
#include <mutex>
#include <thread>

#include "paimon/result.h"

namespace paimon {

std::shared_ptr<Executor> GetReaderBuildExecutor(uint32_t thread_count) {
    static std::shared_ptr<Executor> executor;
    static std::once_flag once;
    std::call_once(once, [thread_count]() {
        const uint32_t hardware_threads = std::thread::hardware_concurrency();
        const uint32_t upper_bound = hardware_threads > 0 ? hardware_threads : 1;
        const uint32_t threads = std::clamp(thread_count, 1u, upper_bound);
        // CreateDefaultExecutor only rejects a zero thread count, which the clamp rules out.
        executor = CreateDefaultExecutor(threads).value();
    });
    return executor;
}

}  // namespace paimon

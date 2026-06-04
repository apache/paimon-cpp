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

#include <cassert>
#include <functional>
#include <future>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "paimon/executor.h"

namespace paimon {

/// Submits a function to be executed asynchronously on a given executor and returns a future.
///
/// This function wraps the provided callable and submits it to the provided executor. The function
/// captures the result using a std::promise, which is used to fulfill the returned std::future. If
/// the callable throws an exception, the exception is captured and set in the promise.
template <typename Func>
auto Via(Executor* executor, Func&& func) -> std::future<std::invoke_result_t<Func>> {
    using ResultType = std::invoke_result_t<Func>;

    static_assert(std::is_invocable_v<Func>, "func must be callable");

    if constexpr (std::is_constructible_v<std::function<void()>, Func>) {
        std::function<void()> test_func = func;
        if (!test_func) {
            assert(false && "func cannot be an empty std::function");
        }
    }

    auto promise = std::make_shared<std::promise<ResultType>>();
    auto future = promise->get_future();

    executor->Add([promise, func = std::forward<Func>(func)]() mutable {
        try {
            if constexpr (std::is_void_v<ResultType>) {
                func();
                promise->set_value();
            } else {
                promise->set_value(func());
            }
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    });

    return future;
}

/// Collects the results of multiple futures.
template <typename T>
std::vector<T> CollectAll(std::vector<std::future<T>>& futures) {
    std::vector<T> results;
    results.reserve(futures.size());
    for (auto& future : futures) {
        results.push_back(future.get());
    }

    return results;
}

/// Waits for all futures with void return type to complete.
inline void Wait(std::vector<std::future<void>>& futures) {
    for (auto& future : futures) {
        if (future.valid()) {
            future.get();
        }
    }
}

}  // namespace paimon

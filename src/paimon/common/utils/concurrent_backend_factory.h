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

#include <functional>
#include <memory>
#include <mutex>
#include <utility>

namespace paimon {

/// Stores one statically linked creator for a concrete concurrent backend interface.
///
/// A plugin registers a creator for each backend specialization it implements. Registration is
/// effective only when Paimon is built without TBB; TBB-backed containers do not query this
/// factory. The first registration for a specialization wins. The registration and its container
/// specialization must be linked into the same executable or shared library. When registration is
/// packaged in a static archive, its object file must be retained by the final link (for example,
/// by referencing a symbol from it or linking the archive whole).
template <typename Backend>
class ConcurrentBackendFactory {
 public:
    using Creator = std::function<std::unique_ptr<Backend>()>;

    ConcurrentBackendFactory() = delete;
    ~ConcurrentBackendFactory() = delete;

    static bool Register(Creator creator) {
        std::lock_guard<std::mutex> lock(GetMutex());
        Creator& registered_creator = GetCreator();
        if (registered_creator) {
            return false;
        }
        registered_creator = std::move(creator);
        return true;
    }

    static std::unique_ptr<Backend> Create() {
        Creator creator;
        {
            std::lock_guard<std::mutex> lock(GetMutex());
            creator = GetCreator();
        }
        return creator ? creator() : nullptr;
    }

 private:
    static Creator& GetCreator() {
        static Creator creator;
        return creator;
    }

    static std::mutex& GetMutex() {
        static std::mutex mutex;
        return mutex;
    }
};

}  // namespace paimon

/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Adapted from Alibaba Havenask
// https://github.com/alibaba/havenask/blob/main/aios/storage/indexlib/util/Singleton.h

#pragma once

#include <atomic>
#include <memory>
#include <mutex>

#include "paimon/macros.h"
#include "paimon/visibility.h"

namespace paimon {

class PAIMON_EXPORT LazyInstantiation {
 protected:
    template <typename T>
    static void Create(T*& ptr) {
        // Publication ordering is handled by the release store in
        // Singleton<T, InstPolicy>::GetInstance(), so no barrier is needed here.
        ptr = new T;
        static std::shared_ptr<T> destroyer(ptr);
    }
};

/// A singleton implementation with customizable instantiation policy.
template <typename T, typename InstPolicy = LazyInstantiation>
class PAIMON_EXPORT Singleton : private InstPolicy {
 protected:
    Singleton(const Singleton&) {}
    Singleton() = default;

 public:
    ~Singleton() = default;

 public:
    /// Provide access to the single instance through double-checked locking.
    ///
    /// Lazy create a singleton instance when `GetInstance()` is called.
    ///
    /// @return The single instance of object.
    static T* GetInstance();
};

template <typename T, typename InstPolicy>
T* Singleton<T, InstPolicy>::GetInstance() {
    static std::atomic<T*> ptr{nullptr};
    static std::mutex mutex;
    T* p = ptr.load(std::memory_order_acquire);
    if (PAIMON_UNLIKELY(p == nullptr)) {
        std::lock_guard<std::mutex> lg(mutex);
        // Re-check under the mutex with a relaxed load; the mutex already
        // synchronizes with the creating thread.
        p = ptr.load(std::memory_order_relaxed);
        if (p == nullptr) {
            InstPolicy::Create(p);
            ptr.store(p, std::memory_order_release);
        }
    }
    return p;
}

// FactoryCreator and IOHook are instantiated exactly once in singleton.cpp, and the
// extern declarations below suppress implicit instantiation everywhere else. The
// file-format/file-system plugins are separate shared libraries linked with
// -Bsymbolic, so a per-library copy of GetInstance()'s function-local static state
// would never be interposed: factory registrations would land in a different
// instance than lookups. Do not replace these with implicit instantiation. Types local to a single
// translation unit (e.g. test-only types) can still instantiate Singleton<T>
// implicitly because they cannot span library boundaries.
class FactoryCreator;
class IOHook;
extern template class Singleton<FactoryCreator>;
extern template class Singleton<IOHook>;

}  // namespace paimon

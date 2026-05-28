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

#include <memory>

#include "paimon/macros.h"
#include "paimon/visibility.h"

namespace paimon {

class PAIMON_EXPORT LazyInstantiation {
 protected:
    template <typename T>
    static void Create(T*& ptr) {
        T* tmp = new T;
        MEMORY_BARRIER();
        ptr = tmp;
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

}  // namespace paimon

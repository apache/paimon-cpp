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

#include "paimon/factories/singleton.h"

#include <atomic>
#include <mutex>

#include "paimon/common/factories/io_hook.h"
#include "paimon/factories/factory_creator.h"

namespace paimon {

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

template class Singleton<FactoryCreator>;
template class Singleton<IOHook>;

}  // namespace paimon

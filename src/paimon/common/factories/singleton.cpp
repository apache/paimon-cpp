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

#include <mutex>

#include "paimon/common/factories/io_hook.h"
#include "paimon/factories/factory_creator.h"

namespace paimon {

template <typename T, typename InstPolicy>
T* Singleton<T, InstPolicy>::GetInstance() {
    static T* ptr;
    static std::mutex mutex;
    if (PAIMON_UNLIKELY(!ptr)) {
        std::lock_guard<std::mutex> lg(mutex);
        if (!ptr) {
            InstPolicy::Create(ptr);
        }
    }
    return const_cast<T*>(ptr);
}

template class Singleton<FactoryCreator>;
template class Singleton<IOHook>;

}  // namespace paimon

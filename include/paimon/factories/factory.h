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

#include "paimon/factories/factory_creator.h"
#include "paimon/visibility.h"

namespace paimon {
/// Factory used to register, e.g., `FileFormatFactory`, `FileSystemFactory`.
/// Call `REGISTER_PAIMON_FACTORY` to register a factory to `FactoryCreator`.
class PAIMON_EXPORT Factory {
 public:
    virtual ~Factory() = default;
    virtual const char* Identifier() const = 0;
};

}  // namespace paimon

#define REGISTER_PAIMON_FACTORY(PAIMON_FACTORY)                                    \
    static __attribute__((constructor)) void Register##PAIMON_FACTORY##Factory() { \
        auto factory_creator = paimon::FactoryCreator::GetInstance();              \
        auto* factory = new PAIMON_FACTORY;                                        \
        factory_creator->Register(factory->Identifier(), factory);                 \
    }

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

#include "paimon/realtime/index/realtime_index_factory.h"

#include "paimon/core/realtime/index/hash/hash_realtime_index.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/realtime/index/realtime_index.h"
#include "paimon/status.h"

namespace paimon {
const char RealtimeIndexFactory::REALTIME_INDEX_IDENTIFIER_SUFFIX[] = "-realtime";

Result<std::shared_ptr<RealtimeIndex>> RealtimeIndexFactory::Get(
    const std::string& identifier, ::ArrowSchema* write_schema,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool) {
    FactoryCreator* creator = FactoryCreator::GetInstance();
    if (!creator) {
        return Status::Invalid("factory creator is null pointer");
    }
    auto* factory = dynamic_cast<RealtimeIndexFactory*>(
        creator->Create(identifier + REALTIME_INDEX_IDENTIFIER_SUFFIX));
    if (!factory) {
        return Status::Invalid("unknown real-time index type: ", identifier);
    }
    return factory->Create(write_schema, options, pool);
}

// Keep built-in registration with the lookup entry point for static linking.
REGISTER_PAIMON_FACTORY(HashRealtimeIndexFactory);

}  // namespace paimon

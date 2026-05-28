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

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "paimon/factories/singleton.h"
#include "paimon/visibility.h"

namespace paimon {

class Factory;

/// Store a map from identifier to factory.
/// After registration, you can get the registered factory by calling the `Create()` method.
class PAIMON_EXPORT FactoryCreator : public Singleton<FactoryCreator> {
 public:
    FactoryCreator() = default;
    ~FactoryCreator();

    /// Register a factory with the given identifier.
    void Register(const std::string& type, Factory* factory);

    /// Create a factory with the given identifier.
    Factory* Create(const std::string& type) const;

    /// Get all registered types.
    std::vector<std::string> GetRegisteredType() const;

 private:
    /// @note For test only.
    void TEST_Unregister(const std::string& type);

    std::map<std::string, Factory*> factories_;

    mutable std::mutex mutex_;
};

}  // namespace paimon

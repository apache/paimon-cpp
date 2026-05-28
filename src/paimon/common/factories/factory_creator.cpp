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

#include "paimon/factories/factory_creator.h"

#include <cstdlib>
#include <iostream>
#include <utility>

#include "paimon/factories/factory.h"

namespace paimon {

FactoryCreator::~FactoryCreator() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto iter : factories_) {
        delete iter.second;
        iter.second = nullptr;
    }
}

Factory* FactoryCreator::Create(const std::string& type) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = factories_.find(type);
    if (iter == factories_.end()) {
        return nullptr;
    }
    return iter->second;
}

void FactoryCreator::Register(const std::string& type, Factory* factory) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = factories_.find(type);
    if (iter != factories_.end()) {
        std::cerr << "register conflict: type " << type << " already exist" << std::endl;
        std::abort();
    }
    factories_[type] = factory;
}

void FactoryCreator::TEST_Unregister(const std::string& type) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = factories_.find(type);
    if (iter != factories_.end()) {
        delete iter->second;
        factories_.erase(iter);
    }
}

std::vector<std::string> FactoryCreator::GetRegisteredType() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> types;
    for (const auto& kv : factories_) {
        types.push_back(kv.first);
    }
    return types;
}

}  // namespace paimon

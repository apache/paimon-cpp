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

#include "paimon/fs/credential_provider_factory.h"

#include "fmt/format.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/status.h"

namespace paimon {

Result<std::shared_ptr<CredentialProvider>> CredentialProviderFactory::Get(
    const std::string& identifier, const std::string& path,
    const std::map<std::string, std::string>& fs_options) {
    auto factory_creator = FactoryCreator::GetInstance();
    auto factory = factory_creator->Create(identifier);
    if (factory == nullptr) {
        return Status::Invalid(
            fmt::format("Create factory failed with identifier '{}'.", identifier));
    }
    auto credential_provider_factory = dynamic_cast<CredentialProviderFactory*>(factory);
    if (credential_provider_factory == nullptr) {
        return Status::Invalid(fmt::format(
            "Failed to cast credential provider factory with identifier '{}'.", identifier));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CredentialProvider> provider,
                           credential_provider_factory->Create(path, fs_options));
    if (provider == nullptr) {
        return Status::Invalid(
            fmt::format("Credential provider factory '{}' created a null provider.", identifier));
    }
    return provider;
}

}  // namespace paimon

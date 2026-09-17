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
#include <memory>
#include <string>

#include "paimon/factories/factory.h"
#include "paimon/fs/credential_provider.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

/// A factory for creating `CredentialProvider` instances.
///
/// Register an implementation with `REGISTER_PAIMON_FACTORY` to build a provider that a
/// file system of your own consults: get it with `Get`, call `GetCredentials` as you sign
/// each access, and pass that file system in through `Catalog::Create` or a builder's
/// `WithFileSystem`. The built-in file systems do not consult a provider; they sign with
/// the static credentials of their own options.
///
/// @note All factories share one identifier space, so an identifier that a file system
/// factory already takes - "oss", "s3", "local", "jindo" - would replace it. Name the
/// provider after where its credentials come from instead.
class PAIMON_EXPORT CredentialProviderFactory : public Factory {
 public:
    /// Create a `CredentialProvider` of current factory for the accesses below a path.
    ///
    /// The options are the file system options of the accesses to authenticate, so an
    /// implementation reads its own configuration out of them.
    virtual Result<std::shared_ptr<CredentialProvider>> Create(
        const std::string& path, const std::map<std::string, std::string>& options) const = 0;

    /// Get a `CredentialProvider` corresponding to identifier for the accesses below a path.
    /// @pre Factory is already registered.
    static Result<std::shared_ptr<CredentialProvider>> Get(
        const std::string& identifier, const std::string& path,
        const std::map<std::string, std::string>& fs_options);
};

}  // namespace paimon

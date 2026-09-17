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
#include <string>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

/// The source of the credentials a file system authenticates its accesses with.
///
/// Implement this to hand out credentials that expire: the provider is asked again at
/// every access, so it can reload them before they expire without the file system being
/// torn down and built again.
///
/// A provider is for a caller that brings its own `FileSystem`. Build one with
/// `CredentialProviderFactory::Get`, consult it from every access of your file system, and
/// pass that file system in through `Catalog::Create`, `ReadContextBuilder::WithFileSystem`,
/// `ScanContextBuilder::WithFileSystem` or `WriteContextBuilder::WithFileSystem`. A file
/// system passed this way is used as-is; the built-in file systems are not involved and
/// authenticate with the static credentials of their own options.
///
/// An implementation is consulted from every thread that accesses the file system and
/// has to be thread-safe.
class PAIMON_EXPORT CredentialProvider {
 public:
    virtual ~CredentialProvider() = default;

    /// Returns the credentials to sign an access with, reloading them when they are about
    /// to expire. The keys are file system options, e.g. "fs.oss.accessKeyId" or
    /// "fs.oss.securityToken", so a caller merges them over its own file system options.
    virtual Result<std::map<std::string, std::string>> GetCredentials() const = 0;
};

}  // namespace paimon

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
    /// to expire. The keys are file system options, so a caller merges them over its own
    /// file system options with `MergeOptionsWithCredentials`.
    virtual Result<std::map<std::string, std::string>> GetCredentials() const = 0;

    /// Merges the issued credentials into the file system options a delegate is built from.
    /// This is the canonical way to shape credentials into the options of an access: a
    /// caller that brings its own file system calls it so the credentials are applied the
    /// same way the built-in data token file system applies them.
    ///
    /// The default overlays the credentials key by key over `base_options`, so they win
    /// wherever they overlap, mirroring the Java client and staying scheme-agnostic. A
    /// provider that knows the file system its credentials are for overrides this to
    /// normalize option aliases or clear stale bucket-scoped variants the credentials
    /// replace.
    virtual std::map<std::string, std::string> MergeOptionsWithCredentials(
        const std::map<std::string, std::string>& base_options,
        const std::map<std::string, std::string>& credentials) const {
        std::map<std::string, std::string> merged = base_options;
        for (const auto& [key, value] : credentials) {
            merged[key] = value;
        }
        return merged;
    }
};

}  // namespace paimon

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

#pragma once
#include <map>
#include <string>

namespace paimon {

/// What a dependency table - the blob table behind a BlobView - is read through. Like the Java
/// `CatalogContext` that `CatalogEnvironment.dependencyReadContext()` returns, it carries no file
/// system. The catalog built from these options issues credentials for the table about to be read,
/// whereas a file system handed down from the read above authenticates as a different table - and
/// the `X-Paimon-Read-Via` header travelling with it cannot excuse that, being request context a
/// server must not treat as authorization proof. A caller supplying its own file system therefore
/// gets no blob view delegation.
struct CatalogContext {
    CatalogContext(const std::string& _root_path,
                   const std::map<std::string, std::string>& _options,
                   const std::map<std::string, std::string>& _fs_scheme_to_identifier_map)
        : root_path(_root_path),
          options(_options),
          fs_scheme_to_identifier_map(_fs_scheme_to_identifier_map) {}

    std::string root_path;
    std::map<std::string, std::string> options;
    /// Maps a URI scheme to the registered file system serving it, the map the file system of the
    /// read this descends from was resolved with. The catalog builds its own file system out of
    /// these options and needs the map to route a scheme that is not their default.
    std::map<std::string, std::string> fs_scheme_to_identifier_map;
};

}  // namespace paimon

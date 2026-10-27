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

#include "paimon/catalog/catalog.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/catalog_options.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/catalog/file_system_catalog.h"
#include "paimon/core/core_options.h"
#include "paimon/schema/schema.h"
#ifdef PAIMON_ENABLE_REST
#include "paimon/rest/rest_catalog.h"
#endif

namespace paimon {

const char Catalog::SYSTEM_DATABASE_NAME[] = "sys";
const char Catalog::SYSTEM_TABLE_SPLITTER[] = "$";
const char Catalog::DB_SUFFIX[] = ".db";
const char Catalog::DB_LOCATION_PROP[] = "location";

Result<std::shared_ptr<Catalog>> Catalog::Create(
    const std::string& root_path, const std::map<std::string, std::string>& options,
    const std::shared_ptr<FileSystem>& file_system,
    const std::map<std::string, std::string>& fs_scheme_to_identifier_map) {
    std::string metastore = "filesystem";
    auto metastore_iter = options.find(CatalogOptions::METASTORE);
    if (metastore_iter != options.end()) {
        // Matched leniently in lower case; the Java catalog factory looks the metastore
        // up by its exact identifier, so only the exact spelling is portable.
        metastore = StringUtils::ToLowerCase(metastore_iter->second);
    }
    if (metastore == "rest") {
#ifdef PAIMON_ENABLE_REST
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<RestCatalog> rest_catalog,
            RestCatalog::Create(root_path, options, file_system, RestHttpClient::Config(),
                                fs_scheme_to_identifier_map));
        // Built from the derived type so that the `enable_shared_from_this` base a
        // `RestCatalog` carries is initialized: a `sys` table keeps its catalog alive through
        // `weak_from_this()`, which stays empty when the pointer is widened first.
        return std::shared_ptr<Catalog>(std::move(rest_catalog));
#else
        return Status::NotImplemented(
            "the rest catalog requires building paimon with PAIMON_ENABLE_REST=ON");
#endif
    }
    if (metastore != "filesystem") {
        return Status::Invalid("unsupported metastore: ", metastore);
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(options, file_system, fs_scheme_to_identifier_map));
    return std::shared_ptr<Catalog>(
        std::make_unique<FileSystemCatalog>(core_options.GetFileSystem(), root_path, options));
}

bool Catalog::SupportsVersionManagement() const {
    return false;
}

}  // namespace paimon

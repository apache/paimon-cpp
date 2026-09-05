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

#include "paimon/visibility.h"

namespace paimon {

/// Catalog-level configuration option keys; table-level keys live in `Options`.
struct PAIMON_EXPORT CatalogOptions {
    /// "metastore" - Metastore of the paimon catalog.
    /// Supported values are "filesystem" (default) and "rest".
    static const char METASTORE[];

    /// "uri" - Server url of the REST catalog. Only used when METASTORE is "rest".
    static const char URI[];

    /// "token" - Token of the "bear" token provider of the REST catalog.
    static const char TOKEN[];

    /// "token.provider" - Authentication provider of the REST catalog. Supported values are
    /// "bear" (the protocol's historical spelling of "bearer") and "dlf".
    static const char TOKEN_PROVIDER[];

    /// "dlf.region" - Region used by DLF request signing. Inferred from URI when absent.
    static const char DLF_REGION[];

    /// "dlf.token-path" - Path of a JSON file containing refreshable DLF credentials.
    static const char DLF_TOKEN_PATH[];

    /// "dlf.access-key-id" - DLF access key id.
    static const char DLF_ACCESS_KEY_ID[];

    /// "dlf.access-key-secret" - DLF access key secret.
    static const char DLF_ACCESS_KEY_SECRET[];

    /// "dlf.security-token" - Optional STS security token used with a DLF access key.
    static const char DLF_SECURITY_TOKEN[];

    /// "dlf.token-loader" - Refreshable DLF token loader ("ecs" or "local_file").
    static const char DLF_TOKEN_LOADER[];

    /// "dlf.token-ecs-metadata-url" - ECS RAM role metadata endpoint.
    static const char DLF_TOKEN_ECS_METADATA_URL[];

    /// "dlf.token-ecs-role-name" - Optional ECS RAM role name.
    static const char DLF_TOKEN_ECS_ROLE_NAME[];

    /// "dlf.signing-algorithm" - DLF signer ("default" or "openapi").
    static const char DLF_SIGNING_ALGORITHM[];

    /// "dlf.oss-endpoint" - OSS endpoint that overrides the "fs.oss.endpoint" of a data
    /// token issued by the REST catalog.
    static const char DLF_OSS_ENDPOINT[];

    /// "data-token.enabled" - Whether table data is accessed with the temporary
    /// credentials issued by the REST catalog instead of the credentials configured in
    /// the catalog options. Defaults to false.
    static const char DATA_TOKEN_ENABLED[];

    /// "table-default." - Prefix of the catalog options that provide table option
    /// defaults: "table-default.<key>=<value>" applies "<key>=<value>" to a created
    /// table when the caller left "<key>" unset.
    static const char TABLE_DEFAULT_OPTION_PREFIX[];
};

}  // namespace paimon

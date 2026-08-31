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

#include "paimon/catalog_options.h"

namespace paimon {

const char CatalogOptions::METASTORE[] = "metastore";
const char CatalogOptions::URI[] = "uri";
const char CatalogOptions::TOKEN[] = "token";
const char CatalogOptions::TOKEN_PROVIDER[] = "token.provider";
const char CatalogOptions::DLF_REGION[] = "dlf.region";
const char CatalogOptions::DLF_TOKEN_PATH[] = "dlf.token-path";
const char CatalogOptions::DLF_ACCESS_KEY_ID[] = "dlf.access-key-id";
const char CatalogOptions::DLF_ACCESS_KEY_SECRET[] = "dlf.access-key-secret";
const char CatalogOptions::DLF_SECURITY_TOKEN[] = "dlf.security-token";
const char CatalogOptions::DLF_TOKEN_LOADER[] = "dlf.token-loader";
const char CatalogOptions::DLF_TOKEN_ECS_METADATA_URL[] = "dlf.token-ecs-metadata-url";
const char CatalogOptions::DLF_TOKEN_ECS_ROLE_NAME[] = "dlf.token-ecs-role-name";
const char CatalogOptions::DLF_SIGNING_ALGORITHM[] = "dlf.signing-algorithm";
const char CatalogOptions::TABLE_DEFAULT_OPTION_PREFIX[] = "table-default.";

}  // namespace paimon

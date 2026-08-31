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

#include "paimon/rest/rest_auth.h"

#include "fmt/format.h"
#include "paimon/catalog_options.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/common/utils/url_utils.h"
#include "paimon/rest/dlf_auth.h"

namespace paimon {

RestAuthParameter RestAuthParameter::Create(const std::string& method,
                                            const std::string& resource_path,
                                            const std::map<std::string, std::string>& query_params,
                                            const std::string& data) {
    RestAuthParameter parameter;
    parameter.method = method;
    parameter.resource_path = resource_path;
    for (const auto& [key, value] : query_params) {
        parameter.parameters[key] = UrlUtils::EncodeString(value);
    }
    parameter.data = data;
    return parameter;
}

Result<std::map<std::string, std::string>> BearTokenAuthProvider::MergeAuthHeader(
    const std::map<std::string, std::string>& base_header,
    const RestAuthParameter& parameter) const {
    std::map<std::string, std::string> headers = base_header;
    headers["Authorization"] = "Bearer " + token_;
    return headers;
}

Result<std::unique_ptr<AuthProvider>> AuthProvider::Create(
    const std::map<std::string, std::string>& options) {
    Result<std::string> provider_value =
        OptionsUtils::GetNonEmptyValueFromMap(options, CatalogOptions::TOKEN_PROVIDER);
    if (!provider_value.ok()) {
        return Status::Invalid(fmt::format("option '{}' must be configured for the rest catalog",
                                           CatalogOptions::TOKEN_PROVIDER));
    }
    // Matched leniently in lower case; other clients may match provider names
    // case-sensitively, so the exact "bear" and "dlf" spellings are portable.
    std::string provider = StringUtils::ToLowerCase(provider_value.value());
    if (provider == "bear") {
        Result<std::string> token =
            OptionsUtils::GetNonEmptyValueFromMap(options, CatalogOptions::TOKEN);
        if (!token.ok()) {
            return Status::Invalid(
                fmt::format("option '{}' must be configured for the bear token provider",
                            CatalogOptions::TOKEN));
        }
        return std::make_unique<BearTokenAuthProvider>(token.value());
    }
    if (provider == "dlf") {
        return DlfAuthProvider::Create(options);
    }
    return Status::NotImplemented(fmt::format(
        "unsupported token provider: {}, supported providers are 'bear' and 'dlf'", provider));
}

}  // namespace paimon

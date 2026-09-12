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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/format/lance/lance_utils.h"

#include <cstring>
#include <map>
#include <optional>

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/format/lance/lance_ffi.h"

namespace paimon::lance {
namespace {

void CopyOption(const std::map<std::string, std::string>& options, const std::string& source,
                const std::string& target, std::map<std::string, std::string>* storage_options) {
    auto iter = options.find(source);
    if (iter != options.end() && !iter->second.empty()) {
        storage_options->insert_or_assign(target, iter->second);
    }
}

const char* CanonicalS3OptionName(const std::string& option) {
    if (option == "access-key" || option == "access.key" || option == "accessKeyId") {
        return "s3.access-key";
    }
    if (option == "secret-key" || option == "secret.key" || option == "accessKeySecret") {
        return "s3.secret-key";
    }
    if (option == "session.token" || option == "session-token" || option == "security.token" ||
        option == "security-token" || option == "securityToken") {
        return "s3.session.token";
    }
    if (option == "endpoint") {
        return "s3.endpoint";
    }
    if (option == "region") {
        return "s3.region";
    }
    if (option == "path-style-access" || option == "path.style.access") {
        return "s3.path-style-access";
    }
    return nullptr;
}

std::map<std::string, std::string> NormalizeS3Options(
    const std::map<std::string, std::string>& options) {
    std::map<std::string, std::string> normalized = options;
    for (const auto& [key, value] : options) {
        for (const char* prefix : {"s3a.", "fs.s3.", "fs.s3a.", "s3."}) {
            if (!StringUtils::StartsWith(key, prefix)) {
                continue;
            }
            const char* canonical = CanonicalS3OptionName(key.substr(std::strlen(prefix)));
            if (canonical != nullptr && normalized.find(canonical) == normalized.end()) {
                normalized.emplace(canonical, value);
            }
            break;
        }
    }
    return normalized;
}

void AddS3Options(const std::map<std::string, std::string>& options,
                  std::map<std::string, std::string>* storage_options) {
    const std::map<std::string, std::string> normalized = NormalizeS3Options(options);
    CopyOption(normalized, "s3.region", "region", storage_options);
    CopyOption(normalized, "s3.endpoint", "endpoint", storage_options);
    CopyOption(normalized, "s3.access-key", "access_key_id", storage_options);
    CopyOption(normalized, "s3.secret-key", "secret_access_key", storage_options);
    CopyOption(normalized, "s3.session.token", "session_token", storage_options);
    auto endpoint = storage_options->find("endpoint");
    if (endpoint != storage_options->end() && endpoint->second.find("://") == std::string::npos) {
        endpoint->second = "https://" + endpoint->second;
    }
    auto path_style = normalized.find("s3.path-style-access");
    if (path_style != normalized.end()) {
        std::optional<bool> enabled = StringUtils::StringToValue<bool>(path_style->second);
        if (enabled) {
            storage_options->insert_or_assign("virtual_hosted_style_request",
                                              *enabled ? "false" : "true");
        }
    }
}

void AddOssOptions(const std::map<std::string, std::string>& options, const std::string& bucket,
                   std::map<std::string, std::string>* storage_options) {
    auto copy_oss_option = [&](const std::string& suffix, const std::string& target) {
        CopyOption(options, "fs.oss." + suffix, target, storage_options);
        if (!bucket.empty()) {
            CopyOption(options, "fs.oss.bucket." + bucket + "." + suffix, target, storage_options);
        }
    };
    // lance-io 0.39 uses OpenDAL for oss://. It consumes oss_endpoint and adds
    // the bucket itself; Java's S3 endpoint/virtual-host options are unused here.
    copy_oss_option("endpoint", "oss_endpoint");
    copy_oss_option("region", "oss_region");
    copy_oss_option("accessKeyId", "oss_access_key_id");
    copy_oss_option("accessKeySecret", "oss_secret_access_key");
    copy_oss_option("sessionToken", "oss_session_token");
    copy_oss_option("securityToken", "oss_session_token");
    copy_oss_option("accessKeyId", "access_key_id");
    copy_oss_option("accessKeySecret", "secret_access_key");
    copy_oss_option("sessionToken", "session_token");
    copy_oss_option("securityToken", "session_token");
    auto endpoint = storage_options->find("oss_endpoint");
    if (endpoint != storage_options->end() && endpoint->second.find("://") == std::string::npos) {
        endpoint->second = "https://" + endpoint->second;
    }
}

}  // namespace

LanceStorageOptions GetLanceStorageOptions(const std::map<std::string, std::string>& options,
                                           const std::string& uri) {
    std::map<std::string, std::string> storage_options;
    Result<Path> parsed_path = PathUtil::ToPath(uri);
    if (parsed_path.ok() && parsed_path.value().scheme == "s3") {
        AddS3Options(options, &storage_options);
    } else if (parsed_path.ok() && parsed_path.value().scheme == "oss") {
        AddOssOptions(options, parsed_path.value().authority, &storage_options);
    }
    for (const auto& [key, value] : options) {
        if (key.rfind(kLanceStorageOptionPrefix, 0) == 0) {
            storage_options.insert_or_assign(key.substr(sizeof(kLanceStorageOptionPrefix) - 1),
                                             value);
        }
    }
    auto endpoint = storage_options.find(
        parsed_path.ok() && parsed_path.value().scheme == "oss" ? "oss_endpoint" : "endpoint");
    if (storage_options.find("allow_http") == storage_options.end() &&
        endpoint != storage_options.end() && StringUtils::StartsWith(endpoint->second, "http://")) {
        storage_options.emplace("allow_http", "true");
    }
    return LanceStorageOptions(storage_options.begin(), storage_options.end());
}

Status LanceFfiError(const std::string& operation) {
    const char* error = paimon_lance_last_error();
    return Status::Invalid(operation, ": ", error == nullptr ? "unknown Lance error" : error);
}

}  // namespace paimon::lance

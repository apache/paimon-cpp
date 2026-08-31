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

#include "paimon/fs/oss/oss_file_system_factory.h"

#include <cctype>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "alibabacloud/oss2/ClientConfiguration.h"
#include "alibabacloud/oss2/OSSClient.h"
#include "alibabacloud/oss2/credentials/CredentialsProvider.h"
#include "fmt/format.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/factories/factory.h"
#include "paimon/fs/oss/oss_file_system.h"

namespace paimon::oss {
namespace {

namespace oss2 = alibabacloud::oss2;

constexpr std::string_view kOssOptionPrefix = "fs.oss.";
constexpr char kEndpointPrefix[] = "oss-";
constexpr char kEndpointSuffix[] = ".aliyuncs.com";
constexpr char kDualStackEndpointSuffix[] = ".oss.aliyuncs.com";
constexpr char kInternalEndpointSuffix[] = "-internal";

bool IsValidRegion(const std::string& region) {
    size_t first_separator = region.find('-');
    if (first_separator < 2 || first_separator > 3 || first_separator + 1 == region.size()) {
        return false;
    }
    for (size_t i = 0; i < first_separator; ++i) {
        if (!std::islower(static_cast<unsigned char>(region[i]))) {
            return false;
        }
    }
    bool previous_was_separator = true;
    for (size_t i = first_separator + 1; i < region.size(); ++i) {
        char value = region[i];
        if (value == '-') {
            if (previous_was_separator || i + 1 == region.size()) {
                return false;
            }
            previous_was_separator = true;
        } else if (std::islower(static_cast<unsigned char>(value)) ||
                   std::isdigit(static_cast<unsigned char>(value))) {
            previous_was_separator = false;
        } else {
            return false;
        }
    }
    return !StringUtils::EndsWith(region, "-dualstack") && !StringUtils::EndsWith(region, "-pub");
}

std::string GetBucketOptionKey(const std::string& bucket, std::string_view option) {
    return fmt::format("fs.oss.bucket.{}.{}", bucket, option.substr(kOssOptionPrefix.size()));
}

const std::string* FindOption(const std::map<std::string, std::string>& options,
                              const std::string& bucket, std::string_view option,
                              std::string* option_key) {
    std::string bucket_option_key = GetBucketOptionKey(bucket, option);
    auto bucket_option = options.find(bucket_option_key);
    if (bucket_option != options.end()) {
        *option_key = std::move(bucket_option_key);
        return &bucket_option->second;
    }
    option_key->assign(option);
    auto global_option = options.find(*option_key);
    return global_option == options.end() ? nullptr : &global_option->second;
}

std::string GetOption(const std::map<std::string, std::string>& options, const std::string& bucket,
                      std::string_view option) {
    std::string option_key;
    const std::string* value = FindOption(options, bucket, option, &option_key);
    return value == nullptr ? "" : *value;
}

Result<std::string> GetRequiredOption(const std::map<std::string, std::string>& options,
                                      const std::string& bucket, std::string_view option) {
    std::string option_key;
    const std::string* value = FindOption(options, bucket, option, &option_key);
    if (value == nullptr || value->empty()) {
        return Status::Invalid(fmt::format("OSS option '{}' must not be empty", option_key));
    }
    return *value;
}

Result<std::unique_ptr<Executor>> CreateExecutor(const std::map<std::string, std::string>& options,
                                                 const std::string& bucket) {
    std::string option_key;
    const std::string* value =
        FindOption(options, bucket, kOssExecutorThreadCountOption, &option_key);
    if (value == nullptr) {
        return CreateDefaultExecutor();
    }
    std::optional<uint32_t> thread_count = StringUtils::StringToValue<uint32_t>(*value);
    if (!thread_count.has_value() || *thread_count == 0) {
        return Status::Invalid(fmt::format(
            "OSS executor thread count for option '{}' must be greater than 0", option_key));
    }
    return CreateDefaultExecutor(*thread_count);
}

std::string NormalizeEndpoint(std::string endpoint) {
    if (!endpoint.empty() && endpoint.find("://") == std::string::npos) {
        endpoint = "https://" + endpoint;
    }
    return endpoint;
}

std::string InferRegion(std::string endpoint) {
    size_t scheme = endpoint.find("://");
    if (scheme != std::string::npos) {
        endpoint.erase(0, scheme + 3);
    }
    size_t slash = endpoint.find('/');
    if (slash != std::string::npos) {
        endpoint.erase(slash);
    }
    size_t port_separator = endpoint.rfind(':');
    if (port_separator != std::string::npos && endpoint.find(':') == port_separator) {
        std::optional<uint16_t> port =
            StringUtils::StringToValue<uint16_t>(endpoint.substr(port_separator + 1));
        if (port.has_value()) {
            endpoint.erase(port_separator);
        }
    }

    std::string region;
    if (StringUtils::StartsWith(endpoint, kEndpointPrefix) &&
        StringUtils::EndsWith(endpoint, kEndpointSuffix)) {
        region = endpoint.substr(
            sizeof(kEndpointPrefix) - 1,
            endpoint.size() - (sizeof(kEndpointPrefix) - 1) - (sizeof(kEndpointSuffix) - 1));
        if (StringUtils::EndsWith(region, kInternalEndpointSuffix)) {
            region.erase(region.size() - (sizeof(kInternalEndpointSuffix) - 1));
        }
    } else if (StringUtils::EndsWith(endpoint, kDualStackEndpointSuffix)) {
        region = endpoint.substr(0, endpoint.size() - (sizeof(kDualStackEndpointSuffix) - 1));
    }
    return IsValidRegion(region) ? region : "";
}

}  // namespace

const char OssFileSystemFactory::IDENTIFIER[] = "oss";

Result<std::unique_ptr<FileSystem>> OssFileSystemFactory::Create(
    const std::string& path, const std::map<std::string, std::string>& options) const {
    PAIMON_ASSIGN_OR_RAISE(Path parsed_path, PathUtil::ToPath(path));
    if (parsed_path.scheme != "oss" || parsed_path.authority.empty()) {
        return Status::Invalid(fmt::format("invalid OSS path '{}'", path));
    }
    const std::string& bucket = parsed_path.authority;
    PAIMON_ASSIGN_OR_RAISE(std::string access_key_id,
                           GetRequiredOption(options, bucket, kOssAccessKeyIdOption));
    PAIMON_ASSIGN_OR_RAISE(std::string access_key_secret,
                           GetRequiredOption(options, bucket, kOssAccessKeySecretOption));
    std::string endpoint = GetOption(options, bucket, kOssEndpointOption);
    std::string region = GetOption(options, bucket, kOssRegionOption);
    if (region.empty()) {
        region = InferRegion(endpoint);
    }
    if (endpoint.empty() && region.empty()) {
        return Status::Invalid("OSS endpoint or region must be configured");
    }
    std::string signature_version = GetOption(options, bucket, kOssSignatureVersionOption);
    if (!signature_version.empty() && signature_version != "v1" && signature_version != "v4") {
        return Status::Invalid(
            fmt::format("invalid OSS signature version '{}'", signature_version));
    }
    if (region.empty() && signature_version != "v1") {
        return Status::Invalid(
            "OSS region must be configured when the endpoint does not identify a region");
    }
    std::string security_token = GetOption(options, bucket, kOssSecurityTokenOption);
    if (security_token.empty()) {
        security_token = GetOption(options, bucket, kOssSessionTokenOption);
    }

    oss2::ClientConfiguration config = oss2::ClientConfiguration::loadDefault();
    if (!endpoint.empty()) {
        config.endpoint = NormalizeEndpoint(endpoint);
    }
    if (!region.empty()) {
        config.region = region;
    }
    if (!signature_version.empty()) {
        config.signatureVersion = signature_version;
    }
    config.userAgent = "paimon-cpp";
    config.credentialsProvider = std::make_shared<oss2::StaticCredentialsProvider>(
        std::move(access_key_id), std::move(access_key_secret), std::move(security_token));
    std::string path_style = GetOption(options, bucket, kOssUsePathStyleOption);
    if (!path_style.empty()) {
        std::optional<bool> value = StringUtils::StringToValue<bool>(path_style);
        if (!value.has_value()) {
            return Status::Invalid(fmt::format("invalid boolean value '{}' for OSS option '{}'",
                                               path_style, kOssUsePathStyleOption));
        }
        config.usePathStyle = *value;
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Executor> executor, CreateExecutor(options, bucket));
    return std::make_unique<OssFileSystem>(bucket, std::make_shared<oss2::OSSClient>(config),
                                           std::move(executor));
}

REGISTER_PAIMON_FACTORY(OssFileSystemFactory);

}  // namespace paimon::oss

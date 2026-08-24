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

#include "paimon/rest/dlf_auth.h"

#include <openssl/evp.h>

#include <array>
#include <cctype>
#include <climits>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <limits>
#include <regex>
#include <set>
#include <sstream>
#include <string_view>
#include <thread>
#include <utility>

#include "fmt/format.h"
#include "paimon/catalog_options.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/common/utils/url_utils.h"
#include "paimon/common/utils/uuid.h"
#include "rapidjson/document.h"

namespace paimon {

namespace {

constexpr int32_t kEcsMetadataRequestTimeoutMillis = 3 * 60 * 1000;

constexpr int64_t kTokenExpirationSafeTimeMillis = 60 * 60 * 1000;
constexpr size_t kMaxTokenResponseBytes = 1024 * 1024;
constexpr const char kDefaultEcsMetadataUrl[] =
    "http://100.100.100.200/latest/meta-data/Ram/security-credentials/";

constexpr const char kAuthorizationHeader[] = "Authorization";
constexpr const char kContentMd5Header[] = "Content-MD5";
constexpr const char kContentTypeHeader[] = "Content-Type";
constexpr const char kDlfDateHeader[] = "x-dlf-date";
constexpr const char kDlfSecurityTokenHeader[] = "x-dlf-security-token";
constexpr const char kDlfVersionHeader[] = "x-dlf-version";
constexpr const char kDlfContentSha256Header[] = "x-dlf-content-sha256";
constexpr const char kUnsignedPayload[] = "UNSIGNED-PAYLOAD";
constexpr const char kJsonMediaType[] = "application/json";

constexpr const char kOpenApiDateHeader[] = "Date";
constexpr const char kOpenApiAcceptHeader[] = "Accept";
constexpr const char kOpenApiHostHeader[] = "Host";
constexpr const char kAcsSignatureMethodHeader[] = "x-acs-signature-method";
constexpr const char kAcsSignatureNonceHeader[] = "x-acs-signature-nonce";
constexpr const char kAcsSignatureVersionHeader[] = "x-acs-signature-version";
constexpr const char kAcsVersionHeader[] = "x-acs-version";
constexpr const char kAcsSecurityTokenHeader[] = "x-acs-security-token";

void TrimWhitespace(std::string* value) {
    size_t begin = 0;
    while (begin < value->size() && std::isspace(static_cast<unsigned char>((*value)[begin]))) {
        ++begin;
    }
    size_t end = value->size();
    while (end > begin && std::isspace(static_cast<unsigned char>((*value)[end - 1]))) {
        --end;
    }
    *value = value->substr(begin, end - begin);
}

std::optional<std::string> FindOption(const std::map<std::string, std::string>& options,
                                      const std::string& key) {
    auto iter = options.find(key);
    if (iter == options.end()) {
        return std::nullopt;
    }
    return iter->second;
}

Result<std::string> RequiredNonEmptyOption(const std::map<std::string, std::string>& options,
                                           const std::string& key) {
    std::optional<std::string> value = FindOption(options, key);
    if (!value || value->empty()) {
        return Status::Invalid(fmt::format("option '{}' must be configured for DLF auth", key));
    }
    return value.value();
}

Result<std::string> RequiredJsonString(const rapidjson::Value& object, const char* key) {
    if (!object.HasMember(key) || !object[key].IsString() || object[key].GetStringLength() == 0) {
        return Status::Invalid(fmt::format("DLF token field '{}' must be a non-empty string", key));
    }
    return std::string(object[key].GetString(), object[key].GetStringLength());
}

Result<std::optional<std::string>> OptionalJsonString(const rapidjson::Value& object,
                                                      const char* key) {
    if (!object.HasMember(key) || object[key].IsNull()) {
        return std::optional<std::string>();
    }
    if (!object[key].IsString()) {
        return Status::Invalid(fmt::format("DLF token field '{}' must be a string", key));
    }
    return std::optional<std::string>(
        std::string(object[key].GetString(), object[key].GetStringLength()));
}

Result<std::tm> ToUtc(std::chrono::system_clock::time_point time) {
    std::time_t seconds = std::chrono::system_clock::to_time_t(time);
    std::tm utc{};
#if defined(_WIN32)
    if (gmtime_s(&utc, &seconds) != 0) {
        return Status::Invalid("failed to convert DLF signing time to UTC");
    }
#else
    if (gmtime_r(&seconds, &utc) == nullptr) {
        return Status::Invalid("failed to convert DLF signing time to UTC");
    }
#endif
    return utc;
}

Result<std::string> FormatDlfTime(std::chrono::system_clock::time_point time) {
    PAIMON_ASSIGN_OR_RAISE(std::tm utc, ToUtc(time));
    std::array<char, 32> buffer{};
    if (std::strftime(buffer.data(), buffer.size(), "%Y%m%dT%H%M%SZ", &utc) == 0) {
        return Status::Invalid("failed to format DLF signing time");
    }
    return std::string(buffer.data());
}

Result<std::string> FormatRfc1123Time(std::chrono::system_clock::time_point time) {
    PAIMON_ASSIGN_OR_RAISE(std::tm utc, ToUtc(time));
    static constexpr std::array<const char*, 7> kWeekdays = {"Sun", "Mon", "Tue", "Wed",
                                                             "Thu", "Fri", "Sat"};
    static constexpr std::array<const char*, 12> kMonths = {
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    if (utc.tm_wday < 0 || utc.tm_wday >= static_cast<int32_t>(kWeekdays.size()) ||
        utc.tm_mon < 0 || utc.tm_mon >= static_cast<int32_t>(kMonths.size())) {
        return Status::Invalid("failed to format DLF OpenAPI signing time");
    }
    return fmt::format("{}, {:02d} {} {:04d} {:02d}:{:02d}:{:02d} GMT", kWeekdays[utc.tm_wday],
                       utc.tm_mday, kMonths[utc.tm_mon], utc.tm_year + 1900, utc.tm_hour,
                       utc.tm_min, utc.tm_sec);
}

Result<int64_t> ParseExpiration(const std::string& expiration) {
    static const std::regex kExpirationPattern(
        "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$");
    if (!std::regex_match(expiration, kExpirationPattern)) {
        return Status::Invalid("invalid DLF token expiration");
    }
    std::tm utc{};
    std::istringstream stream(expiration);
    stream >> std::get_time(&utc, "%Y-%m-%dT%H:%M:%SZ");
    if (stream.fail() || stream.peek() != std::char_traits<char>::eof()) {
        return Status::Invalid("invalid DLF token expiration");
    }
    int32_t year = utc.tm_year;
    int32_t month = utc.tm_mon;
    int32_t day = utc.tm_mday;
    int32_t hour = utc.tm_hour;
    int32_t minute = utc.tm_min;
    int32_t second = utc.tm_sec;
    std::time_t timestamp = timegm(&utc);
    std::tm verified{};
#if defined(_WIN32)
    if (timestamp == static_cast<std::time_t>(-1) || gmtime_s(&verified, &timestamp) != 0) {
        return Status::Invalid("invalid DLF token expiration");
    }
#else
    if (timestamp == static_cast<std::time_t>(-1) || gmtime_r(&timestamp, &verified) == nullptr) {
        return Status::Invalid("invalid DLF token expiration");
    }
#endif
    if (verified.tm_year != year || verified.tm_mon != month || verified.tm_mday != day ||
        verified.tm_hour != hour || verified.tm_min != minute || verified.tm_sec != second) {
        return Status::Invalid("invalid DLF token expiration");
    }
    if (timestamp > std::numeric_limits<int64_t>::max() / 1000) {
        return Status::Invalid("DLF token expiration is out of range");
    }
    return static_cast<int64_t>(timestamp) * 1000;
}

using Bytes = std::vector<uint8_t>;
using EvpMdContext = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;
using EvpPkey = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;

Result<Bytes> Digest(const EVP_MD* digest, std::string_view data) {
    EvpMdContext context(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!context || EVP_DigestInit_ex(context.get(), digest, nullptr) != 1 ||
        EVP_DigestUpdate(context.get(), data.data(), data.size()) != 1) {
        return Status::IOError("failed to calculate DLF request digest");
    }
    Bytes output(EVP_MAX_MD_SIZE);
    unsigned int output_size = 0;
    if (EVP_DigestFinal_ex(context.get(), output.data(), &output_size) != 1) {
        return Status::IOError("failed to calculate DLF request digest");
    }
    output.resize(output_size);
    return output;
}

Result<Bytes> Hmac(const EVP_MD* digest, const Bytes& key, std::string_view data) {
    if (key.size() > static_cast<size_t>(INT_MAX)) {
        return Status::Invalid("DLF signing key is too large");
    }
    EvpPkey signing_key(
        EVP_PKEY_new_mac_key(EVP_PKEY_HMAC, nullptr, key.data(), static_cast<int32_t>(key.size())),
        EVP_PKEY_free);
    EvpMdContext context(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!signing_key || !context ||
        EVP_DigestSignInit(context.get(), nullptr, digest, nullptr, signing_key.get()) != 1 ||
        EVP_DigestSignUpdate(context.get(), data.data(), data.size()) != 1) {
        return Status::IOError("failed to calculate DLF request signature");
    }
    size_t output_size = 0;
    if (EVP_DigestSignFinal(context.get(), nullptr, &output_size) != 1) {
        return Status::IOError("failed to calculate DLF request signature");
    }
    Bytes output(output_size);
    if (EVP_DigestSignFinal(context.get(), output.data(), &output_size) != 1) {
        return Status::IOError("failed to calculate DLF request signature");
    }
    output.resize(output_size);
    return output;
}

Bytes ToBytes(const std::string& value) {
    return Bytes(value.begin(), value.end());
}

std::string HexEncode(const Bytes& value) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string encoded;
    encoded.reserve(value.size() * 2);
    for (uint8_t byte : value) {
        encoded.push_back(kHex[byte >> 4]);
        encoded.push_back(kHex[byte & 0x0f]);
    }
    return encoded;
}

Result<std::string> Base64Encode(const Bytes& value) {
    if (value.size() > static_cast<size_t>(INT_MAX)) {
        return Status::Invalid("DLF digest is too large to encode");
    }
    size_t capacity = 4 * ((value.size() + 2) / 3) + 1;
    std::string encoded(capacity, '\0');
    int32_t size = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(encoded.data()), value.data(),
                                   static_cast<int32_t>(value.size()));
    if (size < 0) {
        return Status::IOError("failed to encode DLF request digest");
    }
    encoded.resize(static_cast<size_t>(size));
    return encoded;
}

Result<std::string> Md5Base64(const std::string& value) {
    PAIMON_ASSIGN_OR_RAISE(Bytes digest, Digest(EVP_md5(), value));
    return Base64Encode(digest);
}

std::string Trimmed(const std::string& value) {
    std::string trimmed = value;
    TrimWhitespace(&trimmed);
    return trimmed;
}

std::string DefaultCanonicalRequest(const RestAuthParameter& parameter,
                                    const DlfRequestSigner::Headers& headers) {
    std::string canonical = parameter.method + "\n" + parameter.resource_path + "\n";
    bool first = true;
    for (const auto& [key, value] : parameter.parameters) {
        if (!first) {
            canonical += "&";
        }
        canonical += Trimmed(key);
        if (!value.empty()) {
            canonical += "=" + Trimmed(value);
        }
        first = false;
    }

    static const std::set<std::string> kSignedHeaders = {
        "content-md5", "content-type",  "x-dlf-content-sha256",
        "x-dlf-date",  "x-dlf-version", "x-dlf-security-token"};
    std::map<std::string, std::string> sorted_headers;
    for (const auto& [key, value] : headers) {
        std::string lower_key = StringUtils::ToLowerCase(key);
        if (kSignedHeaders.count(lower_key) > 0) {
            sorted_headers[lower_key] = Trimmed(value);
        }
    }
    for (const auto& [key, value] : sorted_headers) {
        canonical += "\n" + key + ":" + value;
    }
    auto content_iter = headers.find(kDlfContentSha256Header);
    std::string content_sha =
        content_iter == headers.end() ? std::string(kUnsignedPayload) : content_iter->second;
    return canonical + "\n" + content_sha;
}

Result<std::string> RequiredHeader(const DlfRequestSigner::Headers& headers,
                                   const std::string& name) {
    auto iter = headers.find(name);
    if (iter == headers.end() || iter->second.empty()) {
        return Status::Invalid(fmt::format("DLF signing header '{}' is missing", name));
    }
    return iter->second;
}

std::string OpenApiCanonicalizedHeaders(const DlfRequestSigner::Headers& headers) {
    std::map<std::string, std::string> sorted;
    for (const auto& [key, value] : headers) {
        std::string lower_key = StringUtils::ToLowerCase(key);
        if (StringUtils::StartsWith(lower_key, "x-acs-")) {
            sorted[lower_key] = Trimmed(value);
        }
    }
    std::string canonical;
    for (const auto& [key, value] : sorted) {
        canonical += key + ":" + value + "\n";
    }
    return canonical;
}

std::string OpenApiCanonicalizedResource(const RestAuthParameter& parameter) {
    std::string resource = UrlUtils::DecodeString(parameter.resource_path);
    if (parameter.parameters.empty()) {
        return resource;
    }
    resource += "?";
    bool first = true;
    for (const auto& [key, value] : parameter.parameters) {
        if (!first) {
            resource += "&";
        }
        resource += key;
        std::string decoded = UrlUtils::DecodeString(value);
        if (!decoded.empty()) {
            resource += "=" + decoded;
        }
        first = false;
    }
    return resource;
}

Result<std::string> GenerateNonce(std::chrono::system_clock::time_point now) {
    std::string uuid;
    if (!UUID::Generate(&uuid)) {
        return Status::IOError("failed to generate DLF OpenAPI signing nonce");
    }
    int64_t millis =
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    std::ostringstream thread_id;
    thread_id << std::this_thread::get_id();
    return fmt::format("{}{}{}", uuid, millis, thread_id.str());
}

Result<std::unique_ptr<DlfRequestSigner>> CreateSigner(const std::string& algorithm,
                                                       const std::string& region) {
    if (algorithm == DlfDefaultSigner::kIdentifier) {
        return std::make_unique<DlfDefaultSigner>(region);
    }
    if (algorithm == DlfOpenApiSigner::kIdentifier) {
        return std::make_unique<DlfOpenApiSigner>();
    }
    return Status::Invalid(fmt::format(
        "unsupported DLF signing algorithm '{}', supported values are 'default' and 'openapi'",
        algorithm));
}

}  // namespace

DlfToken::DlfToken(const std::string& access_key_id, const std::string& access_key_secret,
                   const std::optional<std::string>& security_token,
                   const std::optional<int64_t>& expiration_at_millis)
    : access_key_id_(access_key_id),
      access_key_secret_(access_key_secret),
      security_token_(security_token),
      expiration_at_millis_(expiration_at_millis) {}

Result<DlfToken> DlfToken::FromJson(const std::string& json) {
    rapidjson::Document document;
    document.Parse(json.data(), json.size());
    if (document.HasParseError() || !document.IsObject()) {
        return Status::Invalid("failed to parse DLF token JSON");
    }
    PAIMON_ASSIGN_OR_RAISE(std::string access_key_id, RequiredJsonString(document, "AccessKeyId"));
    PAIMON_ASSIGN_OR_RAISE(std::string access_key_secret,
                           RequiredJsonString(document, "AccessKeySecret"));
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> security_token,
                           OptionalJsonString(document, "SecurityToken"));
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> expiration,
                           OptionalJsonString(document, "Expiration"));
    std::optional<int64_t> expiration_at_millis;
    if (expiration) {
        PAIMON_ASSIGN_OR_RAISE(int64_t parsed_expiration, ParseExpiration(expiration.value()));
        expiration_at_millis = parsed_expiration;
    }
    return DlfToken(access_key_id, access_key_secret, security_token, expiration_at_millis);
}

bool DlfToken::ShouldRefresh(std::chrono::system_clock::time_point now) const {
    if (!expiration_at_millis_) {
        return false;
    }
    int64_t now_millis =
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    return expiration_at_millis_.value() - now_millis < kTokenExpirationSafeTimeMillis;
}

DlfLocalFileTokenLoader::DlfLocalFileTokenLoader(const std::string& token_file_path,
                                                 int32_t max_attempts,
                                                 std::chrono::milliseconds retry_delay)
    : token_file_path_(token_file_path), max_attempts_(max_attempts), retry_delay_(retry_delay) {}

Result<DlfToken> DlfLocalFileTokenLoader::LoadToken() {
    if (token_file_path_.empty()) {
        return Status::Invalid("DLF token file path is empty");
    }
    if (max_attempts_ <= 0 || retry_delay_.count() < 0) {
        return Status::Invalid("invalid DLF token file retry configuration");
    }
    Status last_status = Status::Invalid("failed to load DLF token file");
    for (int32_t attempt = 1; attempt <= max_attempts_; ++attempt) {
        std::ifstream file(token_file_path_, std::ios::binary);
        if (!file.is_open()) {
            last_status = Status::IOError(
                fmt::format("failed to read DLF token file '{}'", token_file_path_));
        } else {
            std::string contents(kMaxTokenResponseBytes + 1, '\0');
            file.read(contents.data(), static_cast<std::streamsize>(contents.size()));
            std::streamsize size = file.gcount();
            if (file.bad()) {
                last_status = Status::IOError(
                    fmt::format("failed to read DLF token file '{}'", token_file_path_));
            } else if (size > static_cast<std::streamsize>(kMaxTokenResponseBytes)) {
                last_status = Status::Invalid("DLF token file is too large");
            } else {
                contents.resize(static_cast<size_t>(size));
                Result<DlfToken> token = DlfToken::FromJson(contents);
                if (token.ok()) {
                    return token;
                }
                last_status = Status::Invalid("failed to parse DLF token file");
            }
        }
        if (attempt < max_attempts_) {
            std::this_thread::sleep_for(retry_delay_ * attempt);
        }
    }
    return last_status;
}

std::string DlfLocalFileTokenLoader::Description() const {
    return token_file_path_;
}

DlfEcsTokenLoader::DlfEcsTokenLoader(const std::string& metadata_url,
                                     const std::optional<std::string>& role_name,
                                     std::unique_ptr<HttpClient> http_client)
    : metadata_url_(metadata_url), role_name_(role_name), http_client_(std::move(http_client)) {}

std::unique_ptr<DlfEcsTokenLoader> DlfEcsTokenLoader::Create(
    const std::string& metadata_url, const std::optional<std::string>& role_name) {
    return std::make_unique<DlfEcsTokenLoader>(metadata_url, role_name,
                                               std::make_unique<CurlHttpClient>());
}

Result<std::string> DlfEcsTokenLoader::Get(const std::string& url) const {
    if (!http_client_) {
        return Status::Invalid("DLF ECS metadata HTTP client is not configured");
    }
    HttpRequest request;
    request.url = url;
    request.request_timeout_ms = kEcsMetadataRequestTimeoutMillis;
    std::string body;
    Result<HttpResponse> response =
        http_client_->Execute(request, [&body](const char* data, int64_t size) {
            if (size < 0 || body.size() + static_cast<size_t>(size) > kMaxTokenResponseBytes) {
                return Status::Invalid("DLF ECS metadata response is too large");
            }
            body.append(data, static_cast<size_t>(size));
            return Status::OK();
        });
    if (!response.ok()) {
        return Status::IOError("failed to request DLF credentials from ECS metadata service");
    }
    HttpResponse http_response = std::move(response).value();
    if (http_response.status_code < 200 || http_response.status_code >= 300) {
        return Status::IOError(fmt::format("DLF ECS metadata service returned HTTP status {}",
                                           http_response.status_code));
    }
    if (StringUtils::IsNullOrWhitespaceOnly(body)) {
        return Status::Invalid("DLF ECS metadata service returned an empty response");
    }
    return body;
}

Result<DlfToken> DlfEcsTokenLoader::LoadToken() {
    if (metadata_url_.empty()) {
        return Status::Invalid("DLF ECS metadata URL is empty");
    }
    if (!role_name_) {
        PAIMON_ASSIGN_OR_RAISE(std::string role, Get(metadata_url_));
        TrimWhitespace(&role);
        if (role.empty()) {
            return Status::Invalid("DLF ECS metadata service returned an empty role name");
        }
        role_name_ = role;
    }
    PAIMON_ASSIGN_OR_RAISE(std::string token_json, Get(metadata_url_ + role_name_.value()));
    Result<DlfToken> token = DlfToken::FromJson(token_json);
    if (!token.ok()) {
        return Status::Invalid("failed to parse DLF ECS token response");
    }
    return token;
}

std::string DlfEcsTokenLoader::Description() const {
    return metadata_url_;
}

DlfDefaultSigner::DlfDefaultSigner(const std::string& region) : region_(region) {}

Result<DlfRequestSigner::Headers> DlfDefaultSigner::SignHeaders(
    const std::string& body, std::chrono::system_clock::time_point now,
    const std::optional<std::string>& security_token, const std::string& host) const {
    PAIMON_ASSIGN_OR_RAISE(std::string date_time, FormatDlfTime(now));
    Headers headers = {{kDlfDateHeader, date_time},
                       {kDlfContentSha256Header, kUnsignedPayload},
                       {kDlfVersionHeader, "v1"}};
    if (!body.empty()) {
        PAIMON_ASSIGN_OR_RAISE(std::string content_md5, Md5Base64(body));
        headers[kContentTypeHeader] = kJsonMediaType;
        headers[kContentMd5Header] = content_md5;
    }
    if (security_token) {
        headers[kDlfSecurityTokenHeader] = security_token.value();
    }
    return headers;
}

Result<std::string> DlfDefaultSigner::Authorization(const RestAuthParameter& parameter,
                                                    const DlfToken& token, const std::string& host,
                                                    const Headers& sign_headers) const {
    PAIMON_ASSIGN_OR_RAISE(std::string date_time, RequiredHeader(sign_headers, kDlfDateHeader));
    if (date_time.size() < 8) {
        return Status::Invalid("DLF signing date is invalid");
    }
    std::string date = date_time.substr(0, 8);
    std::string scope = fmt::format("{}/{}/DlfNext/aliyun_v4_request", date, region_);
    std::string canonical_request = DefaultCanonicalRequest(parameter, sign_headers);
    PAIMON_ASSIGN_OR_RAISE(Bytes canonical_hash, Digest(EVP_sha256(), canonical_request));
    std::string string_to_sign =
        fmt::format("DLF4-HMAC-SHA256\n{}\n{}\n{}", date_time, scope, HexEncode(canonical_hash));

    PAIMON_ASSIGN_OR_RAISE(
        Bytes date_key,
        Hmac(EVP_sha256(), ToBytes("aliyun_v4" + token.GetAccessKeySecret()), date));
    PAIMON_ASSIGN_OR_RAISE(Bytes region_key, Hmac(EVP_sha256(), date_key, region_));
    PAIMON_ASSIGN_OR_RAISE(Bytes service_key, Hmac(EVP_sha256(), region_key, "DlfNext"));
    PAIMON_ASSIGN_OR_RAISE(Bytes signing_key, Hmac(EVP_sha256(), service_key, "aliyun_v4_request"));
    PAIMON_ASSIGN_OR_RAISE(Bytes signature, Hmac(EVP_sha256(), signing_key, string_to_sign));
    return fmt::format("DLF4-HMAC-SHA256 Credential={}/{},Signature={}", token.GetAccessKeyId(),
                       scope, HexEncode(signature));
}

Result<DlfRequestSigner::Headers> DlfOpenApiSigner::SignHeaders(
    const std::string& body, std::chrono::system_clock::time_point now,
    const std::optional<std::string>& security_token, const std::string& host) const {
    if (host.empty()) {
        return Status::Invalid("DLF OpenAPI signing host is empty");
    }
    PAIMON_ASSIGN_OR_RAISE(std::string date, FormatRfc1123Time(now));
    PAIMON_ASSIGN_OR_RAISE(std::string nonce, GenerateNonce(now));
    Headers headers = {{kOpenApiDateHeader, date},        {kOpenApiAcceptHeader, kJsonMediaType},
                       {kOpenApiHostHeader, host},        {kAcsSignatureMethodHeader, "HMAC-SHA1"},
                       {kAcsSignatureNonceHeader, nonce}, {kAcsSignatureVersionHeader, "1.0"},
                       {kAcsVersionHeader, "2026-01-18"}};
    if (!body.empty()) {
        PAIMON_ASSIGN_OR_RAISE(std::string content_md5, Md5Base64(body));
        headers[kContentMd5Header] = content_md5;
        headers[kContentTypeHeader] = kJsonMediaType;
    }
    if (security_token) {
        headers[kAcsSecurityTokenHeader] = security_token.value();
    }
    return headers;
}

Result<std::string> DlfOpenApiSigner::Authorization(const RestAuthParameter& parameter,
                                                    const DlfToken& token, const std::string& host,
                                                    const Headers& sign_headers) const {
    PAIMON_ASSIGN_OR_RAISE(std::string accept, RequiredHeader(sign_headers, kOpenApiAcceptHeader));
    PAIMON_ASSIGN_OR_RAISE(std::string date, RequiredHeader(sign_headers, kOpenApiDateHeader));
    std::string content_md5;
    auto md5_iter = sign_headers.find(kContentMd5Header);
    if (md5_iter != sign_headers.end()) {
        content_md5 = md5_iter->second;
    }
    std::string content_type;
    auto type_iter = sign_headers.find(kContentTypeHeader);
    if (type_iter != sign_headers.end()) {
        content_type = type_iter->second;
    }
    std::string string_to_sign =
        parameter.method + "\n" + accept + "\n" + content_md5 + "\n" + content_type + "\n" + date +
        "\n" + OpenApiCanonicalizedHeaders(sign_headers) + OpenApiCanonicalizedResource(parameter);
    PAIMON_ASSIGN_OR_RAISE(Bytes signature,
                           Hmac(EVP_sha1(), ToBytes(token.GetAccessKeySecret()), string_to_sign));
    PAIMON_ASSIGN_OR_RAISE(std::string encoded_signature, Base64Encode(signature));
    return fmt::format("acs {}:{}", token.GetAccessKeyId(), encoded_signature);
}

DlfAuthProvider::DlfAuthProvider(std::unique_ptr<DlfTokenLoader> token_loader,
                                 const std::optional<DlfToken>& token, const std::string& host,
                                 std::unique_ptr<DlfRequestSigner> signer, Clock clock)
    : token_loader_(std::move(token_loader)),
      token_(token),
      host_(host),
      signer_(std::move(signer)),
      clock_(std::move(clock)) {}

Result<std::unique_ptr<DlfAuthProvider>> DlfAuthProvider::Create(
    const std::map<std::string, std::string>& options) {
    PAIMON_ASSIGN_OR_RAISE(std::string uri, RequiredNonEmptyOption(options, CatalogOptions::URI));
    std::string region;
    std::optional<std::string> configured_region = FindOption(options, CatalogOptions::DLF_REGION);
    if (configured_region) {
        if (configured_region->empty()) {
            return Status::Invalid("option 'dlf.region' must not be empty");
        }
        region = configured_region.value();
    } else {
        PAIMON_ASSIGN_OR_RAISE(region, ParseRegionFromUri(uri));
    }

    std::string algorithm;
    std::optional<std::string> configured_algorithm =
        FindOption(options, CatalogOptions::DLF_SIGNING_ALGORITHM);
    if (configured_algorithm) {
        algorithm = configured_algorithm.value();
    } else {
        algorithm = ParseSigningAlgorithmFromUri(uri);
    }

    std::optional<std::string> loader_name = FindOption(options, CatalogOptions::DLF_TOKEN_LOADER);
    std::optional<std::string> token_path = FindOption(options, CatalogOptions::DLF_TOKEN_PATH);
    if (loader_name) {
        if (loader_name.value() == "ecs") {
            std::string metadata_url =
                FindOption(options, CatalogOptions::DLF_TOKEN_ECS_METADATA_URL)
                    .value_or(kDefaultEcsMetadataUrl);
            std::optional<std::string> role_name =
                FindOption(options, CatalogOptions::DLF_TOKEN_ECS_ROLE_NAME);
            return FromTokenLoader(DlfEcsTokenLoader::Create(metadata_url, role_name), uri, region,
                                   algorithm);
        }
        if (loader_name.value() == "local_file") {
            PAIMON_ASSIGN_OR_RAISE(std::string path,
                                   RequiredNonEmptyOption(options, CatalogOptions::DLF_TOKEN_PATH));
            return FromTokenLoader(std::make_unique<DlfLocalFileTokenLoader>(path), uri, region,
                                   algorithm);
        }
        return Status::NotImplemented(
            fmt::format("unsupported DLF token loader '{}', supported values are 'ecs' and "
                        "'local_file'",
                        loader_name.value()));
    }
    if (token_path) {
        if (token_path->empty()) {
            return Status::Invalid("option 'dlf.token-path' must not be empty");
        }
        return FromTokenLoader(std::make_unique<DlfLocalFileTokenLoader>(token_path.value()), uri,
                               region, algorithm);
    }

    std::optional<std::string> access_key_id =
        FindOption(options, CatalogOptions::DLF_ACCESS_KEY_ID);
    std::optional<std::string> access_key_secret =
        FindOption(options, CatalogOptions::DLF_ACCESS_KEY_SECRET);
    if (access_key_id && !access_key_id->empty() && access_key_secret &&
        !access_key_secret->empty()) {
        DlfToken token(access_key_id.value(), access_key_secret.value(),
                       FindOption(options, CatalogOptions::DLF_SECURITY_TOKEN));
        return FromAccessKey(token, uri, region, algorithm);
    }
    return Status::Invalid("DLF token path or access key must be configured for DLF auth");
}

Result<std::unique_ptr<DlfAuthProvider>> DlfAuthProvider::FromAccessKey(
    const DlfToken& token, const std::string& uri, const std::string& region,
    const std::string& signing_algorithm, Clock clock) {
    if (token.GetAccessKeyId().empty() || token.GetAccessKeySecret().empty()) {
        return Status::Invalid("DLF access key id and secret must not be empty");
    }
    PAIMON_ASSIGN_OR_RAISE(std::string host, ExtractHost(uri));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<DlfRequestSigner> signer,
                           CreateSigner(signing_algorithm, region));
    return std::unique_ptr<DlfAuthProvider>(
        new DlfAuthProvider(nullptr, token, host, std::move(signer), std::move(clock)));
}

Result<std::unique_ptr<DlfAuthProvider>> DlfAuthProvider::FromTokenLoader(
    std::unique_ptr<DlfTokenLoader> token_loader, const std::string& uri, const std::string& region,
    const std::string& signing_algorithm, Clock clock) {
    if (!token_loader) {
        return Status::Invalid("DLF token loader must not be null");
    }
    PAIMON_ASSIGN_OR_RAISE(std::string host, ExtractHost(uri));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<DlfRequestSigner> signer,
                           CreateSigner(signing_algorithm, region));
    return std::unique_ptr<DlfAuthProvider>(new DlfAuthProvider(
        std::move(token_loader), std::nullopt, host, std::move(signer), std::move(clock)));
}

Result<DlfToken> DlfAuthProvider::GetFreshToken(std::chrono::system_clock::time_point now) const {
    std::scoped_lock lock(token_mutex_);
    if (token_ && !token_->ShouldRefresh(now)) {
        return token_.value();
    }
    if (!token_loader_) {
        return Status::Invalid("DLF credentials expired and no token loader is configured");
    }
    PAIMON_ASSIGN_OR_RAISE(DlfToken loaded, token_loader_->LoadToken());
    if (loaded.GetAccessKeyId().empty() || loaded.GetAccessKeySecret().empty()) {
        return Status::Invalid("DLF token loader returned empty access key credentials");
    }
    token_ = loaded;
    return loaded;
}

Result<std::map<std::string, std::string>> DlfAuthProvider::MergeAuthHeader(
    const std::map<std::string, std::string>& base_header,
    const RestAuthParameter& parameter) const {
    PAIMON_ASSIGN_OR_RAISE(DlfToken token, GetFreshToken(clock_()));
    std::chrono::system_clock::time_point signing_time = clock_();
    PAIMON_ASSIGN_OR_RAISE(
        DlfRequestSigner::Headers sign_headers,
        signer_->SignHeaders(parameter.data, signing_time, token.GetSecurityToken(), host_));
    PAIMON_ASSIGN_OR_RAISE(std::string authorization,
                           signer_->Authorization(parameter, token, host_, sign_headers));
    std::map<std::string, std::string> headers = base_header;
    for (const auto& [key, value] : sign_headers) {
        headers[key] = value;
    }
    headers[kAuthorizationHeader] = authorization;
    return headers;
}

Result<std::string> DlfAuthProvider::ParseRegionFromUri(const std::string& uri) {
    static const std::regex kRegionPattern("(?:pre-)?([a-z]+-[a-z]+(?:-[0-9]+)?)");
    std::smatch match;
    if (std::regex_search(uri, match, kRegionPattern) && match.size() > 1 &&
        !match.str(1).empty()) {
        return match.str(1);
    }
    return Status::Invalid(
        "could not determine DLF region from option 'dlf.region' or REST catalog URI");
}

std::string DlfAuthProvider::ParseSigningAlgorithmFromUri(const std::string& uri) {
    std::string lower_uri = StringUtils::ToLowerCase(uri);
    return lower_uri.find("dlfnext") == std::string::npos ? DlfDefaultSigner::kIdentifier
                                                          : DlfOpenApiSigner::kIdentifier;
}

Result<std::string> DlfAuthProvider::ExtractHost(const std::string& uri) {
    std::string host = uri;
    std::string lower_uri = StringUtils::ToLowerCase(uri);
    if (StringUtils::StartsWith(lower_uri, "http://")) {
        host.erase(0, 7);
    } else if (StringUtils::StartsWith(lower_uri, "https://")) {
        host.erase(0, 8);
    }
    size_t path = host.find('/');
    if (path != std::string::npos) {
        host.resize(path);
    }
    if (host.empty() || host.find('?') != std::string::npos ||
        host.find('#') != std::string::npos || host.find('@') != std::string::npos) {
        return Status::Invalid("could not determine DLF signing host from REST catalog URI");
    }
    return host;
}

}  // namespace paimon

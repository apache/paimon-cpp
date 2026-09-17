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

#include "paimon/fs/credential_provider_factory.h"

#include <map>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/fs/local/local_file_system_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

/// Credentials read straight out of the options of the path they authenticate, which is
/// enough to tell whether the factory handed them the path and options it was asked with.
class PathCredentialProvider : public CredentialProvider {
 public:
    PathCredentialProvider(std::string path, std::string secret)
        : path_(std::move(path)), secret_(std::move(secret)) {}

    Result<std::map<std::string, std::string>> GetCredentials() const override {
        return std::map<std::string, std::string>{{"path", path_}, {"secret", secret_}};
    }

 private:
    std::string path_;
    std::string secret_;
};

class TestCredentialProviderFactory : public CredentialProviderFactory {
 public:
    static const char IDENTIFIER[];

    const char* Identifier() const override {
        return IDENTIFIER;
    }

    Result<std::shared_ptr<CredentialProvider>> Create(
        const std::string& path, const std::map<std::string, std::string>& options) const override {
        auto secret = options.find("test.secret");
        if (secret == options.end()) {
            return Status::Invalid("option 'test.secret' is required");
        }
        if (secret->second == "none") {
            return std::shared_ptr<CredentialProvider>(nullptr);
        }
        return std::make_shared<PathCredentialProvider>(path, secret->second);
    }
};

const char TestCredentialProviderFactory::IDENTIFIER[] = "test-credential-provider";

}  // namespace

REGISTER_PAIMON_FACTORY(TestCredentialProviderFactory);

TEST(CredentialProviderFactoryTest, TestGet) {
    std::map<std::string, std::string> options{{"test.secret", "token"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CredentialProvider> provider,
                         CredentialProviderFactory::Get(TestCredentialProviderFactory::IDENTIFIER,
                                                        "oss://bucket/table", options));
    ASSERT_OK_AND_ASSIGN(auto credentials, provider->GetCredentials());
    ASSERT_EQ(credentials["path"], "oss://bucket/table");
    ASSERT_EQ(credentials["secret"], "token");
}

TEST(CredentialProviderFactoryTest, TestGetUnregisteredIdentifier) {
    ASSERT_NOK_WITH_MSG(
        CredentialProviderFactory::Get("no-such-provider", "oss://bucket/table", {}).status(),
        "Create factory failed with identifier 'no-such-provider'");
}

TEST(CredentialProviderFactoryTest, TestGetIdentifierOfAnotherFactoryKind) {
    // All factories share one identifier space, so asking for one that is not a credential
    // provider factory has to be reported rather than mistaken for one.
    ASSERT_NOK_WITH_MSG(
        CredentialProviderFactory::Get(LocalFileSystemFactory::IDENTIFIER, "/tmp/table", {})
            .status(),
        "Failed to cast credential provider factory");
}

TEST(CredentialProviderFactoryTest, TestGetPropagatesFactoryFailure) {
    ASSERT_NOK_WITH_MSG(CredentialProviderFactory::Get(TestCredentialProviderFactory::IDENTIFIER,
                                                       "oss://bucket/table", {})
                            .status(),
                        "option 'test.secret' is required");
}

TEST(CredentialProviderFactoryTest, TestGetRejectsNullProvider) {
    std::map<std::string, std::string> options{{"test.secret", "none"}};
    ASSERT_NOK_WITH_MSG(CredentialProviderFactory::Get(TestCredentialProviderFactory::IDENTIFIER,
                                                       "oss://bucket/table", options)
                            .status(),
                        "created a null provider");
}

TEST(CredentialProviderFactoryTest, TestGetIfConfigured) {
    std::map<std::string, std::string> options{
        {CredentialProviderFactory::CREDENTIAL_PROVIDER_OPTION,
         TestCredentialProviderFactory::IDENTIFIER},
        {"test.secret", "token"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CredentialProvider> provider,
                         CredentialProviderFactory::GetIfConfigured("oss://bucket/table", options));
    ASSERT_NE(provider, nullptr);
    ASSERT_OK_AND_ASSIGN(auto credentials, provider->GetCredentials());
    ASSERT_EQ(credentials["secret"], "token");
}

TEST(CredentialProviderFactoryTest, TestGetIfConfiguredWithoutOption) {
    // No provider named means the file system options authenticate the accesses themselves.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CredentialProvider> provider,
                         CredentialProviderFactory::GetIfConfigured("/tmp/table", {}));
    ASSERT_EQ(provider, nullptr);

    std::map<std::string, std::string> empty_option{
        {CredentialProviderFactory::CREDENTIAL_PROVIDER_OPTION, ""}};
    ASSERT_OK_AND_ASSIGN(provider,
                         CredentialProviderFactory::GetIfConfigured("/tmp/table", empty_option));
    ASSERT_EQ(provider, nullptr);
}

}  // namespace paimon::test

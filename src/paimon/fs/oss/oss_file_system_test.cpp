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

#include "paimon/fs/oss/oss_file_system.h"

#include <chrono>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "alibabacloud/oss2/ClientConfiguration.h"
#include "alibabacloud/oss2/OSSClient.h"
#include "alibabacloud/oss2/credentials/CredentialsProvider.h"
#include "alibabacloud/oss2/io/ByteWriter.h"
#include "alibabacloud/oss2/transport/HttpTransport.h"
#include "gtest/gtest.h"
#include "paimon/fs/oss/oss_file_system_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::oss {
namespace {

namespace oss2 = alibabacloud::oss2;

class MockHttpTransport : public oss2::HttpTransport {
 public:
    oss2::ResponseResult send(std::unique_ptr<oss2::RequestMessage>& request,
                              const oss2::RequestOptions& options) override {
        requests_.emplace_back(std::make_unique<oss2::RequestMessage>(*request));
        if (responses_.empty()) {
            return oss2::TransportError{std::make_error_code(std::errc::no_message_available), "",
                                        ""};
        }
        std::unique_ptr<oss2::ResponseMessage> response = std::move(responses_.front());
        responses_.erase(responses_.begin());
        if (response->statusCode / 100 == 2 && options.sinkFactory.has_value() &&
            response->body != nullptr) {
            int64_t content_length = -1;
            auto content_length_header = response->headers.find("Content-Length");
            if (content_length_header != response->headers.end()) {
                content_length = std::stoll(content_length_header->second);
            }
            std::shared_ptr<oss2::ByteWriter> sink =
                options.sinkFactory.value()(content_length, response->headers);
            std::ostringstream body;
            body << response->body->rdbuf();
            const std::string data = body.str();
            sink->write(reinterpret_cast<const uint8_t*>(data.data()), data.size());
            response->body.reset();
        }
        return response;
    }

    std::string getName() const override {
        return "MockHttpTransport";
    }

    void AddResponse(int status_code, oss2::HeaderCollection headers, std::string body = "") {
        std::shared_ptr<std::iostream> response_body;
        if (!body.empty()) {
            response_body = std::make_shared<std::stringstream>(std::move(body));
        }
        responses_.emplace_back(std::make_unique<oss2::ResponseMessage>(oss2::ResponseMessage{
            status_code, "", std::move(headers), std::move(response_body), nullptr}));
    }

    std::vector<std::unique_ptr<oss2::ResponseMessage>> responses_;
    std::vector<std::unique_ptr<oss2::RequestMessage>> requests_;
};

std::unique_ptr<OssFileSystem> CreateFileSystem(
    const std::shared_ptr<MockHttpTransport>& transport) {
    oss2::ClientConfiguration config = oss2::ClientConfiguration::loadDefault();
    config.region = "cn-hangzhou";
    config.credentialsProvider =
        std::make_shared<oss2::StaticCredentialsProvider>("access-key", "secret-key");
    config.httpTransport = transport;
    return std::make_unique<OssFileSystem>("bucket", std::make_shared<oss2::OSSClient>(config),
                                           CreateDefaultExecutor());
}

}  // namespace

TEST(OssFileSystemFactoryTest, TestOptionValidation) {
    OssFileSystemFactory factory;
    std::map<std::string, std::string> options;
    ASSERT_NOK(factory.Create("s3://bucket/key", options));
    ASSERT_NOK(factory.Create("oss://bucket/key", options));

    options[kOssAccessKeyIdOption] = "access-key";
    options[kOssAccessKeySecretOption] = "secret-key";
    options[kOssEndpointOption] = "oss-cn-hangzhou.aliyuncs.com";
    options[kOssUsePathStyleOption] = "treu";
    ASSERT_NOK(factory.Create("oss://bucket/key", options));

    options[kOssUsePathStyleOption] = "false";
    options[kOssSignatureVersionOption] = "v2";
    ASSERT_NOK(factory.Create("oss://bucket/key", options));

    options[kOssSignatureVersionOption] = "v4";
    options[kOssExecutorThreadCountOption] = "0";
    ASSERT_NOK(factory.Create("oss://bucket/key", options));

    options[kOssExecutorThreadCountOption] = "4";
    ASSERT_OK(factory.Create("oss://bucket/key", options));

    options[kOssEndpointOption] = "";
    options[kOssRegionOption] = "cn-hangzhou";
    ASSERT_OK(factory.Create("oss://bucket/key", options));
}

TEST(OssFileSystemFactoryTest, TestBucketOptionsOverrideGlobalOptions) {
    OssFileSystemFactory factory;
    std::map<std::string, std::string> options = {
        {kOssAccessKeyIdOption, ""},
        {kOssAccessKeySecretOption, ""},
        {kOssEndpointOption, ""},
        {"fs.oss.bucket.bucket.accessKeyId", "access-key"},
        {"fs.oss.bucket.bucket.accessKeySecret", "secret-key"},
        {"fs.oss.bucket.bucket.endpoint", "oss-cn-hangzhou.aliyuncs.com"},
    };
    ASSERT_OK(factory.Create("oss://bucket/key", options));
}

TEST(OssFileSystemFactoryTest, TestBucketOptionErrorReportsBucketKey) {
    OssFileSystemFactory factory;
    std::map<std::string, std::string> options = {
        {kOssAccessKeyIdOption, "access-key"},
        {kOssAccessKeySecretOption, "secret-key"},
        {kOssEndpointOption, "oss-cn-hangzhou.aliyuncs.com"},
        {"fs.oss.bucket.bucket.accessKeyId", ""},
    };
    ASSERT_NOK_WITH_MSG(factory.Create("oss://bucket/key", options),
                        "fs.oss.bucket.bucket.accessKeyId");
}

TEST(OssFileSystemFactoryTest, TestEndpointRegionValidation) {
    OssFileSystemFactory factory;
    std::map<std::string, std::string> options = {
        {kOssAccessKeyIdOption, "access-key"},
        {kOssAccessKeySecretOption, "secret-key"},
    };

    options[kOssEndpointOption] = "oss-cn-hangzhou.aliyuncs.com";
    ASSERT_OK(factory.Create("oss://bucket/key", options));

    options[kOssEndpointOption] = "oss-cn-hangzhou-internal.aliyuncs.com";
    ASSERT_OK(factory.Create("oss://bucket/key", options));

    options[kOssEndpointOption] = "oss-ap-southeast-1.aliyuncs.com:443";
    ASSERT_OK(factory.Create("oss://bucket/key", options));

    options[kOssEndpointOption] = "cn-hangzhou.oss.aliyuncs.com";
    ASSERT_OK(factory.Create("oss://bucket/key", options));

    options[kOssEndpointOption] = "oss-accelerate.aliyuncs.com";
    ASSERT_NOK_WITH_MSG(factory.Create("oss://bucket/key", options), "OSS region must be");

    options[kOssEndpointOption] = "oss-accelerate-overseas.aliyuncs.com";
    ASSERT_NOK_WITH_MSG(factory.Create("oss://bucket/key", options), "OSS region must be");

    options[kOssEndpointOption] = "oss.example.com";
    ASSERT_NOK_WITH_MSG(factory.Create("oss://bucket/key", options), "OSS region must be");

    options[kOssRegionOption] = "cn-hangzhou";
    ASSERT_OK(factory.Create("oss://bucket/key", options));

    options.erase(kOssRegionOption);
    options[kOssSignatureVersionOption] = "v1";
    ASSERT_OK(factory.Create("oss://bucket/key", options));
}

TEST(OssFileSystemTest, TestHeadObjectParsesMetadata) {
    std::shared_ptr<MockHttpTransport> transport = std::make_shared<MockHttpTransport>();
    transport->AddResponse(200, {{"Content-Length", "3"},
                                 {"Last-Modified", "not-a-timestamp"},
                                 {"x-oss-request-id", "request-id"}});
    std::unique_ptr<OssFileSystem> file_system = CreateFileSystem(transport);

    ASSERT_OK_AND_ASSIGN(FileStatus status, file_system->GetFileStatus("oss://bucket/key"));
    ASSERT_EQ(3, status.GetLen());
    ASSERT_EQ(FileStatus::kUnknownModificationTime, status.GetModificationTime());
    ASSERT_EQ(1U, transport->requests_.size());
    ASSERT_EQ("HEAD", transport->requests_[0]->method);
}

TEST(OssFileSystemTest, TestHeadObjectNotFound) {
    std::shared_ptr<MockHttpTransport> transport = std::make_shared<MockHttpTransport>();
    transport->AddResponse(404, {{"x-oss-request-id", "request-id"}},
                           "<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>");
    transport->AddResponse(200, {},
                           "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>");
    std::unique_ptr<OssFileSystem> file_system = CreateFileSystem(transport);

    Result<FileStatus> status = file_system->GetFileStatus("oss://bucket/missing");
    ASSERT_TRUE(status.status().IsNotExist()) << status.status().ToString();
    ASSERT_NOK_WITH_MSG(status, "does not exist");
}

TEST(OssFileSystemTest, TestHeadObjectErrorMapping) {
    std::shared_ptr<MockHttpTransport> transport = std::make_shared<MockHttpTransport>();
    transport->AddResponse(403, {{"x-oss-request-id", "request-id"}},
                           "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>");
    std::unique_ptr<OssFileSystem> file_system = CreateFileSystem(transport);

    Result<FileStatus> status = file_system->GetFileStatus("oss://bucket/key");
    ASSERT_TRUE(status.status().IsIOError()) << status.status().ToString();
    ASSERT_NOK_WITH_MSG(status, "code=AccessDenied, status=403");
}

TEST(OssFileSystemTest, TestListObjects) {
    std::shared_ptr<MockHttpTransport> transport = std::make_shared<MockHttpTransport>();
    transport->AddResponse(404, {{"x-oss-request-id", "request-id"}},
                           "<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>");
    transport->AddResponse(200, {{"x-oss-request-id", "request-id"}}, R"(
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>prefix/file</Key>
    <Size>3</Size>
    <LastModified>2024-01-01T00:00:00.000Z</LastModified>
  </Contents>
  <CommonPrefixes><Prefix>prefix/sub/</Prefix></CommonPrefixes>
</ListBucketResult>)");
    std::unique_ptr<OssFileSystem> file_system = CreateFileSystem(transport);
    std::vector<FileStatus> statuses;

    ASSERT_OK(file_system->ListFileStatus("oss://bucket/prefix", &statuses));
    ASSERT_EQ(2U, statuses.size());
    ASSERT_EQ(1704067200000, statuses[0].GetModificationTime());
    ASSERT_EQ(2U, transport->requests_.size());
    ASSERT_EQ("HEAD", transport->requests_[0]->method);
    ASSERT_EQ("GET", transport->requests_[1]->method);
    ASSERT_NE(std::string::npos, transport->requests_[1]->uri.find("list-type=2"));
}

TEST(OssFileSystemTest, TestListObjectsErrorMapping) {
    std::shared_ptr<MockHttpTransport> transport = std::make_shared<MockHttpTransport>();
    transport->AddResponse(403, {{"x-oss-request-id", "request-id"}},
                           "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>");
    std::unique_ptr<OssFileSystem> file_system = CreateFileSystem(transport);
    std::vector<BasicFileStatus> statuses;

    Status status = file_system->ListDir("oss://bucket/prefix/", &statuses);
    ASSERT_TRUE(status.IsIOError()) << status.ToString();
    ASSERT_NOK_WITH_MSG(status, "code=AccessDenied, status=403");
}

TEST(OssFileSystemTest, TestGetObjectRangeAndShortRead) {
    std::shared_ptr<MockHttpTransport> transport = std::make_shared<MockHttpTransport>();
    transport->AddResponse(206, {{"Content-Length", "3"}}, "abc");
    std::unique_ptr<OssFileSystem> file_system = CreateFileSystem(transport);
    FileStatus file_status("oss://bucket/key", 3);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> stream, file_system->Open(file_status));
    char buffer[3];

    ASSERT_OK_AND_ASSIGN(int64_t bytes_read, stream->Read(buffer, 3, 0));
    ASSERT_EQ(3, bytes_read);
    ASSERT_EQ("abc", std::string(buffer, sizeof(buffer)));
    ASSERT_EQ(1U, transport->requests_.size());
    ASSERT_EQ("bytes=0-2", transport->requests_[0]->headers.at("range"));

    std::shared_ptr<MockHttpTransport> short_transport = std::make_shared<MockHttpTransport>();
    short_transport->AddResponse(206, {{"Content-Length", "2"}}, "ab");
    std::unique_ptr<OssFileSystem> short_file_system = CreateFileSystem(short_transport);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> short_stream,
                         short_file_system->Open(file_status));
    ASSERT_NOK_WITH_MSG(short_stream->Read(buffer, 3, 0), "expected 3");
}

TEST(OssFileSystemTest, TestGetObjectRangeErrorMapping) {
    std::shared_ptr<MockHttpTransport> transport = std::make_shared<MockHttpTransport>();
    transport->AddResponse(403, {{"x-oss-request-id", "request-id"}},
                           "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>");
    std::unique_ptr<OssFileSystem> file_system = CreateFileSystem(transport);
    FileStatus file_status("oss://bucket/key", 3);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> stream, file_system->Open(file_status));
    char buffer[3];

    Result<int64_t> result = stream->Read(buffer, 3, 0);
    ASSERT_TRUE(result.status().IsIOError()) << result.status().ToString();
    ASSERT_NOK_WITH_MSG(result, "code=AccessDenied, status=403");
}

TEST(OssFileSystemTest, TestGetObjectRangeAsync) {
    std::shared_ptr<MockHttpTransport> transport = std::make_shared<MockHttpTransport>();
    transport->AddResponse(206, {{"Content-Length", "3"}}, "abc");
    std::unique_ptr<OssFileSystem> file_system = CreateFileSystem(transport);
    FileStatus file_status("oss://bucket/key", 3);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> stream, file_system->Open(file_status));
    char buffer[3];
    std::promise<Status> promise;
    std::future<Status> future = promise.get_future();

    stream->ReadAsync(buffer, 3, 0,
                      [&promise](Status status) { promise.set_value(std::move(status)); });

    ASSERT_EQ(std::future_status::ready, future.wait_for(std::chrono::seconds(5)));
    ASSERT_OK(future.get());
    ASSERT_EQ("abc", std::string(buffer, sizeof(buffer)));
    ASSERT_EQ(1U, transport->requests_.size());
    ASSERT_EQ("bytes=0-2", transport->requests_[0]->headers.at("range"));
}

}  // namespace paimon::oss

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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>

#include "alibabacloud/oss2/OSSClient.h"
#include "alibabacloud/oss2/Operation.h"
#include "alibabacloud/oss2/Types.h"
#include "alibabacloud/oss2/io/ByteWriter.h"
#include "alibabacloud/oss2/models/BucketBasic.h"
#include "alibabacloud/oss2/models/ObjectBasic.h"
#include "fmt/format.h"
#include "paimon/executor.h"

namespace paimon::oss {
namespace {

namespace oss2 = alibabacloud::oss2;

constexpr int32_t kMaxKeysPerRequest = 1000;

bool IsNotFoundError(const oss2::OperationError& error) {
    return error.getStatusCode() == 404 || error.getCode() == "NoSuchKey" ||
           error.getCode() == "NoSuchBucket" || error.getCode() == "NotFound";
}

Status ToPaimonStatus(const oss2::OperationError& error, const std::string& operation,
                      const ObjectStorePath& path) {
    std::string message = fmt::format("OSS {} 'oss://{}/{}' failed: code={}, status={}, message={}",
                                      operation, path.bucket, path.key, error.getCode(),
                                      error.getStatusCode(), error.getMessage());
    if (!error.getRequestId().empty()) {
        message += fmt::format(", request_id={}", error.getRequestId());
    }
    if (IsNotFoundError(error)) {
        return Status::NotExist(message);
    }
    if (error.getCode() == "RequestCanceled") {
        return Status::Cancelled(message);
    }
    return Status::IOError(message);
}

class OssObjectStoreClient : public ObjectStoreClient,
                             public std::enable_shared_from_this<OssObjectStoreClient> {
 public:
    OssObjectStoreClient(std::string bucket, std::shared_ptr<oss2::OSSClient> client,
                         std::unique_ptr<Executor> executor)
        : bucket_(std::move(bucket)), client_(std::move(client)), executor_(std::move(executor)) {}

    Result<ObjectMetadata> HeadObject(const ObjectStorePath& path) const override {
        PAIMON_RETURN_NOT_OK(ValidateBucket(path));
        oss2::models::HeadObjectRequest request;
        request.setBucket(path.bucket).setKey(path.key);
        oss2::HeadObjectOutcome outcome = client_->headObject(request);
        if (!outcome.has_value()) {
            return ToPaimonStatus(outcome.error(), "HeadObject", path);
        }
        const oss2::models::HeadObjectResult& result = outcome.value();
        if (result.getContentLength() < 0) {
            return Status::IOError("OSS HeadObject response is missing Content-Length");
        }
        return ObjectMetadata{
            path.key, result.getContentLength(),
            ObjectStoreFileSystemUtils::ParseModificationTime(result.getLastModified())};
    }

    Result<ListObjectsResult> ListObjects(const ObjectStorePath& path,
                                          const std::string& continuation_token,
                                          int32_t max_keys) const override {
        PAIMON_RETURN_NOT_OK(ValidateBucket(path));
        oss2::models::ListObjectsV2Request request;
        request.setBucket(path.bucket).setPrefix(path.key).setDelimiter("/");
        if (!continuation_token.empty()) {
            request.setContinuationToken(continuation_token);
        }
        if (max_keys > 0) {
            // OSS limits ListObjectsV2 requests to 1000 keys.
            request.setMaxKeys(std::min(max_keys, kMaxKeysPerRequest));
        }
        oss2::ListObjectsV2Outcome outcome = client_->listObjectsV2(request);
        if (!outcome.has_value()) {
            return ToPaimonStatus(outcome.error(), "ListObjectsV2", path);
        }
        const oss2::models::ListObjectsV2Result& value = outcome.value();
        ListObjectsResult result;
        result.objects.reserve(value.getContents().size());
        for (const oss2::models::ObjectSummary& object : value.getContents()) {
            result.objects.push_back(ObjectMetadata{
                object.key, object.size,
                ObjectStoreFileSystemUtils::ParseModificationTime(object.lastModified)});
        }
        result.common_prefixes.reserve(value.getCommonPrefixes().size());
        for (const oss2::models::CommonPrefix& prefix : value.getCommonPrefixes()) {
            result.common_prefixes.push_back(prefix.prefix);
        }
        result.is_truncated = value.getIsTruncated();
        result.continuation_token = value.getNextContinuationToken();
        return result;
    }

    Result<int64_t> GetObjectRange(const ObjectStorePath& path, int64_t offset, int64_t size,
                                   char* buffer) const override {
        PAIMON_RETURN_NOT_OK(ValidateBucket(path));
        if (size == 0) {
            return 0;
        }
        auto writer = std::make_shared<std::shared_ptr<oss2::MemoryWriter>>();
        oss2::SinkFactory sink;
        sink.isOneShot = false;
        sink.supplier = [buffer, size, writer](int64_t, const oss2::HeaderCollection&) {
            auto memory_writer = std::make_shared<oss2::MemoryWriter>(
                reinterpret_cast<uint8_t*>(buffer), static_cast<size_t>(size));
            *writer = memory_writer;
            return memory_writer;
        };
        oss2::models::GetObjectRequest request;
        request.setBucket(path.bucket)
            .setKey(path.key)
            .setRange(fmt::format("bytes={}-{}", offset, offset + size - 1))
            .setRangeBehavior("standard")
            .setSinkFactory(std::move(sink));
        oss2::GetObjectOutcome outcome = client_->getObject(request);
        if (!outcome.has_value()) {
            return ToPaimonStatus(outcome.error(), "GetObject", path);
        }
        int64_t written =
            *writer ? static_cast<int64_t>((*writer)->written()) : static_cast<int64_t>(0);
        if (written != size) {
            return Status::IOError(
                fmt::format("OSS GetObject read {} bytes for oss://{}/{}, expected {}", written,
                            path.bucket, path.key, size));
        }
        return written;
    }

    void GetObjectRangeAsync(const ObjectStorePath& path, int64_t offset, int64_t size,
                             char* buffer, std::function<void(Status)>&& callback) const override {
        std::shared_ptr<const OssObjectStoreClient> self = shared_from_this();
        executor_->Add([self = std::move(self), path, offset, size, buffer,
                        callback = std::move(callback)]() mutable {
            Result<int64_t> result = self->GetObjectRange(path, offset, size, buffer);
            callback(result.ok() ? Status::OK() : result.status());
        });
    }

 private:
    Status ValidateBucket(const ObjectStorePath& path) const {
        if (path.bucket != bucket_) {
            return Status::Invalid(
                fmt::format("OSS file system for bucket '{}' cannot access "
                            "'oss://{}/{}'",
                            bucket_, path.bucket, path.key));
        }
        return Status::OK();
    }

    std::string bucket_;
    std::shared_ptr<oss2::OSSClient> client_;
    std::unique_ptr<Executor> executor_;
};

}  // namespace

OssFileSystem::OssFileSystem(std::string bucket, std::shared_ptr<oss2::OSSClient> client,
                             std::unique_ptr<Executor> executor)
    : ObjectStoreFileSystem("oss", std::make_shared<OssObjectStoreClient>(
                                       std::move(bucket), std::move(client), std::move(executor))) {
}

}  // namespace paimon::oss

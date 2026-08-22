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

#pragma once

#include <curl/curl.h>

#include <cctype>
#include <cerrno>
#include <cstdint>
#include <ctime>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "paimon/fs/file_system.h"
#include "paimon/visibility.h"

namespace paimon {

class ObjectStoreFileSystemUtils {
 public:
    static inline int64_t ParseModificationTime(const std::string& value) {
        std::tm parsed_time{};
        const char* current = strptime(value.c_str(), "%Y-%m-%dT%H:%M:%S", &parsed_time);
        if (current != nullptr) {
            int32_t milliseconds = 0;
            int32_t fraction_digits = 0;
            if (*current == '.') {
                ++current;
                const char* fraction_begin = current;
                while (std::isdigit(static_cast<unsigned char>(*current))) {
                    if (fraction_digits < 3) {
                        milliseconds = milliseconds * 10 + (*current - '0');
                    }
                    ++fraction_digits;
                    ++current;
                }
                if (current == fraction_begin) {
                    current = nullptr;
                }
                while (fraction_digits < 3) {
                    milliseconds *= 10;
                    ++fraction_digits;
                }
            }

            int32_t timezone_offset_seconds = 0;
            bool valid_timezone = false;
            if (current != nullptr && *current == 'Z' && current[1] == '\0') {
                valid_timezone = true;
            } else if (current != nullptr && (*current == '+' || *current == '-') &&
                       std::isdigit(static_cast<unsigned char>(current[1])) &&
                       std::isdigit(static_cast<unsigned char>(current[2])) && current[3] == ':' &&
                       std::isdigit(static_cast<unsigned char>(current[4])) &&
                       std::isdigit(static_cast<unsigned char>(current[5])) && current[6] == '\0') {
                int32_t hours = (current[1] - '0') * 10 + current[2] - '0';
                int32_t minutes = (current[4] - '0') * 10 + current[5] - '0';
                if (hours <= 23 && minutes <= 59) {
                    timezone_offset_seconds = (hours * 60 + minutes) * 60;
                    if (*current == '-') {
                        timezone_offset_seconds = -timezone_offset_seconds;
                    }
                    valid_timezone = true;
                }
            }

            if (valid_timezone) {
                errno = 0;
                time_t seconds = timegm(&parsed_time);
                if (seconds != static_cast<time_t>(-1) || errno != EOVERFLOW) {
                    int64_t utc_seconds = static_cast<int64_t>(seconds) - timezone_offset_seconds;
                    return utc_seconds * 1000 + milliseconds;
                }
            }
        }

        time_t seconds = curl_getdate(value.c_str(), nullptr);
        if (seconds == static_cast<time_t>(-1)) {
            return FileStatus::kUnknownModificationTime;
        }
        return static_cast<int64_t>(seconds) * 1000;
    }
};

struct ObjectStorePath {
    std::string bucket;
    std::string key;
};

struct ObjectMetadata {
    std::string key;
    int64_t size = 0;
    int64_t modification_time = FileStatus::kUnknownModificationTime;
};

struct ListObjectsResult {
    std::vector<ObjectMetadata> objects;
    std::vector<std::string> common_prefixes;
    std::string continuation_token;
    bool is_truncated = false;
};

class PAIMON_EXPORT ObjectStoreClient {
 public:
    virtual ~ObjectStoreClient() = default;

    virtual Result<ObjectMetadata> HeadObject(const ObjectStorePath& path) const = 0;
    virtual Result<ListObjectsResult> ListObjects(const ObjectStorePath& path,
                                                  const std::string& continuation_token,
                                                  int32_t max_keys) const = 0;
    virtual Result<int64_t> GetObjectRange(const ObjectStorePath& path, int64_t offset,
                                           int64_t size, char* buffer) const = 0;
    virtual void GetObjectRangeAsync(const ObjectStorePath& path, int64_t offset, int64_t size,
                                     char* buffer,
                                     std::function<void(Status)>&& callback) const = 0;
};

class ReadAheadMemoryLimiter {
 public:
    explicit ReadAheadMemoryLimiter(int64_t limit);

    int64_t ReserveUpTo(int64_t min_size, int64_t max_size);
    void Release(int64_t size);

 private:
    int64_t limit_;
    int64_t used_ = 0;
    std::mutex mutex_;
};

class PAIMON_EXPORT ObjectStoreFileSystem : public FileSystem {
 public:
    ObjectStoreFileSystem(std::string scheme, std::shared_ptr<ObjectStoreClient> client);
    ObjectStoreFileSystem(std::string scheme, std::shared_ptr<ObjectStoreClient> client,
                          int64_t read_ahead_memory_limit);
    ~ObjectStoreFileSystem() override = default;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override;
    Result<std::unique_ptr<InputStream>> Open(const FileStatus& file_status) const override;
    Result<FileStatus> GetFileStatus(const std::string& path) const override;
    Status ListDir(const std::string& directory,
                   std::vector<BasicFileStatus>* file_status_list) const override;
    Status ListFileStatus(const std::string& path,
                          std::vector<FileStatus>* file_status_list) const override;
    Result<bool> Exists(const std::string& path) const override;

    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override;
    Status Mkdirs(const std::string& path) const override;
    Status Rename(const std::string& src, const std::string& dst) const override;
    Status Delete(const std::string& path, bool recursive = true) const override;
    Status WriteFile(const std::string& path, const std::string& content, bool overwrite) override;
    Status AtomicStore(const std::string& path, const std::string& content) override;

 protected:
    Result<ObjectStorePath> ParsePath(const std::string& path) const;
    std::string ToUri(const ObjectStorePath& path, bool is_dir = false) const;

 private:
    Result<bool> DirectoryExists(const ObjectStorePath& path) const;
    Status ListDirectory(const ObjectStorePath& path, std::vector<BasicFileStatus>* basic_statuses,
                         std::vector<FileStatus>* statuses) const;
    Status ReadOnlyStatus() const;

    std::string scheme_;
    std::shared_ptr<ObjectStoreClient> client_;
    std::shared_ptr<ReadAheadMemoryLimiter> read_ahead_limiter_;
};

}  // namespace paimon

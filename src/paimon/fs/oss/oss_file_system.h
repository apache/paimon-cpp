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

#include <memory>
#include <string>

#include "paimon/common/fs/object_store_file_system.h"
#include "paimon/executor.h"

namespace alibabacloud::oss2 {
class OSSClient;
}

namespace paimon::oss {

inline constexpr char kOssAccessKeyIdOption[] = "fs.oss.accessKeyId";
inline constexpr char kOssAccessKeySecretOption[] = "fs.oss.accessKeySecret";
inline constexpr char kOssEndpointOption[] = "fs.oss.endpoint";
inline constexpr char kOssRegionOption[] = "fs.oss.region";
inline constexpr char kOssSignatureVersionOption[] = "fs.oss.signatureVersion";
inline constexpr char kOssSecurityTokenOption[] = "fs.oss.securityToken";
inline constexpr char kOssSessionTokenOption[] = "fs.oss.sessionToken";
inline constexpr char kOssUsePathStyleOption[] = "fs.oss.usePathStyle";
inline constexpr char kOssExecutorThreadCountOption[] = "fs.oss.executor.thread-count";

class OssFileSystem : public ObjectStoreFileSystem {
 public:
    OssFileSystem(std::string bucket, std::shared_ptr<alibabacloud::oss2::OSSClient> client,
                  std::unique_ptr<Executor> executor);
    ~OssFileSystem() override = default;
};

}  // namespace paimon::oss

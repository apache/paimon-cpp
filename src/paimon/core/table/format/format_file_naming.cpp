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

#include "paimon/core/table/format/format_file_naming.h"

#include "fmt/format.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/core/utils/partition_path_utils.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace paimon {

namespace {

/// Fails when `value` is anything other than part of a single file name.
Status CheckFileNameComponent(const std::string& value, const std::string& what) {
    if (value.find('/') != std::string::npos || value.find('\\') != std::string::npos) {
        return Status::Invalid(
            fmt::format("{} '{}' cannot contain a path separator: it names part of one file, not "
                        "a path",
                        what, value));
    }
    if (value == "." || value == ".." || value.find("..") != std::string::npos) {
        return Status::Invalid(fmt::format("{} '{}' cannot contain '..'", what, value));
    }
    return Status::OK();
}

}  // namespace

Result<FormatFileNaming> FormatFileNaming::Create(const std::string& extension,
                                                  const std::string& prefix) {
    if (extension.empty()) {
        return Status::Invalid("format table file naming requires a file extension");
    }
    // Both go straight into a file name joined onto a directory, and the file is created before
    // any commit sees it, so this is the only place a separator or a `..` can be stopped.
    PAIMON_RETURN_NOT_OK(CheckFileNameComponent(prefix, Options::DATA_FILE_PREFIX));
    PAIMON_RETURN_NOT_OK(CheckFileNameComponent(extension, "file extension"));
    // A hidden prefix would name files this table's own scan skips.
    if (PartitionPathUtils::IsHiddenName(prefix)) {
        return Status::Invalid(
            fmt::format("{} '{}' cannot start with '_' or '.': a scan skips every file whose name "
                        "does",
                        Options::DATA_FILE_PREFIX, prefix));
    }
    std::string uuid;
    if (!UUID::Generate(&uuid)) {
        return Status::Invalid("failed to generate uuid for format table file naming");
    }
    return FormatFileNaming(uuid, extension, prefix);
}

std::string FormatFileNaming::NextFileName() {
    return fmt::format("{}{}-{}.{}", prefix_, uuid_, file_count_++, extension_);
}

Result<std::string> FormatFileNaming::NextTempFilePath() {
    // A uuid of its own rather than this write's, as Java Paimon does: `_temporary` is shared,
    // and a name derived from the target would collide with a retry of the same write.
    std::string uuid;
    if (!UUID::Generate(&uuid)) {
        return Status::Invalid("failed to generate uuid for a staged format table file");
    }
    return fmt::format("{}/{}{}", kTempDirName, kTempFilePrefix, uuid);
}

bool FormatFileNaming::IsTempFilePath(const std::string& relative_path) {
    const std::string directory_prefix = std::string(kTempDirName) + "/";
    if (!StringUtils::StartsWith(relative_path, directory_prefix)) {
        return false;
    }
    const std::string name = relative_path.substr(directory_prefix.size());
    // One level below `_temporary` only: a deeper path is another job's staging tree, not ours.
    return name.find('/') == std::string::npos && StringUtils::StartsWith(name, kTempFilePrefix) &&
           name.size() > std::string(kTempFilePrefix).size();
}

}  // namespace paimon

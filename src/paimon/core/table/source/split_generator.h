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
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "paimon/common/utils/scan_cancellation.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/result.h"

namespace paimon {
struct DataFileMeta;

/// Generate splits from `DataFileMeta`s.
class SplitGenerator {
 public:
    struct SplitGroup {
        static SplitGroup RawConvertibleGroup(std::vector<std::shared_ptr<DataFileMeta>>&& files) {
            return SplitGroup(std::move(files), true);
        }

        static SplitGroup NonRawConvertibleGroup(
            std::vector<std::shared_ptr<DataFileMeta>>&& files) {
            return SplitGroup(std::move(files), false);
        }

        bool operator==(const SplitGroup& other) const {
            if (this == &other) {
                return true;
            }
            if (files.size() != other.files.size()) {
                return false;
            }
            for (size_t i = 0; i < files.size(); ++i) {
                if (!(*(files[i]) == *(other.files[i]))) {
                    return false;
                }
            }
            return raw_convertible == other.raw_convertible;
        }

        std::vector<std::shared_ptr<DataFileMeta>> files;
        bool raw_convertible;

     private:
        SplitGroup(std::vector<std::shared_ptr<DataFileMeta>>&& _files, bool _raw_convertible)
            : files(std::move(_files)), raw_convertible(_raw_convertible) {}
    };

 public:
    virtual ~SplitGenerator() = default;
    virtual Result<std::vector<SplitGroup>> SplitForBatch(
        std::vector<std::shared_ptr<DataFileMeta>>&& files) const = 0;

    virtual Result<std::vector<SplitGroup>> SplitForBatchWithCancellation(
        std::vector<std::shared_ptr<DataFileMeta>>&& files,
        const ScanCancellationToken* token) const {
        PAIMON_RETURN_NOT_OK(CheckScanCancellation(token));
        if (token) {
            return Status::NotImplemented(
                "split generator does not support cooperative cancellation");
        }
        return SplitForBatch(std::move(files));
    }

    virtual Result<std::vector<SplitGroup>> SplitForStreaming(
        std::vector<std::shared_ptr<DataFileMeta>>&& files) const = 0;
};
}  // namespace paimon

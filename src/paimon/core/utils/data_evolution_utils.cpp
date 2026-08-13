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

#include "paimon/core/utils/data_evolution_utils.h"

#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/vector_store_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/status.h"

namespace paimon {

bool DataEvolutionUtils::IsNormalFile(const std::string& file_name) {
    return !BlobUtils::IsBlobFile(file_name) && !VectorStoreUtils::IsVectorStoreFile(file_name);
}

Result<std::shared_ptr<DataFileMeta>> DataEvolutionUtils::RetrieveAnchorFile(
    const std::vector<std::shared_ptr<DataFileMeta>>& files) {
    std::shared_ptr<DataFileMeta> anchor;
    for (const auto& file : files) {
        if (!IsNormalFile(file->file_name)) {
            continue;
        }
        if (anchor == nullptr || file->max_sequence_number < anchor->max_sequence_number ||
            (file->max_sequence_number == anchor->max_sequence_number &&
             file->file_name < anchor->file_name)) {
            anchor = file;
        }
    }
    if (anchor == nullptr) {
        return Status::Invalid(
            "Data-evolution deletion vectors should have a normal anchor file in each row range "
            "group.");
    }
    return anchor;
}

}  // namespace paimon

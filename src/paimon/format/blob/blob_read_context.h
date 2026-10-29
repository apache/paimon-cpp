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

#include <cstdint>
#include <memory>
#include <optional>

#include "paimon/visibility.h"

namespace paimon {

struct BlobBatchRequest {
    /// Upper bound on selected rows before outer deletion-vector filtering.
    int32_t max_rows;
    bool read_payload;
};

/// Shared by one file's blob reader and its fallback segment. A request stays active for the
/// entire outer NextBatchWithBitmap() call, including retries for fully deleted batches.
/// Access is confined to the thread reading that segment.
struct BlobReadContext {
    std::optional<BlobBatchRequest> request;
};

/// Internal builder hook used before the format reader is wrapped. Only the blob builder
/// implements it; neither ReaderBuilder nor the wrapping readers need blob-specific methods.
class PAIMON_EXPORT BlobReadContextAwareBuilder {
 public:
    virtual ~BlobReadContextAwareBuilder() = default;

    virtual void SetBlobReadContext(const std::shared_ptr<BlobReadContext>& context) = 0;
};

}  // namespace paimon

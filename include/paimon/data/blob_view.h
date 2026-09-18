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
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

/// Coordinates of a BLOB value stored in an upstream table.
struct PAIMON_EXPORT BlobView {
    Identifier identifier;
    int32_t field_id;
    int64_t row_id;

    bool operator==(const BlobView& other) const;
    bool operator!=(const BlobView& other) const;

    std::string ToString() const;

    /// Deserializes BLOB view bytes into a BlobView.
    static Result<BlobView> FromView(const char* buffer, uint64_t size);

    /// Serializes a BlobView into BLOB view bytes.
    static Result<PAIMON_UNIQUE_PTR<Bytes>> ToView(const BlobView& blob_view,
                                                   const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon

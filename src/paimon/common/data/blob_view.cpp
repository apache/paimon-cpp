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

#include "paimon/data/blob_view.h"

#include <memory>

#include "fmt/format.h"
#include "paimon/common/data/blob_view_struct.h"
#include "paimon/status.h"

namespace paimon {

bool BlobView::operator==(const BlobView& other) const {
    return identifier == other.identifier && field_id == other.field_id && row_id == other.row_id;
}

bool BlobView::operator!=(const BlobView& other) const {
    return !(*this == other);
}

std::string BlobView::ToString() const {
    return fmt::format("BlobView{{identifier={}, fieldId={}, rowId={}}}", identifier.GetFullName(),
                       field_id, row_id);
}

Result<BlobView> BlobView::FromView(const char* buffer, uint64_t size) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlobViewStruct> view_struct,
                           BlobViewStruct::Deserialize(buffer, size));
    return BlobView{view_struct->GetIdentifier(), view_struct->FieldId(), view_struct->RowId()};
}

Result<PAIMON_UNIQUE_PTR<Bytes>> BlobView::ToView(const BlobView& blob_view,
                                                  const std::shared_ptr<MemoryPool>& pool) {
    if (pool == nullptr) {
        return Status::Invalid("memory pool is nullptr");
    }

    BlobViewStruct view_struct(blob_view.identifier, blob_view.field_id, blob_view.row_id);
    return view_struct.Serialize(pool);
}

}  // namespace paimon

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
#include <utility>
#include <vector>

#include "paimon/predicate/predicate.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/utils/roaring_bitmap32.h"
namespace paimon {
/// The batch reader for a single file supports returning the line number of the last batch read for
/// deletion vector judgment.
class PAIMON_EXPORT FileBatchReader : public BatchReader {
 public:
    /// @return The schema of the file.
    virtual Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const = 0;

    /// Resets the read schema and predicate.
    ///
    /// If `SetReadSchema()` is not called, `NextBatch()` will return data with the file schema.
    /// After resetting the read schema, `NextBatch()` will read data starting from the first row.
    ///
    /// @param read_schema The schema to set for reading.
    /// @param predicate The predicate to apply for filtering data.
    /// @param selection_bitmap The bitmap to apply for filtering data.
    /// @return The status of the operation.
    virtual Status SetReadSchema(::ArrowSchema* read_schema,
                                 const std::shared_ptr<Predicate>& predicate,
                                 const std::optional<RoaringBitmap32>& selection_bitmap) = 0;
    using BatchReader::NextBatch;
    using BatchReader::NextBatchWithBitmap;

    /// Get the file-level row ID for a given batch-relative row index
    /// in the previously read batch.
    ///
    /// @param batch_row_id  Zero-based index within the current batch.
    /// @return The corresponding file-level row ID, or Status::Invalid
    ///         if no batch has been read yet, the last batch was EOF,
    ///         or batch_row_id is out of range.
    virtual Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const = 0;

    /// Get the number of rows in the file.
    virtual Result<uint64_t> GetNumberOfRows() const = 0;

    /// Get whether or not support read precisely while bitmap pushed down.
    virtual bool SupportPreciseBitmapSelection() const = 0;

    /// Starts whatever background work this reader would otherwise start on its first read, so a
    /// caller that knows this reader is next can pay that startup while still consuming the
    /// previous one.
    ///
    /// This is an optional hint and never changes what the reader returns: ordering, filtering and
    /// metrics are the same with or without it. A reader with nothing to start, or one that is not
    /// yet ready to start it, does nothing. Calling it before the reader is configured (for example
    /// before `SetReadSchema()`), or after `Close()`, is such a no-op.
    ///
    /// It reports no error on purpose: the caller may warm up a reader it never ends up reading,
    /// and a hint about a file nobody reads must not fail the read in progress. An implementation
    /// that cannot start its work leaves it to be started by the first read, which reports the
    /// failure itself.
    /// @warning The call starts background work owned by this reader, so it must be made from the
    /// same thread that reads this reader, and not concurrently with any other call on it.
    virtual void Warmup() {}
};

}  // namespace paimon

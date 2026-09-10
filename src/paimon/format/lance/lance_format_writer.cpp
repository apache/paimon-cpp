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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/format/lance/lance_format_writer.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/math.h"
#include "paimon/common/utils/scope_guard.h"

namespace paimon::lance {

LanceFormatWriter::LanceFormatWriter(const std::shared_ptr<arrow::Schema>& schema,
                                     const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                                     PaimonLanceWriter* writer)
    : schema_(schema),
      arrow_pool_(arrow_pool),
      writer_(writer),
      metrics_(std::make_shared<MetricsImpl>()) {}

Result<std::unique_ptr<LanceFormatWriter>> LanceFormatWriter::Create(
    const std::string& path, const std::shared_ptr<arrow::Schema>& schema,
    const LanceStorageOptions& storage_options,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (path.empty() || schema == nullptr || arrow_pool == nullptr) {
        return Status::Invalid("Lance writer requires a path, schema, and memory pool");
    }
    std::vector<const char*> keys;
    std::vector<const char*> values;
    keys.reserve(storage_options.size());
    values.reserve(storage_options.size());
    for (const auto& [key, value] : storage_options) {
        keys.push_back(key.c_str());
        values.push_back(value.c_str());
    }
    PaimonLanceWriter* writer = nullptr;
    if (paimon_lance_writer_open(path.c_str(), keys.data(), values.data(), keys.size(), &writer) !=
        0) {
        return LanceFfiError("open Lance writer");
    }
    return std::unique_ptr<LanceFormatWriter>(new LanceFormatWriter(schema, arrow_pool, writer));
}

LanceFormatWriter::~LanceFormatWriter() {
    paimon_lance_writer_free(writer_);
}

Status LanceFormatWriter::AddBatch(::ArrowArray* batch) {
    if (batch == nullptr) {
        return Status::Invalid("Lance writer batch is nullptr");
    }
    if (finished_) {
        return Status::Invalid("cannot add a batch after Lance writer is finished");
    }
    auto row_count = static_cast<uint64_t>(batch->length);
    ::ArrowSchema import_schema = {};
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema_, &import_schema));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(batch, &import_schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> normalized,
                           ArrowUtils::NormalizeArrayOffsets(array, arrow_pool_.get()));
    ::ArrowArray ffi_array = {};
    ::ArrowSchema ffi_schema = {};
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*normalized, &ffi_array, &ffi_schema));
    ScopeGuard guard([&]() {
        ArrowArrayRelease(&ffi_array);
        ArrowSchemaRelease(&ffi_schema);
    });
    if (paimon_lance_writer_write(writer_, &ffi_array, &ffi_schema) != 0) {
        return LanceFfiError("write Lance batch");
    }
    rows_written_ += row_count;
    return Status::OK();
}

Status LanceFormatWriter::Flush() {
    return Status::OK();
}

Status LanceFormatWriter::Finish() {
    if (finished_) {
        return Status::OK();
    }
    uint64_t row_count = 0;
    if (paimon_lance_writer_finish(writer_, &row_count) != 0) {
        return LanceFfiError("finish Lance writer");
    }
    if (row_count != rows_written_) {
        return Status::Invalid("Lance writer row count mismatch");
    }
    finished_ = true;
    return Status::OK();
}

Result<bool> LanceFormatWriter::ReachTargetSize(bool suggested_check, int64_t target_size) const {
    if (!suggested_check) {
        return false;
    }
    PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(target_size, "Lance target size"));
    uint64_t position = 0;
    if (paimon_lance_writer_tell(writer_, &position) != 0) {
        return LanceFfiError("get Lance writer position");
    }
    return position >= static_cast<uint64_t>(target_size);
}

std::shared_ptr<Metrics> LanceFormatWriter::GetWriterMetrics() const {
    return metrics_;
}

Status LanceFormatWriter::AddMetadata(const std::map<std::string, std::string>& metadata) {
    (void)metadata;
    return Status::NotImplemented("Lance writer metadata is not supported");
}

}  // namespace paimon::lance

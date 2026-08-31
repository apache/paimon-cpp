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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/format/mosaic/mosaic_format_writer.h"

#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/fs/file_system.h"

namespace paimon::mosaic {

MosaicFormatWriter::MosaicFormatWriter(const std::shared_ptr<OutputStream>& output,
                                       const std::shared_ptr<arrow::Schema>& schema,
                                       std::unique_ptr<MosaicOutputContext> output_context,
                                       MosaicWriterHandle* writer)
    : output_(output),
      schema_(schema),
      output_context_(std::move(output_context)),
      writer_(writer),
      metrics_(std::make_shared<MetricsImpl>()) {}

Result<std::unique_ptr<MosaicFormatWriter>> MosaicFormatWriter::Create(
    const std::shared_ptr<OutputStream>& output, const std::shared_ptr<arrow::Schema>& schema,
    const MosaicWriterOptions& options) {
    if (output == nullptr || schema == nullptr) {
        return Status::Invalid("Mosaic writer requires non-null output and schema");
    }
    auto output_context = std::make_unique<MosaicOutputContext>(output);
    MosaicOutputFile output_file = {};
    output_file.ctx = output_context.get();
    output_file.write_fn = MosaicOutputContext::Write;
    output_file.flush_fn = MosaicOutputContext::Flush;
    output_file.get_pos_fn = MosaicOutputContext::GetPos;

    ::ArrowSchema ffi_schema = {};
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &ffi_schema));
    MosaicWriterHandle* writer = mosaic_writer_open(output_file, &ffi_schema, options);
    if (writer == nullptr) {
        Status status = MosaicFfiError("open Mosaic writer", output_context->GetCallbackStatus());
        ArrowSchemaRelease(&ffi_schema);
        return status;
    }
    return std::unique_ptr<MosaicFormatWriter>(
        new MosaicFormatWriter(output, schema, std::move(output_context), writer));
}

MosaicFormatWriter::~MosaicFormatWriter() {
    if (writer_ != nullptr) {
        if (!finished_) {
            mosaic_writer_close(writer_);
        }
        mosaic_writer_free(writer_);
    }
}

Status MosaicFormatWriter::AddBatch(::ArrowArray* batch) {
    if (batch == nullptr) {
        return Status::Invalid("Mosaic writer batch is nullptr");
    }
    if (finished_) {
        return Status::Invalid("cannot add a batch after Mosaic writer is finished");
    }
    ::ArrowSchema ffi_schema = {};
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema_, &ffi_schema));
    if (mosaic_writer_write_batch(writer_, batch, &ffi_schema) != 0) {
        Status status = MosaicFfiError("write Mosaic batch", output_context_->GetCallbackStatus());
        ArrowSchemaRelease(&ffi_schema);
        return status;
    }
    return Status::OK();
}

Status MosaicFormatWriter::Flush() {
    if (finished_) {
        return Status::OK();
    }
    return output_->Flush();
}

Status MosaicFormatWriter::Finish() {
    if (finished_) {
        return Status::OK();
    }
    if (mosaic_writer_close(writer_) != 0) {
        return MosaicFfiError("finish Mosaic writer", output_context_->GetCallbackStatus());
    }
    finished_ = true;
    return Status::OK();
}

Result<bool> MosaicFormatWriter::ReachTargetSize(bool suggested_check, int64_t target_size) const {
    if (!suggested_check) {
        return false;
    }
    int64_t estimated_size = 0;
    if (mosaic_writer_estimated_file_size(writer_, &estimated_size) != 0) {
        return MosaicFfiError("estimate Mosaic file size", output_context_->GetCallbackStatus());
    }
    return estimated_size >= target_size;
}

std::shared_ptr<Metrics> MosaicFormatWriter::GetWriterMetrics() const {
    return metrics_;
}

Status MosaicFormatWriter::AddMetadata(const std::map<std::string, std::string>& metadata) {
    (void)metadata;
    return Status::NotImplemented("Mosaic writer metadata is not supported");
}

}  // namespace paimon::mosaic

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

#include "paimon/format/vortex/vortex_format_writer.h"

#include <utility>

#include "arrow/c/bridge.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/vortex/vortex_ffi_util.h"
#include "paimon/fs/file_system.h"

namespace paimon::vortex {

VortexFormatWriter::VortexFormatWriter(std::shared_ptr<OutputStream> output,
                                       std::shared_ptr<arrow::Schema> schema, VxSessionPtr session,
                                       std::shared_ptr<VortexOutputContext> output_context,
                                       vx_callback_sink* sink)
    : output_(std::move(output)),
      schema_(std::move(schema)),
      session_(std::move(session)),
      output_context_(std::move(output_context)),
      sink_(sink),
      metrics_(std::make_shared<MetricsImpl>()) {}

Result<std::unique_ptr<VortexFormatWriter>> VortexFormatWriter::Create(
    const std::shared_ptr<OutputStream>& output, const std::shared_ptr<arrow::Schema>& schema) {
    if (output == nullptr || schema == nullptr) {
        return Status::Invalid("Vortex writer requires non-null output and schema");
    }
    VxSessionPtr session(vx_session_new(), vx_session_free);
    if (session == nullptr) {
        return Status::IOError("failed to create Vortex session");
    }
    ::ArrowSchema ffi_schema = {};
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &ffi_schema));
    vx_error* error = nullptr;
    // vx_dtype_from_arrow_schema consumes ffi_schema on both success and failure.
    VxDtypePtr dtype(vx_dtype_from_arrow_schema(session.get(), &ffi_schema, &error), vx_dtype_free);
    if (dtype == nullptr) {
        return VortexFfiError("convert Arrow schema to Vortex dtype", error);
    }
    error = nullptr;
    auto output_context = std::make_shared<VortexOutputContext>(output);
    vx_callback_sink* sink = vx_callback_sink_open(
        session.get(), VortexOutputContext::MakeCallbacks(output_context), dtype.get(), &error);
    if (sink == nullptr) {
        return VortexCallbackError("open Vortex callback sink", error,
                                   output_context->GetCallbackStatus());
    }
    return std::unique_ptr<VortexFormatWriter>(new VortexFormatWriter(
        output, schema, std::move(session), std::move(output_context), sink));
}

VortexFormatWriter::~VortexFormatWriter() {
    if (sink_ != nullptr) {
        // Not finished: abort so Vortex drops the sink without writing a footer. Whatever reached
        // the output stream is an incomplete file, which the caller discards along with it.
        vx_callback_sink_abort(sink_);
        sink_ = nullptr;
    }
}

Status VortexFormatWriter::AddBatch(::ArrowArray* batch) {
    if (batch == nullptr) {
        return Status::Invalid("Vortex writer batch is nullptr");
    }
    if (finished_ || sink_ == nullptr) {
        return Status::Invalid("cannot add a batch after Vortex writer is finished");
    }
    ::ArrowSchema ffi_schema = {};
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema_, &ffi_schema));
    vx_error* error = nullptr;
    // vx_array_from_arrow consumes both `batch` and `ffi_schema` on success and on failure.
    VxArrayPtr array(
        vx_array_from_arrow(session_.get(), batch, &ffi_schema, /*nullable=*/false, &error),
        vx_array_free);
    if (array == nullptr) {
        return VortexFfiError("convert Arrow batch to Vortex array", error);
    }
    error = nullptr;
    vx_callback_sink_push(sink_, array.get(), &error);
    if (error != nullptr) {
        return VortexCallbackError("push batch to Vortex sink", error,
                                   output_context_->GetCallbackStatus());
    }
    return Status::OK();
}

Status VortexFormatWriter::Flush() {
    if (finished_) {
        return Status::OK();
    }
    // Vortex buffers internally, so this only flushes what its sink has already handed over.
    return output_->Flush();
}

Status VortexFormatWriter::Finish() {
    if (finished_) {
        return Status::OK();
    }
    if (sink_ == nullptr) {
        return Status::Invalid("Vortex writer sink is not open");
    }
    vx_error* error = nullptr;
    vx_callback_sink_close(sink_, &error);
    // close consumes the sink even when it reports an error
    sink_ = nullptr;
    if (error != nullptr) {
        return VortexCallbackError("close Vortex writer", error,
                                   output_context_->GetCallbackStatus());
    }
    // A write callback that failed leaves the file incomplete even if Vortex itself reported
    // success, so surface it here as well.
    PAIMON_RETURN_NOT_OK(output_context_->GetCallbackStatus());
    finished_ = true;
    return Status::OK();
}

Result<bool> VortexFormatWriter::ReachTargetSize(bool suggested_check, int64_t target_size) const {
    if (!suggested_check || output_context_ == nullptr) {
        return false;
    }
    // Bytes already handed to the output stream. Vortex still holds buffered data and writes the
    // footer on close, so this is a lower bound and rolling only ever happens later than ideal.
    return output_context_->BytesWritten() >= target_size;
}

std::shared_ptr<Metrics> VortexFormatWriter::GetWriterMetrics() const {
    return metrics_;
}

Status VortexFormatWriter::AddMetadata(const std::map<std::string, std::string>& metadata) {
    (void)metadata;
    return Status::NotImplemented("Vortex writer metadata is not supported");
}

}  // namespace paimon::vortex

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

#pragma once

#include <map>
#include <memory>
#include <string>

#include "paimon/format/format_writer.h"
#include "paimon/format/vortex/vortex_ffi.h"
#include "paimon/format/vortex/vortex_ffi_util.h"
#include "paimon/format/vortex/vortex_io_callbacks.h"
#include "paimon/result.h"

namespace arrow {
class MemoryPool;
class Schema;
}  // namespace arrow
namespace paimon {
class Metrics;
class OutputStream;
}  // namespace paimon

namespace paimon::vortex {

/// Writes a Vortex file.
///
/// Bytes go straight to the paimon `OutputStream` through `vx_callback_sink_open`, the
/// callback-based sink this repository adds to vortex-ffi: Vortex writes into `output_context_`,
/// which forwards to the stream. Vortex's own sink (`vx_array_sink_open_file`) can only create a
/// local file, which would mean staging the file on local disk and copying it back on finish.
class VortexFormatWriter : public FormatWriter {
 public:
    static Result<std::unique_ptr<VortexFormatWriter>> Create(
        const std::shared_ptr<OutputStream>& output, const std::shared_ptr<arrow::Schema>& schema,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    ~VortexFormatWriter() override;

    Status AddBatch(::ArrowArray* batch) override;
    Status Flush() override;
    Status Finish() override;
    Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size) const override;
    std::shared_ptr<Metrics> GetWriterMetrics() const override;
    Status AddMetadata(const std::map<std::string, std::string>& metadata) override;

 private:
    VortexFormatWriter(std::shared_ptr<OutputStream> output, std::shared_ptr<arrow::Schema> schema,
                       VxSessionPtr session, std::shared_ptr<VortexOutputContext> output_context,
                       vx_callback_sink* sink, std::shared_ptr<arrow::MemoryPool> arrow_pool);

    std::shared_ptr<OutputStream> output_;
    std::shared_ptr<arrow::Schema> schema_;
    // The caller's Arrow pool, used for any buffer (re)allocation on the write path so it counts
    // against Paimon's memory accounting instead of Arrow's default pool.
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    VxSessionPtr session_;
    std::shared_ptr<VortexOutputContext> output_context_;
    // Raw pointer on purpose: close (in Finish) and abort (in the destructor) are mutually
    // exclusive terminations that each consume the sink even on error, which a single-deleter
    // RAII alias cannot express. Nulled right after either, so it is never used or freed twice.
    vx_callback_sink* sink_;
    std::shared_ptr<Metrics> metrics_;
    bool finished_ = false;
};

}  // namespace paimon::vortex

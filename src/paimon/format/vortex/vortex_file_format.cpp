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

#include "paimon/format/vortex/vortex_file_format.h"

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/format/vortex/vortex_defs.h"
#include "paimon/format/vortex/vortex_predicate_converter.h"
#include "paimon/format/vortex/vortex_reader_builder.h"
#include "paimon/format/vortex/vortex_stats_extractor.h"
#include "paimon/format/vortex/vortex_writer_builder.h"

namespace paimon::vortex {

Result<std::unique_ptr<ReaderBuilder>> VortexFileFormat::CreateReaderBuilder(
    int32_t batch_size) const {
    PAIMON_ASSIGN_OR_RAISE(uint32_t max_in_values, OptionsUtils::GetValueFromMap<uint32_t>(
                                                       options_, kVortexReadInPushdownMaxValues,
                                                       kDefaultVortexReadInPushdownMaxValues));
    const VortexPredicateOptions predicate_options{max_in_values};
    PAIMON_ASSIGN_OR_RAISE(int64_t max_gap,
                           OptionsUtils::GetValueFromMap<int64_t>(
                               options_, kVortexReadIoCoalesceMaxGap,
                               static_cast<int64_t>(kDefaultVortexReadIoCoalesceMaxGap)));
    PAIMON_ASSIGN_OR_RAISE(int64_t max_size,
                           OptionsUtils::GetValueFromMap<int64_t>(
                               options_, kVortexReadIoCoalesceMaxSize,
                               static_cast<int64_t>(kDefaultVortexReadIoCoalesceMaxSize)));
    if (max_gap < 0) {
        return Status::Invalid(fmt::format("{} must be non-negative", kVortexReadIoCoalesceMaxGap));
    }
    if (max_size <= 0) {
        return Status::Invalid(fmt::format("{} must be positive", kVortexReadIoCoalesceMaxSize));
    }
    if (max_gap > max_size) {
        return Status::Invalid(fmt::format("{} must not exceed {}, got {} > {}",
                                           kVortexReadIoCoalesceMaxGap,
                                           kVortexReadIoCoalesceMaxSize, max_gap, max_size));
    }
    return std::make_unique<VortexReaderBuilder>(
        batch_size, predicate_options,
        VortexIoCoalesceOptions{static_cast<uint64_t>(max_gap), static_cast<uint64_t>(max_size)});
}

Result<std::unique_ptr<WriterBuilder>> VortexFileFormat::CreateWriterBuilder(
    ::ArrowSchema* schema, int32_t batch_size) const {
    (void)batch_size;
    if (schema == nullptr) {
        return Status::Invalid("Vortex writer schema is nullptr");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> typed_schema,
                                      arrow::ImportSchema(schema));
    return std::make_unique<VortexWriterBuilder>(typed_schema, options_);
}

Result<std::unique_ptr<FormatStatsExtractor>> VortexFileFormat::CreateStatsExtractor(
    ::ArrowSchema* schema) const {
    if (schema == nullptr) {
        return Status::Invalid("Vortex stats schema is nullptr");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> typed_schema,
                                      arrow::ImportSchema(schema));
    return std::make_unique<VortexStatsExtractor>(typed_schema);
}

}  // namespace paimon::vortex

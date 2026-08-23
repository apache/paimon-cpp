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

#include "paimon/format/mosaic/mosaic_file_format.h"

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/mosaic/mosaic_reader_builder.h"
#include "paimon/format/mosaic/mosaic_writer_builder.h"

namespace paimon::mosaic {

Result<std::unique_ptr<ReaderBuilder>> MosaicFileFormat::CreateReaderBuilder(
    int32_t batch_size) const {
    return std::make_unique<MosaicReaderBuilder>(batch_size);
}

Result<std::unique_ptr<WriterBuilder>> MosaicFileFormat::CreateWriterBuilder(
    ::ArrowSchema* schema, int32_t batch_size) const {
    (void)batch_size;
    if (schema == nullptr) {
        return Status::Invalid("Mosaic writer schema is nullptr");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> typed_schema,
                                      arrow::ImportSchema(schema));
    return std::make_unique<MosaicWriterBuilder>(typed_schema, options_);
}

Result<std::unique_ptr<FormatStatsExtractor>> MosaicFileFormat::CreateStatsExtractor(
    ::ArrowSchema* schema) const {
    if (schema != nullptr) {
        ArrowSchemaRelease(schema);
    }
    return Status::NotImplemented("Mosaic statistics extraction is not supported");
}

}  // namespace paimon::mosaic

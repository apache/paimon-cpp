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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/format/parquet/parquet_format_writer.h"

#include <map>
#include <string>
#include <string_view>
#include <utility>

#include "arrow/array/array_dict.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/cast.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/util/base64.h"
#include "arrow/util/key_value_metadata.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/arrow_output_stream_adapter.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "parquet/arrow/writer.h"
#include "parquet/properties.h"

namespace arrow {
class MemoryPool;
class Schema;
}  // namespace arrow
namespace paimon {
class OutputStream;
}  // namespace paimon
struct ArrowArray;

namespace paimon::parquet {

Result<std::unique_ptr<ParquetFormatWriter>> ParquetFormatWriter::Create(
    const std::shared_ptr<OutputStream>& output_stream,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<::parquet::WriterProperties>& writer_properties, uint64_t max_memory_use,
    const std::shared_ptr<arrow::MemoryPool>& pool) {
    auto out = std::make_shared<ArrowOutputStreamAdapter>(output_stream);
    ::parquet::ArrowWriterProperties::Builder arrow_properties_builder;
    auto arrow_writer_properties =
        arrow_properties_builder.enable_deprecated_int96_timestamps()->build();
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::unique_ptr<::parquet::arrow::FileWriter> file_writer,
        ::parquet::arrow::FileWriter::Open(*schema, pool.get(), out, writer_properties,
                                           arrow_writer_properties));
    return std::unique_ptr<ParquetFormatWriter>(
        new ParquetFormatWriter(std::move(file_writer), out, schema, max_memory_use, pool));
}

Status ParquetFormatWriter::AddBatch(ArrowArray* batch) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> batch_schema, ResolveBatchSchema(batch));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<::arrow::RecordBatch> record_batch,
                                      arrow::ImportRecordBatch(batch, batch_schema));
    PAIMON_ASSIGN_OR_RAISE(record_batch, FlattenUnwritableDictionaries(record_batch));
    if (static_cast<uint64_t>(pool_->bytes_allocated()) > max_memory_use_) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(writer_->NewBufferedRowGroup());
    }
    PAIMON_RETURN_NOT_OK_FROM_ARROW(writer_->WriteRecordBatch(*record_batch));
    total_records_written_ += (*record_batch).num_rows();
    return Status::OK();
}

Result<std::shared_ptr<arrow::Schema>> ParquetFormatWriter::ResolveBatchSchema(
    const ::ArrowArray* batch) const {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::DataType> batch_type,
        ArrowUtils::ResolveDictionaryStructTypeFromLayout(logical_struct_type_, batch));
    if (batch_type == logical_struct_type_) {
        return schema_;
    }
    return arrow::schema(batch_type->fields(), schema_->metadata());
}

Result<std::shared_ptr<arrow::RecordBatch>> ParquetFormatWriter::FlattenUnwritableDictionaries(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) const {
    arrow::ArrayVector columns;
    arrow::FieldVector fields;
    for (int32_t i = 0; i < record_batch->num_columns(); ++i) {
        const std::shared_ptr<arrow::Array>& column = record_batch->column(i);
        if (column->type_id() != arrow::Type::DICTIONARY ||
            checked_cast<const arrow::DictionaryArray&>(*column).dictionary()->null_count() == 0) {
            continue;
        }
        if (columns.empty()) {
            columns = record_batch->columns();
            fields = record_batch->schema()->fields();
        }
        const auto& dictionary_type = checked_cast<const arrow::DictionaryType&>(*column->type());
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Array> flattened,
            CastingUtils::Cast(column, dictionary_type.value_type(),
                               arrow::compute::CastOptions::Safe(), pool_.get()));
        columns[i] = std::move(flattened);
        fields[i] = fields[i]->WithType(dictionary_type.value_type());
    }
    if (columns.empty()) {
        return record_batch;
    }
    return arrow::RecordBatch::Make(arrow::schema(fields, record_batch->schema()->metadata()),
                                    record_batch->num_rows(), std::move(columns));
}

Status ParquetFormatWriter::Flush() {
    metrics_->SetCounter(ParquetMetrics::WRITE_RECORD_COUNT, total_records_written_);
    return Status::OK();
}

Status ParquetFormatWriter::Finish() {
    PAIMON_RETURN_NOT_OK(Flush());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(writer_->Close());
    return Status::OK();
}

Status ParquetFormatWriter::AddMetadata(const std::map<std::string, std::string>& metadata) {
    if (metadata.empty()) {
        return Status::OK();
    }
    auto key_value_metadata = std::make_shared<arrow::KeyValueMetadata>();
    for (const auto& [key, value] : metadata) {
        key_value_metadata->Append(key, arrow::util::base64_encode(std::string_view(value)));
    }
    PAIMON_RETURN_NOT_OK_FROM_ARROW(writer_->AddKeyValueMetadata(key_value_metadata));
    return Status::OK();
}

Result<bool> ParquetFormatWriter::ReachTargetSize(bool suggested_check, int64_t target_size) const {
    if (suggested_check) {
        PAIMON_ASSIGN_OR_RAISE(const uint64_t length, GetEstimateLength());
        return length >= static_cast<uint64_t>(target_size);
    }
    return false;
}

Result<uint64_t> ParquetFormatWriter::GetEstimateLength() const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(int64_t written_bytes, out_->Tell());
    return writer_->GetBufferedSize() + written_bytes;
}

ParquetFormatWriter::ParquetFormatWriter(std::unique_ptr<::parquet::arrow::FileWriter> writer,
                                         const std::shared_ptr<ArrowOutputStreamAdapter>& out,
                                         const std::shared_ptr<arrow::Schema>& schema,
                                         uint64_t max_memory_use,
                                         const std::shared_ptr<arrow::MemoryPool>& pool)
    : pool_(pool),
      out_(out),
      writer_(std::move(writer)),
      schema_(schema),
      logical_struct_type_(arrow::struct_(schema->fields())),
      metrics_(std::make_shared<MetricsImpl>()),
      max_memory_use_(max_memory_use) {}

}  // namespace paimon::parquet

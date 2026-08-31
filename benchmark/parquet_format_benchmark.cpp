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

// Format-level micro-benchmarks for the Parquet reader and writer. These drive
// ParquetWriterBuilder / ParquetFileBatchReader directly, so a format-layer change can be
// attributed without the catalog lookup, split planning, merge/sort and commit that the
// table-level read_write_benchmark includes. Each case comment says what that case answers.
//
// Every axis - type, cardinality, null density, batch size, encoding, selectivity - is swept on
// its own rather than as a combined matrix, because the point is attributing one change rather
// than describing a workload.
//
// Data is generated outside the timed region and on Arrow's default pool, because the writer
// cuts a new row group once its own pool crosses parquet.writer.max.memory.use.
//
// IO goes through the local FileSystem into a temporary directory - the project has no in-memory
// FileSystem - so absolute numbers are only meaningful relative to each other.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/util/bit_util.h"
#include "benchmark/benchmark.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/defs.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/parquet/parquet_field_id_converter.h"
#include "paimon/format/parquet/parquet_file_batch_reader.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace {

using ::paimon::ArrowInputStreamAdapter;
using ::paimon::BatchReader;
using ::paimon::FieldType;
using ::paimon::FileStatus;
using ::paimon::FileSystem;
using ::paimon::FormatWriter;
using ::paimon::InputStream;
using ::paimon::Literal;
using ::paimon::OutputStream;
using ::paimon::PathUtil;
using ::paimon::Predicate;
using ::paimon::PredicateBuilder;
using ::paimon::Result;
using ::paimon::RoaringBitmap32;
using ::paimon::Status;
using ::paimon::parquet::ParquetFieldIdConverter;
using ::paimon::parquet::ParquetFileBatchReader;
using ::paimon::parquet::ParquetWriterBuilder;

constexpr int64_t kRowsPerFile = 100'000;
constexpr int64_t kRowsPerBatch = 10'000;
constexpr int32_t kReadBatchSize = 4096;
constexpr int32_t kWriteBatchSize = 1024;
// Small enough that a 100K-row file spans many pages, so page-level pruning has something to
// prune. Arrow's page limit is byte-based; a row-count limit is not available yet.
constexpr int64_t kPageSizeBytes = 64 * 1024;
// Four row groups per read fixture, so row-group pruning and page pruning are both in play.
constexpr int64_t kRowGroupLength = 25'000;
constexpr int64_t kStringCardinality = 1'000;
// Few enough distinct values that arrow keeps the column dictionary-encoded for the whole file,
// which is the shape the wide-schema case wants: per-column work small, per-batch cost visible.
constexpr int64_t kLowStringCardinality = 10;
constexpr int32_t kVectorDimension = 16;
constexpr int32_t kListLength = 4;
// Same as kListLength, so the MAP and LIST cases differ only in the extra key leaf.
constexpr int32_t kMapEntries = kListLength;
constexpr char kDefaultCompression[] = "zstd";
// Nulls are placed over a 100-row window, so a requested density is exact, not statistical.
constexpr int64_t kNullWindow = 100;

std::shared_ptr<arrow::Field> MakeField(const std::string& name,
                                        const std::shared_ptr<arrow::DataType>& type,
                                        int32_t field_id) {
    return arrow::field(name, type,
                        arrow::KeyValueMetadata::Make({ParquetFieldIdConverter::PARQUET_FIELD_ID},
                                                      {std::to_string(field_id)}));
}

std::shared_ptr<arrow::DataType> StructColumnType() {
    return arrow::struct_({arrow::field("a", arrow::int64()), arrow::field("b", arrow::utf8())});
}

std::shared_ptr<arrow::DataType> ListColumnType() {
    return arrow::list(arrow::int64());
}

std::shared_ptr<arrow::DataType> VectorColumnType() {
    return arrow::fixed_size_list(arrow::field("element", arrow::float32(), /*nullable=*/false),
                                  kVectorDimension);
}

std::shared_ptr<arrow::DataType> MapColumnType() {
    return arrow::map(arrow::utf8(), arrow::int64());
}

std::shared_ptr<arrow::DataType> DictionaryStringType() {
    return arrow::dictionary(arrow::int32(), arrow::utf8());
}

std::shared_ptr<arrow::DataType> DictionaryInt32Type() {
    return arrow::dictionary(arrow::int32(), arrow::int32());
}

// The three-column file the flat read cases scan. `id` is ordered so a range predicate maps
// onto a contiguous row range, which is what makes page-index pruning measurable.
std::shared_ptr<arrow::Schema> FlatSchema() {
    return arrow::schema({MakeField("id", arrow::int64(), 0), MakeField("name", arrow::utf8(), 1),
                          MakeField("amount", arrow::decimal128(18, 4), 2)});
}

std::shared_ptr<arrow::Schema> DecimalSchema(int32_t precision) {
    return arrow::schema({MakeField("amount", arrow::decimal128(precision, 4), 0)});
}

std::shared_ptr<arrow::Schema> DoubleSchema() {
    return arrow::schema({MakeField("value", arrow::float64(), 0)});
}

std::shared_ptr<arrow::Schema> NestedSchema() {
    return arrow::schema(
        {MakeField("id", arrow::int64(), 0), MakeField("info", StructColumnType(), 1),
         MakeField("tags", ListColumnType(), 2), MakeField("embedding", VectorColumnType(), 3),
         MakeField("attrs", MapColumnType(), 4)});
}

// A VECTOR is stored as a Parquet LIST and the reader hands back the file's own types, so a
// format-level read asks for the physical type; VectorFileBatchReader restores the view above.
std::shared_ptr<arrow::Schema> NestedReadSchema() {
    return arrow::schema({MakeField("id", arrow::int64(), 0),
                          MakeField("info", StructColumnType(), 1),
                          MakeField("tags", ListColumnType(), 2),
                          MakeField("embedding",
                                    arrow::list(arrow::field("element", arrow::float32(),
                                                             /*nullable=*/false)),
                                    3),
                          MakeField("attrs", MapColumnType(), 4)});
}

Result<std::shared_ptr<arrow::Array>> MakeInt64Column(int64_t num_rows, int64_t offset) {
    arrow::Int64Builder builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(num_rows));
    for (int64_t i = 0; i < num_rows; ++i) {
        builder.UnsafeAppend(offset + i);
    }
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&array));
    return array;
}

Result<std::shared_ptr<arrow::Array>> MakeDoubleColumn(int64_t num_rows, int64_t offset) {
    arrow::DoubleBuilder builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(num_rows));
    for (int64_t i = 0; i < num_rows; ++i) {
        builder.UnsafeAppend(static_cast<double>(offset + i) * 1.5);
    }
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&array));
    return array;
}

Result<std::shared_ptr<arrow::Array>> MakeBooleanColumn(int64_t num_rows, int64_t offset) {
    arrow::BooleanBuilder builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(num_rows));
    for (int64_t i = 0; i < num_rows; ++i) {
        builder.UnsafeAppend(((offset + i) & 1) == 0);
    }
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&array));
    return array;
}

// Low cardinality is what arrow dictionary-encodes; high cardinality falls back to plain.
Result<std::shared_ptr<arrow::Array>> MakeStringColumn(int64_t num_rows, int64_t offset,
                                                       int64_t cardinality) {
    arrow::StringBuilder builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(num_rows));
    for (int64_t i = 0; i < num_rows; ++i) {
        const int64_t value = (offset + i) % cardinality;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append("value_" + std::to_string(value)));
    }
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&array));
    return array;
}

// INT32 cycling through `cardinality` distinct values, the flat control for
// BM_ParquetWrite_DictionaryInt32: same logical values, same cardinality, same width, so the
// delta between the two is the dictionary materialization alone.
Result<std::shared_ptr<arrow::Array>> MakeInt32Column(int64_t num_rows, int64_t offset,
                                                      int64_t cardinality) {
    arrow::Int32Builder builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(num_rows));
    for (int64_t i = 0; i < num_rows; ++i) {
        builder.UnsafeAppend(static_cast<int32_t>(((offset + i) % cardinality) * 7));
    }
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&array));
    return array;
}

// Dictionary-encoded input: `values->length()` distinct values behind an int32 index array. Arrow
// hands the indices straight to Parquet when the value type is binary-like
// (DictionaryDirectWriteSupported) and densifies them otherwise, so the value type alone decides
// whether the writer materializes anything.
Result<std::shared_ptr<arrow::Array>> MakeDictionaryColumn(
    const std::shared_ptr<arrow::Array>& values, int64_t num_rows, int64_t offset) {
    const int64_t cardinality = values->length();
    arrow::Int32Builder index_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(index_builder.Reserve(num_rows));
    for (int64_t i = 0; i < num_rows; ++i) {
        index_builder.UnsafeAppend(static_cast<int32_t>((offset + i) % cardinality));
    }
    std::shared_ptr<arrow::Array> indices;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(index_builder.Finish(&indices));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::DictionaryArray::FromArrays(indices, values));
    return array;
}

// STRING values: binary-like, so arrow can write the indices directly.
Result<std::shared_ptr<arrow::Array>> MakeDictionaryStringColumn(int64_t num_rows, int64_t offset,
                                                                 int64_t cardinality) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> values,
                           MakeStringColumn(cardinality, /*offset=*/0, cardinality));
    return MakeDictionaryColumn(values, num_rows, offset);
}

// INT32 values: is_base_binary_like excludes them, so arrow densifies before writing. The
// dictionary holds the same `i * 7` values MakeInt32Column emits inline, so the two are directly
// comparable.
Result<std::shared_ptr<arrow::Array>> MakeDictionaryInt32Column(int64_t num_rows, int64_t offset,
                                                                int64_t cardinality) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> values,
                           MakeInt32Column(cardinality, /*offset=*/0, cardinality));
    return MakeDictionaryColumn(values, num_rows, offset);
}

// Precision drives the Parquet physical type: <= 9 is INT32, <= 18 is INT64, larger is
// FIXED_LEN_BYTE_ARRAY, and the three take different transfer paths on read.
Result<std::shared_ptr<arrow::Array>> MakeDecimalColumn(int64_t num_rows, int64_t offset,
                                                        int32_t precision, int32_t scale) {
    arrow::Decimal128Builder builder(arrow::decimal128(precision, scale));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(num_rows));
    for (int64_t i = 0; i < num_rows; ++i) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(arrow::Decimal128(offset + i)));
    }
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&array));
    return array;
}

Result<std::shared_ptr<arrow::Array>> MakeStructArray(
    const arrow::FieldVector& fields, const std::vector<std::shared_ptr<arrow::Array>>& columns) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> array,
                                      arrow::StructArray::Make(columns, fields));
    return paimon::checked_pointer_cast<arrow::Array>(array);
}

Result<std::shared_ptr<arrow::Array>> MakeStructColumn(int64_t num_rows, int64_t offset) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> a, MakeInt64Column(num_rows, offset));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> b,
                           MakeStringColumn(num_rows, offset, kStringCardinality));
    return MakeStructArray(StructColumnType()->fields(), {a, b});
}

// Constant element count per row, so the delta against a flat BIGINT column is the levels.
Result<std::shared_ptr<arrow::Array>> MakeListColumn(int64_t num_rows, int64_t offset) {
    auto value_builder = std::make_shared<arrow::Int64Builder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder, ListColumnType());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(num_rows));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->Reserve(num_rows * kListLength));
    for (int64_t i = 0; i < num_rows; ++i) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append());
        for (int32_t j = 0; j < kListLength; ++j) {
            value_builder->UnsafeAppend(offset + i + j);
        }
    }
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&array));
    return array;
}

// The writer converts VECTOR to a Parquet LIST, so this also covers ParquetVectorConverter.
Result<std::shared_ptr<arrow::Array>> MakeVectorColumn(int64_t num_rows, int64_t offset) {
    auto value_builder = std::make_shared<arrow::FloatBuilder>();
    arrow::FixedSizeListBuilder builder(arrow::default_memory_pool(), value_builder,
                                        VectorColumnType());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(num_rows));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->Reserve(num_rows * kVectorDimension));
    for (int64_t i = 0; i < num_rows; ++i) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append());
        for (int32_t j = 0; j < kVectorDimension; ++j) {
            value_builder->UnsafeAppend(static_cast<float>((offset + i) % 1024) +
                                        static_cast<float>(j));
        }
    }
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&array));
    return array;
}

// A Parquet MAP is a LIST of a two-field STRUCT, so against LIST the difference is the key leaf.
Result<std::shared_ptr<arrow::Array>> MakeMapColumn(int64_t num_rows, int64_t offset,
                                                    int32_t entries) {
    auto key_builder = std::make_shared<arrow::StringBuilder>();
    auto item_builder = std::make_shared<arrow::Int64Builder>();
    arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, item_builder,
                              MapColumnType());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(num_rows));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Reserve(num_rows * entries));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(item_builder->Reserve(num_rows * entries));
    for (int64_t i = 0; i < num_rows; ++i) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append());
        for (int32_t j = 0; j < entries; ++j) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Append("key_" + std::to_string(j)));
            item_builder->UnsafeAppend(offset + i + j);
        }
    }
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&array));
    return array;
}

// Masks `null_pct` percent of the slots null. Doing it after the fact rather than in every
// generator keeps the values identical across densities, so the delta between two settings is the
// level machinery alone. Only the top level is masked: a STRUCT or LIST row, not its leaves.
Result<std::shared_ptr<arrow::Array>> WithNulls(const std::shared_ptr<arrow::Array>& array,
                                                int64_t offset, int64_t null_pct) {
    if (null_pct <= 0) {
        return array;
    }
    // Builder output starts at slot 0, so the bitmap below can be indexed by position.
    if (array->data()->offset != 0) {
        return Status::Invalid("WithNulls expects an unsliced array");
    }
    const int64_t length = array->length();
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Buffer> validity,
        arrow::AllocateEmptyBitmap(length, arrow::default_memory_pool()));
    int64_t null_count = 0;
    for (int64_t i = 0; i < length; ++i) {
        if ((offset + i) % kNullWindow < null_pct) {
            ++null_count;
        } else {
            arrow::bit_util::SetBit(validity->mutable_data(), i);
        }
    }
    std::shared_ptr<arrow::ArrayData> data = array->data()->Copy();
    data->buffers[0] = std::move(validity);
    data->SetNullCount(null_count);
    return arrow::MakeArray(data);
}

using BatchFactory = std::function<Result<std::shared_ptr<arrow::Array>>(
    const std::shared_ptr<arrow::Schema>& schema, int64_t offset, int64_t rows)>;

using ColumnFactory =
    std::function<Result<std::shared_ptr<arrow::Array>>(int64_t rows, int64_t offset)>;

ColumnFactory NullableColumnFactory(const ColumnFactory& make_column, int64_t null_pct) {
    return [make_column, null_pct](int64_t rows,
                                   int64_t offset) -> Result<std::shared_ptr<arrow::Array>> {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> column, make_column(rows, offset));
        return WithNulls(column, offset, null_pct);
    };
}

// Lifts a one-column generator into a batch factory for a one-field schema.
BatchFactory SingleColumnBatch(const ColumnFactory& make_column) {
    return [make_column](const std::shared_ptr<arrow::Schema>& schema, int64_t offset,
                         int64_t rows) -> Result<std::shared_ptr<arrow::Array>> {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> column, make_column(rows, offset));
        return MakeStructArray(schema->fields(), {column});
    };
}

Result<std::shared_ptr<arrow::Array>> MakeNullableFlatBatch(
    const std::shared_ptr<arrow::Schema>& schema, int64_t offset, int64_t rows, int64_t null_pct) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> ids, MakeInt64Column(rows, offset));
    PAIMON_ASSIGN_OR_RAISE(ids, WithNulls(ids, offset, null_pct));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> names,
                           MakeStringColumn(rows, offset, kStringCardinality));
    PAIMON_ASSIGN_OR_RAISE(names, WithNulls(names, offset, null_pct));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> amounts,
                           MakeDecimalColumn(rows, offset, /*precision=*/18, /*scale=*/4));
    PAIMON_ASSIGN_OR_RAISE(amounts, WithNulls(amounts, offset, null_pct));
    return MakeStructArray(schema->fields(), {ids, names, amounts});
}

Result<std::shared_ptr<arrow::Array>> MakeFlatBatch(const std::shared_ptr<arrow::Schema>& schema,
                                                    int64_t offset, int64_t rows) {
    return MakeNullableFlatBatch(schema, offset, rows, /*null_pct=*/0);
}

// One low-cardinality VARCHAR per field, cheap enough that a wide schema leaves the per-column
// setup holding the measurement. The staggered offset keeps the columns from being identical.
Result<std::shared_ptr<arrow::Array>> MakeWideBatch(const std::shared_ptr<arrow::Schema>& schema,
                                                    int64_t offset, int64_t rows) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(schema->num_fields());
    for (int32_t i = 0; i < schema->num_fields(); ++i) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> column,
                               MakeStringColumn(rows, offset + i, kLowStringCardinality));
        columns.push_back(std::move(column));
    }
    return MakeStructArray(schema->fields(), columns);
}

Result<std::shared_ptr<arrow::Array>> MakeNestedBatch(const std::shared_ptr<arrow::Schema>& schema,
                                                      int64_t offset, int64_t rows) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> ids, MakeInt64Column(rows, offset));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> info, MakeStructColumn(rows, offset));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> tags, MakeListColumn(rows, offset));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> embedding, MakeVectorColumn(rows, offset));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> attrs,
                           MakeMapColumn(rows, offset, kMapEntries));
    return MakeStructArray(schema->fields(), {ids, info, tags, embedding, attrs});
}

// Whether every AddBatch call gets its own array or they all share one. It matters for dictionary
// input: arrow compares each batch's dictionary against the previous one, so N distinct-but-equal
// dictionaries and one dictionary written N times are not the same write. Only
// BM_ParquetWrite_MemoryThreshold reuses one batch; every other case generates each batch
// independently. That is about the arrays, not the values - generators that cycle with a period
// dividing the batch size, such as the boolean and the low-cardinality string ones, produce
// batches that are equal in value but separate objects.
enum class BatchReuse { kFresh, kReused };

Result<std::vector<std::shared_ptr<arrow::Array>>> MakeBatches(
    const std::shared_ptr<arrow::Schema>& schema, const BatchFactory& make_batch,
    int64_t rows_per_batch, int64_t total_rows = kRowsPerFile,
    BatchReuse reuse = BatchReuse::kFresh) {
    if (reuse == BatchReuse::kReused) {
        // One array written N times cannot express a short final batch, and silently writing a
        // full one instead would put more rows in the file than the reported metrics divide by.
        if (total_rows % rows_per_batch != 0) {
            return Status::Invalid("BatchReuse::kReused needs total_rows divisible by " +
                                   std::to_string(rows_per_batch) + ", got " +
                                   std::to_string(total_rows));
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> batch,
                               make_batch(schema, /*offset=*/0, rows_per_batch));
        return std::vector<std::shared_ptr<arrow::Array>>(total_rows / rows_per_batch,
                                                          std::move(batch));
    }
    std::vector<std::shared_ptr<arrow::Array>> batches;
    for (int64_t offset = 0; offset < total_rows; offset += rows_per_batch) {
        const int64_t rows = std::min<int64_t>(rows_per_batch, total_rows - offset);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> batch,
                               make_batch(schema, offset, rows));
        batches.push_back(std::move(batch));
    }
    return batches;
}

Result<int64_t> WriteParquetFile(const std::shared_ptr<FileSystem>& fs, const std::string& path,
                                 const std::shared_ptr<arrow::Schema>& schema,
                                 const std::vector<std::shared_ptr<arrow::Array>>& batches,
                                 const std::map<std::string, std::string>& options,
                                 const std::string& compression) {
    ParquetWriterBuilder writer_builder(schema, kWriteBatchSize, options);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out, fs->Create(path, /*overwrite=*/true));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatWriter> writer,
                           writer_builder.Build(out, compression));
    for (const auto& batch : batches) {
        // AddBatch imports - and so consumes - the C array, so each batch needs a fresh export.
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*batch, &c_array));
        PAIMON_RETURN_NOT_OK(writer->AddBatch(&c_array));
    }
    PAIMON_RETURN_NOT_OK(writer->Finish());
    PAIMON_RETURN_NOT_OK(out->Close());
    PAIMON_ASSIGN_OR_RAISE(FileStatus file_status, fs->GetFileStatus(path));
    return file_status.GetLen();
}

struct ReadStats {
    int64_t rows = 0;
    // Reader-side counterpart of file size: makes pruning and skipping visible apart from CPU.
    uint64_t storage_bytes = 0;
    // Without these a filtered case shows only that it got faster, not that pruning is why.
    uint64_t row_groups_total = 0;
    uint64_t row_groups_after_filter = 0;
    uint64_t batches = 0;
};

// Every counter read below is set unconditionally by the reader - the row-group pair in
// SetReadSchema, the batch count in NextBatch - so an absent one means the metric moved or stopped
// being recorded. Reporting that as a zero would hide the regression behind a plausible number,
// so the error propagates and fails the case instead.
Result<uint64_t> ReadCounter(const std::shared_ptr<paimon::Metrics>& metrics,
                             const std::string& name) {
    return metrics->GetCounter(name);
}

Result<ReadStats> ReadParquetFile(const std::shared_ptr<FileSystem>& fs, const std::string& path,
                                  const std::shared_ptr<arrow::Schema>& read_schema,
                                  const std::shared_ptr<Predicate>& predicate,
                                  const std::optional<RoaringBitmap32>& selection_bitmap,
                                  const std::map<std::string, std::string>& options,
                                  int32_t batch_size,
                                  const std::shared_ptr<arrow::MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(FileStatus file_status, fs->GetFileStatus(path));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, fs->Open(path));
    auto in_stream = std::make_shared<ArrowInputStreamAdapter>(input, file_status.GetLen(), pool);
    // Held separately so the counter outlives the adapter the reader takes ownership of.
    std::shared_ptr<std::atomic<uint64_t>> storage_read_bytes = in_stream->StorageReadBytes();
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<ParquetFileBatchReader> reader,
        ParquetFileBatchReader::Create(std::move(in_stream), options, batch_size,
                                       /*file_metadata=*/nullptr, storage_read_bytes, pool,
                                       /*hints=*/std::nullopt));

    // SetReadSchema imports the C schema and takes ownership of it.
    ArrowSchema c_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, &c_schema));
    PAIMON_RETURN_NOT_OK(reader->SetReadSchema(&c_schema, predicate, selection_bitmap));

    ReadStats stats;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        stats.rows += batch.first->length;
        ArrowArrayRelease(batch.first.get());
        ArrowSchemaRelease(batch.second.get());
    }

    std::shared_ptr<paimon::Metrics> metrics = reader->GetReaderMetrics();
    PAIMON_ASSIGN_OR_RAISE(
        stats.row_groups_total,
        ReadCounter(metrics, paimon::parquet::ParquetMetrics::READ_ROW_GROUPS_TOTAL));
    PAIMON_ASSIGN_OR_RAISE(
        stats.row_groups_after_filter,
        ReadCounter(metrics, paimon::parquet::ParquetMetrics::READ_ROW_GROUPS_AFTER_FILTER));
    PAIMON_ASSIGN_OR_RAISE(stats.batches,
                           ReadCounter(metrics, paimon::parquet::ParquetMetrics::READ_BATCH_COUNT));
    reader->Close();
    stats.storage_bytes = storage_read_bytes->load();
    return stats;
}

// Row groups the written file actually ended up with. The writer decides that itself - by row
// count, or by its pool crossing parquet.writer.max.memory.use - so the footer is the only
// reliable source.
//
// The write schema is deliberately not used to read back, because for two cases it does not
// describe the file. A VECTOR is stored as a LIST, and CollectLeafIndices branches on the file
// type, so a FixedSizeList read type against a file LIST is rejected outright. A dictionary column
// is stored as its value type; that one survives leaf collection, which compares nothing for
// atomic fields, and would fail later against read_data_type_ once a batch is read. Create()
// resolves the file's own schema and sets the row-group counters while doing so, which is all this
// needs - no read schema, no batch read, and neither trap.
Result<uint64_t> ReadRowGroupCount(const std::shared_ptr<FileSystem>& fs, const std::string& path,
                                   const std::shared_ptr<arrow::MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(FileStatus file_status, fs->GetFileStatus(path));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, fs->Open(path));
    auto in_stream = std::make_shared<ArrowInputStreamAdapter>(input, file_status.GetLen(), pool);
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<ParquetFileBatchReader> reader,
        ParquetFileBatchReader::Create(std::move(in_stream), /*options=*/{}, kReadBatchSize,
                                       /*file_metadata=*/nullptr, /*storage_read_bytes=*/nullptr,
                                       pool, /*hints=*/std::nullopt));
    PAIMON_ASSIGN_OR_RAISE(uint64_t row_groups,
                           ReadCounter(reader->GetReaderMetrics(),
                                       paimon::parquet::ParquetMetrics::READ_ROW_GROUPS_TOTAL));
    reader->Close();
    return row_groups;
}

// Owns a temporary directory plus the shared FileSystem / MemoryPool, removed on destruction.
class BenchmarkEnv {
 public:
    static Result<std::unique_ptr<BenchmarkEnv>> Create() {
        std::unique_ptr<paimon::test::UniqueTestDirectory> dir =
            paimon::test::UniqueTestDirectory::Create();
        if (!dir) {
            return Status::IOError("failed to create a temporary benchmark directory");
        }
        return std::unique_ptr<BenchmarkEnv>(new BenchmarkEnv(std::move(dir)));
    }

    const std::shared_ptr<FileSystem>& fs() const {
        return fs_;
    }

    // The reader takes an arrow pool; the writer builder allocates its own from the paimon pool.
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool() const {
        return arrow_pool_;
    }

    std::string PathOf(const std::string& file_name) const {
        return PathUtil::JoinPath(dir_->Str(), file_name);
    }

 private:
    explicit BenchmarkEnv(std::unique_ptr<paimon::test::UniqueTestDirectory> dir)
        : dir_(std::move(dir)),
          fs_(dir_->GetFileSystem()),
          arrow_pool_(paimon::GetArrowPool(paimon::GetDefaultPool())) {}

    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

// A file the read cases scan, built once per configuration; rebuilding it would dominate.
class ReadFixture {
 public:
    ReadFixture(const std::string& file_name, const std::shared_ptr<arrow::Schema>& schema,
                const BatchFactory& make_batch,
                const std::map<std::string, std::string>& write_options = {}) {
        status_ = Build(file_name, schema, make_batch, write_options);
    }

    const Status& status() const {
        return status_;
    }

    const std::string& path() const {
        return path_;
    }

    const std::shared_ptr<FileSystem>& fs() const {
        return env_->fs();
    }

    const std::shared_ptr<arrow::MemoryPool>& arrow_pool() const {
        return env_->arrow_pool();
    }

    int64_t file_bytes() const {
        return file_bytes_;
    }

 private:
    Status Build(const std::string& file_name, const std::shared_ptr<arrow::Schema>& schema,
                 const BatchFactory& make_batch,
                 const std::map<std::string, std::string>& write_options) {
        PAIMON_ASSIGN_OR_RAISE(env_, BenchmarkEnv::Create());
        path_ = env_->PathOf(file_name);
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<arrow::Array>> batches,
                               MakeBatches(schema, make_batch, kRowsPerBatch));
        std::map<std::string, std::string> options = write_options;
        options[paimon::parquet::PARQUET_PAGE_SIZE] = std::to_string(kPageSizeBytes);
        options[paimon::parquet::PARQUET_WRITE_MAX_ROW_GROUP_LENGTH] =
            std::to_string(kRowGroupLength);
        PAIMON_ASSIGN_OR_RAISE(file_bytes_, WriteParquetFile(env_->fs(), path_, schema, batches,
                                                             options, kDefaultCompression));
        return Status::OK();
    }

    std::unique_ptr<BenchmarkEnv> env_;
    std::string path_;
    int64_t file_bytes_ = 0;
    Status status_;
};

// Built on first use, so a case excluded by --benchmark_filter never pays to write its file.
// google/benchmark may report from a different thread than it registered on, hence the lock.
const ReadFixture& GetFixture(const std::string& key,
                              const std::function<std::unique_ptr<ReadFixture>()>& build) {
    static std::mutex mutex;
    static std::map<std::string, std::unique_ptr<ReadFixture>> fixtures;
    std::lock_guard<std::mutex> guard(mutex);
    std::unique_ptr<ReadFixture>& fixture = fixtures[key];
    if (!fixture) {
        fixture = build();
    }
    return *fixture;
}

const ReadFixture& FlatFixture() {
    return GetFixture("flat", [] {
        return std::make_unique<ReadFixture>("flat.parquet", FlatSchema(), &MakeFlatBatch);
    });
}

const ReadFixture& NestedFixture() {
    return GetFixture("nested", [] {
        return std::make_unique<ReadFixture>("nested.parquet", NestedSchema(), &MakeNestedBatch);
    });
}

const ReadFixture& NullableFlatFixture(int64_t null_pct) {
    const std::string key = "flat_nulls_" + std::to_string(null_pct);
    return GetFixture(key, [key, null_pct] {
        return std::make_unique<ReadFixture>(
            key + ".parquet", FlatSchema(),
            [null_pct](const std::shared_ptr<arrow::Schema>& schema, int64_t offset, int64_t rows) {
                return MakeNullableFlatBatch(schema, offset, rows, null_pct);
            });
    });
}

// A one-column file, for read cases that isolate a single decoder.
const ReadFixture& ColumnFixture(const std::string& key,
                                 const std::shared_ptr<arrow::Schema>& schema,
                                 const ColumnFactory& make_column) {
    return GetFixture(key, [key, schema, make_column] {
        return std::make_unique<ReadFixture>(key + ".parquet", schema,
                                             SingleColumnBatch(make_column));
    });
}

// The flat fixture only carries precision 18, so without this the FIXED_LEN_BYTE_ARRAY path that
// precision 38 takes is written but never read.
const ReadFixture& DecimalFixture(int32_t precision) {
    return ColumnFixture("decimal_" + std::to_string(precision), DecimalSchema(precision),
                         [precision](int64_t rows, int64_t offset) {
                             return MakeDecimalColumn(rows, offset, precision, /*scale=*/4);
                         });
}

// The flat schema carries no floating-point column, so a DOUBLE read needs a file of its own.
const ReadFixture& DoubleFixture() {
    return ColumnFixture("double", DoubleSchema(), &MakeDoubleColumn);
}

// The same data with dictionary encoding off, giving the read side a plain baseline.
const ReadFixture& PlainFlatFixture() {
    return GetFixture("flat_plain", [] {
        std::map<std::string, std::string> options;
        options[paimon::parquet::PARQUET_ENABLE_DICTIONARY] = "false";
        return std::make_unique<ReadFixture>("flat_plain.parquet", FlatSchema(), &MakeFlatBatch,
                                             options);
    });
}

// google/benchmark's own main exits 0 whatever happened, so a case that called SkipWithError
// prints as skipped while `ctest -L benchmark` still passes. Nothing here skips on purpose, so
// the flag every error sets becomes the process exit code. It is recorded here rather than in a
// custom reporter because passing one to RunSpecifiedBenchmarks would bypass
// CreateDefaultDisplayReporter and with it --benchmark_format, --benchmark_color and
// --benchmark_counters_tabular.
std::atomic<bool> g_failed{false};

bool FailBenchmark(::benchmark::State& state, const Status& status) {
    if (status.ok()) {
        return false;
    }
    g_failed.store(true);
    state.SkipWithError(status.ToString().c_str());
    return true;
}

class Timer {
 public:
    double ElapsedNanos() const {
        return std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - started_)
            .count();
    }

 private:
    std::chrono::steady_clock::time_point started_ = std::chrono::steady_clock::now();
};

// google/benchmark reports items/s; ns per row is what the issue asks for, so the timed region
// is also measured directly and divided by the row count.
void ReportRowRate(::benchmark::State& state, int64_t rows, double elapsed_ns) {
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * rows);
    const double total_rows = static_cast<double>(state.iterations()) * static_cast<double>(rows);
    if (total_rows > 0) {
        state.counters["ns_per_row"] = ::benchmark::Counter(elapsed_ns / total_rows);
    }
}

// Bytes per iteration, plus the per-row form that shows a CPU-for-size trade next to ns_per_row.
void ReportBytes(::benchmark::State& state, const std::string& name, int64_t bytes, int64_t rows) {
    state.counters[name] = ::benchmark::Counter(static_cast<double>(bytes));
    if (rows > 0) {
        state.counters["bytes_per_row"] =
            ::benchmark::Counter(static_cast<double>(bytes) / static_cast<double>(rows));
    }
}

// Everything the writer needs per file - properties, output stream, schema conversion, footer -
// is inside the timed region, because that is what a caller pays per data file.
void RunWriteBenchmark(::benchmark::State& state, const std::shared_ptr<arrow::Schema>& schema,
                       const BatchFactory& make_batch, int64_t rows_per_batch,
                       const std::map<std::string, std::string>& options,
                       const std::string& compression, int64_t total_rows = kRowsPerFile,
                       BatchReuse reuse = BatchReuse::kFresh) {
    Result<std::unique_ptr<BenchmarkEnv>> env = BenchmarkEnv::Create();
    if (FailBenchmark(state, env.status())) {
        return;
    }
    Result<std::vector<std::shared_ptr<arrow::Array>>> batches =
        MakeBatches(schema, make_batch, rows_per_batch, total_rows, reuse);
    if (FailBenchmark(state, batches.status())) {
        return;
    }
    const std::string path = env.value()->PathOf("write_case.parquet");

    int64_t file_bytes = 0;
    Timer timer;
    for (auto _ : state) {
        Result<int64_t> written = WriteParquetFile(env.value()->fs(), path, schema, batches.value(),
                                                   options, compression);
        if (FailBenchmark(state, written.status())) {
            return;
        }
        file_bytes = written.value();
    }
    // Captured before the read-back below, which reopens the file and parses its footer. Reading
    // the timer after would put that inside ns_per_row but not inside google/benchmark's own
    // real_time, leaving the two disagreeing by a fixed amount that matters at low iteration
    // counts.
    const double elapsed_ns = timer.ElapsedNanos();

    // Read back rather than computed: with a byte-triggered flush the count is not predictable
    // from the arguments, and where a row-count limit does make it predictable, reporting the real
    // number is what would catch the prediction being wrong.
    Result<uint64_t> row_groups =
        ReadRowGroupCount(env.value()->fs(), path, env.value()->arrow_pool());
    if (FailBenchmark(state, row_groups.status())) {
        return;
    }

    ReportRowRate(state, total_rows, elapsed_ns);
    ReportBytes(state, "file_bytes", file_bytes, total_rows);
    state.counters["batches"] = ::benchmark::Counter(
        static_cast<double>((total_rows + rows_per_batch - 1) / rows_per_batch));
    state.counters["row_groups"] = ::benchmark::Counter(static_cast<double>(row_groups.value()));
}

// Single-column variant: one column isolates one encoder.
void RunColumnWriteBenchmark(::benchmark::State& state, const std::shared_ptr<arrow::Field>& field,
                             const ColumnFactory& make_column, int64_t rows_per_batch,
                             const std::map<std::string, std::string>& options,
                             int64_t total_rows = kRowsPerFile,
                             BatchReuse reuse = BatchReuse::kFresh) {
    RunWriteBenchmark(state, arrow::schema({field}), SingleColumnBatch(make_column), rows_per_batch,
                      options, kDefaultCompression, total_rows, reuse);
}

void BM_ParquetWrite_Int64(::benchmark::State& state) {
    RunColumnWriteBenchmark(state, MakeField("id", arrow::int64(), 0), &MakeInt64Column,
                            kRowsPerBatch, /*options=*/{});
}

void BM_ParquetWrite_Double(::benchmark::State& state) {
    RunColumnWriteBenchmark(state, MakeField("value", arrow::float64(), 0), &MakeDoubleColumn,
                            kRowsPerBatch, /*options=*/{});
}

void BM_ParquetWrite_Boolean(::benchmark::State& state) {
    RunColumnWriteBenchmark(state, MakeField("flag", arrow::boolean(), 0), &MakeBooleanColumn,
                            kRowsPerBatch, /*options=*/{});
}

void BM_ParquetWrite_String(::benchmark::State& state) {
    const int64_t cardinality = state.range(0);
    RunColumnWriteBenchmark(state, MakeField("name", arrow::utf8(), 0),
                            [cardinality](int64_t rows, int64_t offset) {
                                return MakeStringColumn(rows, offset, cardinality);
                            },
                            kRowsPerBatch, /*options=*/{});
}

// arg: number of distinct values. The flat control for BM_ParquetWrite_DictionaryInt32.
void BM_ParquetWrite_FlatInt32(::benchmark::State& state) {
    const int64_t cardinality = state.range(0);
    RunColumnWriteBenchmark(state, MakeField("value", arrow::int32(), 0),
                            [cardinality](int64_t rows, int64_t offset) {
                                return MakeInt32Column(rows, offset, cardinality);
                            },
                            kRowsPerBatch, /*options=*/{});
}

// arg: dictionary cardinality. Same values as BM_ParquetWrite_String at the same cardinality, but
// handed to the writer already dictionary-encoded - nothing in ParquetWriterBuilder rejects a
// dictionary arrow type, so the writer does reach this path - and the pair isolates what it saves
// when it can pass indices through instead of materializing every value.
void BM_ParquetWrite_DictionaryString(::benchmark::State& state) {
    const int64_t cardinality = state.range(0);
    RunColumnWriteBenchmark(state, MakeField("name", DictionaryStringType(), 0),
                            [cardinality](int64_t rows, int64_t offset) {
                                return MakeDictionaryStringColumn(rows, offset, cardinality);
                            },
                            kRowsPerBatch, /*options=*/{});
}

// The same axis on an INTEGER dictionary, which arrow cannot direct-write - is_base_binary_like
// excludes int32, so it densifies first. Its baseline is BM_ParquetWrite_FlatInt32 at the same
// cardinality, not the String case: only the flat INT32 control holds value, width and encoding
// fixed, so only that delta is the materialization cost.
void BM_ParquetWrite_DictionaryInt32(::benchmark::State& state) {
    const int64_t cardinality = state.range(0);
    RunColumnWriteBenchmark(state, MakeField("value", DictionaryInt32Type(), 0),
                            [cardinality](int64_t rows, int64_t offset) {
                                return MakeDictionaryInt32Column(rows, offset, cardinality);
                            },
                            kRowsPerBatch, /*options=*/{});
}

// Same data with dictionary encoding turned off, as an encoding baseline.
void BM_ParquetWrite_StringNoDictionary(::benchmark::State& state) {
    const int64_t cardinality = state.range(0);
    std::map<std::string, std::string> options;
    options[paimon::parquet::PARQUET_ENABLE_DICTIONARY] = "false";
    RunColumnWriteBenchmark(
        state, MakeField("name", arrow::utf8(), 0),
        [cardinality](int64_t rows, int64_t offset) {
            return MakeStringColumn(rows, offset, cardinality);
        },
        kRowsPerBatch, options);
}

void BM_ParquetWrite_Decimal(::benchmark::State& state) {
    const auto precision = static_cast<int32_t>(state.range(0));
    RunColumnWriteBenchmark(state, MakeField("amount", arrow::decimal128(precision, 4), 0),
                            [precision](int64_t rows, int64_t offset) {
                                return MakeDecimalColumn(rows, offset, precision, /*scale=*/4);
                            },
                            kRowsPerBatch, /*options=*/{});
}

void BM_ParquetWrite_Struct(::benchmark::State& state) {
    RunColumnWriteBenchmark(state, MakeField("info", StructColumnType(), 0), &MakeStructColumn,
                            kRowsPerBatch, /*options=*/{});
}

void BM_ParquetWrite_List(::benchmark::State& state) {
    RunColumnWriteBenchmark(state, MakeField("tags", ListColumnType(), 0), &MakeListColumn,
                            kRowsPerBatch, /*options=*/{});
}

void BM_ParquetWrite_Vector(::benchmark::State& state) {
    RunColumnWriteBenchmark(state, MakeField("embedding", VectorColumnType(), 0), &MakeVectorColumn,
                            kRowsPerBatch, /*options=*/{});
}

// arg: entries per map row.
void BM_ParquetWrite_Map(::benchmark::State& state) {
    const auto entries = static_cast<int32_t>(state.range(0));
    RunColumnWriteBenchmark(
        state, MakeField("attrs", MapColumnType(), 0),
        [entries](int64_t rows, int64_t offset) { return MakeMapColumn(rows, offset, entries); },
        kRowsPerBatch, /*options=*/{});
}

// arg: percentage of nulls. Every field is nullable, so the column is `optional` and definition
// levels are written at every density including zero; the sweep moves what arrow's null-free fast
// path is worth. BIGINT is the narrowest column, so levels are the largest share of what is left.
void BM_ParquetWrite_Nulls(::benchmark::State& state) {
    const int64_t null_pct = state.range(0);
    RunColumnWriteBenchmark(state, MakeField("id", arrow::int64(), 0),
                            NullableColumnFactory(&MakeInt64Column, null_pct), kRowsPerBatch,
                            /*options=*/{});
}

// arg: rows per AddBatch call, at a fixed total row count. Smaller batches mean the same data
// carries more per-batch fixed cost, and the file-level setup is amortized over more calls.
void BM_ParquetWrite_BatchSize(::benchmark::State& state) {
    RunColumnWriteBenchmark(state, MakeField("id", arrow::int64(), 0), &MakeInt64Column,
                            state.range(0), /*options=*/{});
}

// arg: number of columns at a fixed row count. With BM_ParquetWrite_BatchSize this separates the
// two ways per-AddBatch work grows - more calls, or more columns per call - where that work is
// AddBatch importing the C array plus arrow walking the schema, neither of which encodes a value.
void BM_ParquetWrite_ColumnCount(::benchmark::State& state) {
    const auto columns = static_cast<int32_t>(state.range(0));
    arrow::FieldVector fields;
    fields.reserve(columns);
    for (int32_t i = 0; i < columns; ++i) {
        fields.push_back(MakeField("c" + std::to_string(i), arrow::utf8(), i));
    }
    RunWriteBenchmark(state, arrow::schema(fields), &MakeWideBatch, kRowsPerBatch, /*options=*/{},
                      kDefaultCompression);
    state.counters["columns"] = ::benchmark::Counter(static_cast<double>(columns));
}

// arg: maximum rows per row group, the write-side half of BM_ParquetRead_Filtered - a row group
// is the unit the reader prunes, so more of them buys finer pruning at the cost of footer
// metadata, flush work and codec material. The byte-triggered counterpart is
// BM_ParquetWrite_MemoryThreshold.
void BM_ParquetWrite_RowGroupSize(::benchmark::State& state) {
    const int64_t row_group_length = state.range(0);
    std::map<std::string, std::string> options;
    options[paimon::parquet::PARQUET_WRITE_MAX_ROW_GROUP_LENGTH] = std::to_string(row_group_length);
    RunWriteBenchmark(state, FlatSchema(), &MakeFlatBatch, kRowsPerBatch, options,
                      kDefaultCompression);
}

// args: the parquet.writer.max.memory.use threshold in KiB, and the number of AddBatch calls.
// Unlike every other case here the file is not a fixed kRowsPerFile: batches are a fixed 10'000
// rows and the total grows with the batch count, because the question is whether retained size
// accumulates across batches until the writer cuts a row group. Holding the total fixed and
// shrinking the batch would measure AddBatch granularity instead, which is what
// BM_ParquetWrite_BatchSize already does. The input is low-cardinality dictionary data, where
// retained and flat size differ most, written as one batch object repeatedly rather than a fresh
// one each time so arrow sees the same dictionary rather than N equal ones.
//
// Nothing here sets a row-group row limit, so the byte threshold is the only thing that can
// trigger a flush. row_groups reports whether it fired at all; if it reads 1 everywhere the sweep
// measured nothing, and the unit test only covers the mechanism, not this particular setting.
void BM_ParquetWrite_MemoryThreshold(::benchmark::State& state) {
    constexpr int64_t kFlushRowsPerBatch = 10'000;
    std::map<std::string, std::string> options;
    options[paimon::parquet::PARQUET_WRITER_MAX_MEMORY_USE] = std::to_string(state.range(0) * 1024);
    RunColumnWriteBenchmark(
        state, MakeField("name", DictionaryStringType(), 0),
        [](int64_t rows, int64_t offset) {
            return MakeDictionaryStringColumn(rows, offset, kLowStringCardinality);
        },
        kFlushRowsPerBatch, options, kFlushRowsPerBatch * state.range(1), BatchReuse::kReused);
}

// Codec sweep over the mixed flat schema, so the CPU-versus-size trade shows on data that is not
// uniformly one type. Levels stay at the ParquetWriterBuilder defaults. The names are the ones
// Parquet accepts, not the ones arrow does: "lz4" resolves to arrow's LZ4_FRAME, which
// parquet::IsCodecSupported rejects, so the two framings Parquet defines are spelled out.
void BM_ParquetWrite_Compression(::benchmark::State& state, const char* compression) {
    RunWriteBenchmark(state, FlatSchema(), &MakeFlatBatch, kRowsPerBatch, /*options=*/{},
                      compression);
}

// Rows a read case has to materialize for its number to mean anything. Pruning is not precise, so
// a filtered case gets a range rather than an exact count. Without the bound, a fixture that
// silently stopped producing rows would just look fast.
struct RowExpectation {
    int64_t min = kRowsPerFile;
    int64_t max = kRowsPerFile;
};

void RunReadBenchmark(::benchmark::State& state, const ReadFixture& fixture,
                      const std::shared_ptr<arrow::Schema>& read_schema,
                      const std::shared_ptr<Predicate>& predicate,
                      const std::optional<RoaringBitmap32>& selection_bitmap,
                      const std::map<std::string, std::string>& options, int32_t batch_size,
                      const RowExpectation& expected = {}) {
    if (FailBenchmark(state, fixture.status())) {
        return;
    }

    ReadStats stats;
    Timer timer;
    for (auto _ : state) {
        Result<ReadStats> result =
            ReadParquetFile(fixture.fs(), fixture.path(), read_schema, predicate, selection_bitmap,
                            options, batch_size, fixture.arrow_pool());
        if (FailBenchmark(state, result.status())) {
            return;
        }
        stats = result.value();
    }
    if (stats.rows < expected.min || stats.rows > expected.max) {
        FailBenchmark(state, Status::Invalid("read " + std::to_string(stats.rows) +
                                             " rows, expected " + std::to_string(expected.min) +
                                             " to " + std::to_string(expected.max)));
        return;
    }

    const double elapsed_ns = timer.ElapsedNanos();
    ReportRowRate(state, stats.rows, elapsed_ns);
    // Normalized by what the file holds, not what this case materialized: pruning drops the
    // per-materialized-row numerator and denominator together, so ns_per_row and bytes_per_row can
    // rise while the run gets faster. Only a denominator every setting shares is comparable across
    // settings that prune by different amounts.
    const double total_input_rows =
        static_cast<double>(state.iterations()) * static_cast<double>(kRowsPerFile);
    if (total_input_rows > 0) {
        state.counters["ns_per_input_row"] = ::benchmark::Counter(elapsed_ns / total_input_rows);
        state.counters["bytes_per_input_row"] = ::benchmark::Counter(
            static_cast<double>(stats.storage_bytes) / static_cast<double>(kRowsPerFile));
    }
    ReportBytes(state, "read_bytes", static_cast<int64_t>(stats.storage_bytes), stats.rows);
    state.counters["rows_read"] = ::benchmark::Counter(static_cast<double>(stats.rows));
    state.counters["file_bytes"] = ::benchmark::Counter(static_cast<double>(fixture.file_bytes()));
    state.counters["row_groups"] =
        ::benchmark::Counter(static_cast<double>(stats.row_groups_total));
    state.counters["row_groups_after_filter"] =
        ::benchmark::Counter(static_cast<double>(stats.row_groups_after_filter));
    state.counters["batches"] = ::benchmark::Counter(static_cast<double>(stats.batches));
}

void BM_ParquetRead_FullScan(::benchmark::State& state) {
    RunReadBenchmark(state, FlatFixture(), FlatSchema(), /*predicate=*/nullptr,
                     /*selection_bitmap=*/std::nullopt, /*options=*/{}, kReadBatchSize);
}

// One column at a time, separating per-column transfer from materializing the whole row.
void BM_ParquetRead_Projection(::benchmark::State& state, const char* column) {
    std::shared_ptr<arrow::Field> field = FlatSchema()->GetFieldByName(column);
    if (!field) {
        FailBenchmark(state, Status::Invalid("unknown projection column"));
        return;
    }
    RunReadBenchmark(state, FlatFixture(), arrow::schema({field}), /*predicate=*/nullptr,
                     /*selection_bitmap=*/std::nullopt, /*options=*/{}, kReadBatchSize);
}

// args: percentage of rows the predicate keeps, and whether page-level pruning is on. The `id`
// column is ordered, so the surviving rows form a prefix; running both page-index settings makes
// the pruning gain attributable instead of merely visible.
void BM_ParquetRead_Filtered(::benchmark::State& state) {
    const int64_t threshold = kRowsPerFile * state.range(0) / 100;
    // `id` is ordered, so row-group statistics alone must already discard every group past the
    // threshold - that holds with page-index filtering off too. Bounding at the row-group grain
    // rather than at kRowsPerFile is what makes a pruning regression fail the case instead of
    // quietly reading the whole file.
    const int64_t row_group_bound =
        ((threshold + kRowGroupLength - 1) / kRowGroupLength) * kRowGroupLength;
    const bool enable_page_index = state.range(1) != 0;
    std::shared_ptr<Predicate> predicate = PredicateBuilder::LessThan(
        /*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT, Literal(threshold));
    std::map<std::string, std::string> options;
    options[paimon::parquet::PARQUET_READ_ENABLE_PAGE_INDEX_FILTER] =
        enable_page_index ? "true" : "false";
    RunReadBenchmark(state, FlatFixture(), FlatSchema(), predicate,
                     /*selection_bitmap=*/std::nullopt, options, kReadBatchSize,
                     RowExpectation{threshold, row_group_bound});
}

// arg: distance between selected rows. The strides straddle the coalesce hole limit (32 rows by
// default), which splits this into two regimes: at or below it neighbouring single-row ranges
// merge into long spans, so rows_read runs far ahead of selected_rows and nothing is skipped;
// above it every row stays its own range and arrow's Skip decodes and discards each gap instead.
// Compare read_bytes and ns_per_input_row across the two.
void BM_ParquetRead_SkipHeavy(::benchmark::State& state) {
    const int64_t stride = state.range(0);
    RoaringBitmap32 bitmap;
    for (int64_t row = 0; row < kRowsPerFile; row += stride) {
        bitmap.Add(static_cast<int32_t>(row));
    }
    const int64_t selected = bitmap.Cardinality();
    state.counters["selected_rows"] = ::benchmark::Counter(static_cast<double>(selected));
    RunReadBenchmark(state, FlatFixture(), FlatSchema(), /*predicate=*/nullptr, bitmap,
                     /*options=*/{}, kReadBatchSize, RowExpectation{selected, kRowsPerFile});
}

// arg: percentage of nulls, the read side of BM_ParquetWrite_Nulls: definition levels have to be
// decoded back into a validity bitmap, and past some density arrow may do less work, not more.
void BM_ParquetRead_Nulls(::benchmark::State& state) {
    RunReadBenchmark(state, NullableFlatFixture(state.range(0)), FlatSchema(),
                     /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt, /*options=*/{},
                     kReadBatchSize);
}

// The VARCHAR column from a dictionary-encoded file and from a plain-encoded one. Both are
// decoded by arrow, so this is arrow's dictionary path against its plain path on equal values.
void BM_ParquetRead_Encoding(::benchmark::State& state, bool enable_dictionary) {
    std::shared_ptr<arrow::Field> field = FlatSchema()->GetFieldByName("name");
    RunReadBenchmark(state, enable_dictionary ? FlatFixture() : PlainFlatFixture(),
                     arrow::schema({field}), /*predicate=*/nullptr,
                     /*selection_bitmap=*/std::nullopt, /*options=*/{}, kReadBatchSize);
}

// arg: decimal precision, the read side of BM_ParquetWrite_Decimal. Precision picks the physical
// type - INT32, INT64 or FIXED_LEN_BYTE_ARRAY, since ParquetWriterBuilder enables
// store_decimal_as_integer - and the three take different paths back to Decimal128Array.
void BM_ParquetRead_Decimal(::benchmark::State& state) {
    const auto precision = static_cast<int32_t>(state.range(0));
    RunReadBenchmark(state, DecimalFixture(precision), DecimalSchema(precision),
                     /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt, /*options=*/{},
                     kReadBatchSize);
}

void BM_ParquetRead_Double(::benchmark::State& state) {
    RunReadBenchmark(state, DoubleFixture(), DoubleSchema(), /*predicate=*/nullptr,
                     /*selection_bitmap=*/std::nullopt, /*options=*/{}, kReadBatchSize);
}

// arg: rows per NextBatch call. At a fixed row count this turns the per-batch fixed cost of
// ParquetFileBatchReader::NextBatch - Validate, the two metrics counters, the ArrowSchema
// export - into a number, separated from the per-row decoding cost.
void BM_ParquetRead_BatchSize(::benchmark::State& state) {
    RunReadBenchmark(state, FlatFixture(), FlatSchema(), /*predicate=*/nullptr,
                     /*selection_bitmap=*/std::nullopt, /*options=*/{},
                     static_cast<int32_t>(state.range(0)));
}

// Each nested column costs definition and repetition levels a flat column does not pay.
void BM_ParquetRead_Nested(::benchmark::State& state, const char* column) {
    std::shared_ptr<arrow::Schema> full_schema = NestedReadSchema();
    std::shared_ptr<arrow::Schema> read_schema = full_schema;
    if (column != nullptr) {
        std::shared_ptr<arrow::Field> field = full_schema->GetFieldByName(column);
        if (!field) {
            FailBenchmark(state, Status::Invalid("unknown nested column"));
            return;
        }
        read_schema = arrow::schema({field});
    }
    RunReadBenchmark(state, NestedFixture(), read_schema, /*predicate=*/nullptr,
                     /*selection_bitmap=*/std::nullopt, /*options=*/{}, kReadBatchSize);
}

}  // namespace

BENCHMARK(BM_ParquetWrite_Int64)->Unit(benchmark::kMillisecond)->UseRealTime();
BENCHMARK(BM_ParquetWrite_Double)->Unit(benchmark::kMillisecond)->UseRealTime();
BENCHMARK(BM_ParquetWrite_Boolean)->Unit(benchmark::kMillisecond)->UseRealTime();
BENCHMARK(BM_ParquetWrite_String)
    ->ArgName("cardinality")
    ->Arg(10)
    ->Arg(1000)
    ->Arg(kRowsPerFile)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetWrite_StringNoDictionary)
    ->ArgName("cardinality")
    ->Arg(10)
    ->Arg(1000)
    ->Arg(kRowsPerFile)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetWrite_FlatInt32)
    ->ArgName("cardinality")
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetWrite_DictionaryString)
    ->ArgName("cardinality")
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetWrite_DictionaryInt32)
    ->ArgName("cardinality")
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetWrite_Decimal)
    ->ArgName("precision")
    ->Arg(9)
    ->Arg(18)
    ->Arg(38)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetWrite_Struct)->Unit(benchmark::kMillisecond)->UseRealTime();
BENCHMARK(BM_ParquetWrite_List)->Unit(benchmark::kMillisecond)->UseRealTime();
BENCHMARK(BM_ParquetWrite_Vector)->Unit(benchmark::kMillisecond)->UseRealTime();
BENCHMARK(BM_ParquetWrite_Map)
    ->ArgName("entries")
    ->Arg(3)
    ->Arg(5)
    ->Arg(10)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetWrite_Nulls)
    ->ArgName("null_pct")
    ->Arg(0)
    ->Arg(20)
    ->Arg(50)
    ->Arg(70)
    ->Arg(100)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetWrite_BatchSize)
    ->ArgName("rows_per_batch")
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetWrite_ColumnCount)
    ->ArgName("columns")
    ->Arg(1)
    ->Arg(5)
    ->Arg(10)
    ->Arg(20)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetWrite_RowGroupSize)
    ->ArgName("row_group_rows")
    ->Arg(5000)
    ->Arg(25000)
    ->Arg(kRowsPerFile)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetWrite_MemoryThreshold)
    ->ArgNames({"max_memory_kib", "batches"})
    ->Args({512, 50})
    ->Args({512, 200})
    ->Args({4096, 200})
    ->Args({65536, 200})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetWrite_Compression, none, "none")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetWrite_Compression, snappy, "snappy")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetWrite_Compression, gzip, "gzip")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetWrite_Compression, lz4_raw, "lz4_raw")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetWrite_Compression, lz4_hadoop, "lz4_hadoop")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetWrite_Compression, brotli, "brotli")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetWrite_Compression, zstd, "zstd")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_ParquetRead_FullScan)->Unit(benchmark::kMillisecond)->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetRead_Projection, id, "id")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetRead_Projection, name, "name")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetRead_Projection, amount, "amount")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetRead_Filtered)
    ->ArgNames({"keep_pct", "page_index"})
    ->Args({1, 1})
    ->Args({1, 0})
    ->Args({10, 1})
    ->Args({10, 0})
    ->Args({50, 1})
    ->Args({50, 0})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetRead_SkipHeavy)
    ->ArgName("stride")
    ->Arg(8)
    ->Arg(32)
    ->Arg(64)
    ->Arg(512)
    ->Arg(4096)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetRead_Nulls)
    ->ArgName("null_pct")
    ->Arg(0)
    ->Arg(20)
    ->Arg(50)
    ->Arg(70)
    ->Arg(100)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetRead_Encoding, dictionary, true)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetRead_Encoding, plain, false)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetRead_Decimal)
    ->ArgName("precision")
    ->Arg(9)
    ->Arg(18)
    ->Arg(38)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_ParquetRead_Double)->Unit(benchmark::kMillisecond)->UseRealTime();
BENCHMARK(BM_ParquetRead_BatchSize)
    ->ArgName("batch_size")
    ->Arg(512)
    ->Arg(4096)
    ->Arg(16384)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetRead_Nested, info_struct, "info")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetRead_Nested, tags_list, "tags")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetRead_Nested, embedding_vector, "embedding")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetRead_Nested, attrs_map, "attrs")
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK_CAPTURE(BM_ParquetRead_Nested, all_columns, nullptr)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return g_failed.load() ? 1 : 0;
}

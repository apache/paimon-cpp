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

// Round-trip unit tests for the Vortex file format: write, then read back via vortex-ffi.

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_stats_extractor.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::vortex::test {

class VortexFileFormatTest : public ::testing::Test {
 public:
    void SetUp() override {
        ASSERT_OK_AND_ASSIGN(format_,
                             FileFormatFactory::Get("vortex", {{"file.format", "vortex"}}));
        file_system_ = std::make_shared<LocalFileSystem>();
        directory_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_NE(directory_, nullptr);
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
    }

    // Writes `array` (a struct array matching `schema`) to a Vortex file at `path`, pushing it in
    // `batch_size` slices exactly like the production write path does.
    Status WriteFile(const std::string& path, const std::shared_ptr<arrow::Schema>& schema,
                     const std::shared_ptr<arrow::Array>& array, int32_t batch_size) const {
        ::ArrowSchema ffi_schema = {};
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &ffi_schema));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriterBuilder> writer_builder,
                               format_->CreateWriterBuilder(&ffi_schema, batch_size));
        writer_builder->WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> output,
                               file_system_->Create(path, /*overwrite=*/false));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatWriter> writer,
                               writer_builder->Build(output, "zstd"));
        for (int64_t offset = 0; offset < array->length(); offset += batch_size) {
            std::shared_ptr<arrow::Array> slice = array->Slice(offset, batch_size);
            ::ArrowArray ffi_array = {};
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*slice, &ffi_array));
            PAIMON_RETURN_NOT_OK(writer->AddBatch(&ffi_array));
        }
        PAIMON_RETURN_NOT_OK(writer->Finish());
        return output->Close();
    }

    // Reads the whole Vortex file at `path` back as a single concatenated struct array, checking
    // the reported row count and that no batch exceeds the configured batch size.
    Result<std::shared_ptr<arrow::Array>> ReadFile(const std::string& path,
                                                   const std::shared_ptr<arrow::Schema>& schema,
                                                   int32_t batch_size,
                                                   uint64_t expected_row_count) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> reader_builder,
                               format_->CreateReaderBuilder(batch_size));
        reader_builder->WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, file_system_->Open(path));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> reader,
                               reader_builder->Build(input));
        ::ArrowSchema ffi_schema = {};
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &ffi_schema));
        PAIMON_RETURN_NOT_OK(reader->SetReadSchema(&ffi_schema, /*predicate=*/nullptr,
                                                   /*selection_bitmap=*/std::nullopt));
        PAIMON_ASSIGN_OR_RAISE(uint64_t total_rows, reader->GetNumberOfRows());
        if (total_rows != expected_row_count) {
            return Status::Invalid("unexpected Vortex row count");
        }

        std::vector<std::shared_ptr<arrow::Array>> batches;
        uint64_t rows_seen = 0;
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
            if (BatchReader::IsEofBatch(batch)) {
                break;
            }
            PAIMON_RETURN_NOT_OK(paimon::test::ReadResultCollector::CheckBatchOffset(batch));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> arrow_batch,
                arrow::ImportArray(batch.first.get(), batch.second.get()));
            if (arrow_batch->length() > batch_size) {
                return Status::Invalid("Vortex read batch exceeds configured batch size");
            }
            rows_seen += static_cast<uint64_t>(arrow_batch->length());
            batches.push_back(std::move(arrow_batch));
        }
        if (rows_seen != expected_row_count) {
            return Status::Invalid("Vortex batches do not contain all rows");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> result,
                                          arrow::Concatenate(batches, arrow_pool_.get()));
        return result;
    }

    void AssertReadWithBatchSizes(const std::string& path,
                                  const std::shared_ptr<arrow::Schema>& schema,
                                  const std::shared_ptr<arrow::Array>& expected,
                                  std::initializer_list<int32_t> batch_sizes) const {
        for (int32_t batch_size : batch_sizes) {
            SCOPED_TRACE("batch_size=" + std::to_string(batch_size));
            ASSERT_OK_AND_ASSIGN(
                std::shared_ptr<arrow::Array> actual,
                ReadFile(path, schema, batch_size,
                         /*expected_row_count=*/static_cast<uint64_t>(expected->length())));
            ASSERT_TRUE(actual->Equals(expected)) << actual->ToString() << "\nvs\n"
                                                  << expected->ToString();
        }
    }

 protected:
    std::shared_ptr<FileFormat> format_;
    std::shared_ptr<LocalFileSystem> file_system_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> directory_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

TEST_F(VortexFileFormatTest, WriteThenRead) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "data.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields), R"([[1,"one"],[2,null],[3,"three"],[4,"four"],[5,"five"]])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 5, 8});
}

TEST_F(VortexFileFormatTest, WriteThenReadSupportedTypes) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "supported-types.vortex");
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),
        arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),
        arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),
        arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),
        arrow::field("f8", arrow::binary()),
        arrow::field("f9", arrow::date32()),
        arrow::field("f10", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f11", arrow::decimal128(2, 2)),
        arrow::field("f12", arrow::decimal128(30, 2)),
        arrow::field("f13", arrow::list(arrow::float32())),
    };
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          [true,-128,-32768,-2147483648,-4294967298,0.5,1.141592659,"vortex","binary",
           -1,"1970-01-01 00:00:00.000000001","-0.99","-123456789987654321.45",[1.5,null]],
          [false,127,32767,2147483647,4294967296,2.0,3.141592657,"","",
           12345,"2030-12-31 23:59:59.999999999","0.78","123456789987654321.45",[]],
          [null,null,null,null,null,null,null,null,null,null,null,null,null,null]
        ])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 5});
}

// Regression: a list<utf8> column round-trips through Vortex's Utf8View export. The leaf strings
// include a null element, an empty list, a null list, an empty string and a string long enough to
// exceed the 12-byte StringView inline threshold (exercising the non-inline view buffer path).
TEST_F(VortexFileFormatTest, WriteThenReadListOfString) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "list-of-string.vortex");
    arrow::FieldVector fields = {arrow::field("tags", arrow::list(arrow::utf8()))};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          [["one","two",null]],
          [[]],
          [null],
          [["","a-longer-string-that-exceeds-the-inline-view-threshold","x"]],
          [[null,null]]
        ])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 5});
}

// Regression for the FIXED_SIZE_LIST branch of NormalizeViewArray: without recursion into the
// elements, a fixed_size_list<utf8> column comes back as fixed_size_list<utf8view> (Vortex
// hardcodes Utf8 -> Utf8View), mismatching the normalized read schema and silently corrupting the
// data.
TEST_F(VortexFileFormatTest, WriteThenReadFixedSizeListOfString) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "fsl-of-string.vortex");
    arrow::FieldVector fields = {arrow::field("triples", arrow::fixed_size_list(arrow::utf8(), 3))};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          [["a","bb","ccc"]],
          [null],
          [["","a-longer-string-that-exceeds-the-inline-view-threshold","z"]],
          [[null,"q",null]]
        ])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 4});
}

// Regression for a struct nested inside a list: the LIST branch recurses into the struct, whose
// utf8 leaf is a view; the combination must normalize back to list<struct<utf8, int32>>.
TEST_F(VortexFileFormatTest, WriteThenReadListOfStruct) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "list-of-struct.vortex");
    std::shared_ptr<arrow::DataType> item =
        arrow::struct_({arrow::field("name", arrow::utf8()), arrow::field("id", arrow::int32())});
    arrow::FieldVector fields = {arrow::field("items", arrow::list(item))};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          [[{"name":"a","id":1},{"name":null,"id":2}]],
          [[]],
          [null],
          [[{"name":"a-longer-string-that-exceeds-the-inline-view-threshold","id":null}]]
        ])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3});
}

TEST_F(VortexFileFormatTest, ExtractStatisticsReportsRowCount) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "statistics.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32()),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields), R"([[1,"one"],[2,"two"],[3,null],[4,"four"],[5,"five"]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/2));

    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatStatsExtractor> extractor,
                         format_->CreateStatsExtractor(&ffi_schema));
    ASSERT_OK_AND_ASSIGN(auto result, extractor->ExtractWithFileInfo(file_system_, path, pool_));
    ASSERT_EQ(result.second.GetRowCount(), 5);
    // Vortex exposes no per-column statistics; only the row count is verified.
    ASSERT_TRUE(result.first.empty());
}

TEST_F(VortexFileFormatTest, ProjectedReadReturnsOnlyRequestedColumns) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "projection.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("name", arrow::utf8()),
                                 arrow::field("score", arrow::float64())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([[1,"one",1.5],[2,null,2.5],[3,"three",3.5]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, written, /*batch_size=*/2));

    // Read a reordered subset {name, id}; the reader must return exactly those columns, proving it
    // honors SetReadSchema's projection rather than always emitting the full file schema.
    std::shared_ptr<arrow::Schema> projected = arrow::schema({fields[1], fields[0]});
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields[1], fields[0]}),
                                                  R"([["one",1],[null,2],["three",3]])")
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual,
                         ReadFile(path, projected, /*batch_size=*/2, /*expected_row_count=*/3));
    ASSERT_TRUE(actual->Equals(expected)) << actual->ToString() << "\nvs\n" << expected->ToString();
}

// Regression: nested projection must prune sub-fields, not merely relabel the parent type. Reading
// only r.b from a file whose r is ROW<a, b> must yield r as ROW<b> with matching data; before the
// recursive fix the full ROW<a, b> child was kept under a ROW<b> type, so the exported ArrowSchema
// disagreed with the array layout and silently corrupted the output. The b column also exercises
// view normalization inside a nested struct.
TEST_F(VortexFileFormatTest, ProjectedReadPrunesNestedStruct) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "nested-projection.vortex");
    std::shared_ptr<arrow::DataType> r_type =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("r", r_type)};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields),
            R"([[1,{"a":10,"b":"x"}],[2,{"a":20,"b":null}],[3,{"a":30,"b":"z"}]])")
            .ValueOrDie();
    // batch_size=2 also slices the top-level struct column on write (offset > 0), exercising the
    // writer's offset rebasing together with the nested-projection read fix.
    ASSERT_OK(WriteFile(path, schema, written, /*batch_size=*/2));

    // Read only r.b: drop the top-level id column and prune r.a.
    std::shared_ptr<arrow::DataType> r_pruned = arrow::struct_({arrow::field("b", arrow::utf8())});
    arrow::FieldVector projected_fields = {arrow::field("r", r_pruned)};
    std::shared_ptr<arrow::Schema> projected = arrow::schema(projected_fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(projected_fields),
                                                  R"([[{"b":"x"}],[{"b":null}],[{"b":"z"}]])")
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual,
                         ReadFile(path, projected, /*batch_size=*/2, /*expected_row_count=*/3));
    ASSERT_TRUE(actual->Equals(expected)) << actual->ToString() << "\nvs\n" << expected->ToString();
}

// Regression guard for ordered scans: 200 sequential rows span many Vortex chunks, and a small read
// batch size forces many batches. With ScanOptions.ordered(true) the rows must come back in storage
// order, so the concatenated result equals the written 0..199 sequence exactly (physical row
// positions, which the upper layer assigns by batch order, stay aligned for deletion vectors and
// primary-key merge).
TEST_F(VortexFileFormatTest, ReadPreservesRowOrderAcrossBatches) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "row-order.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false)};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::string json = "[";
    for (int32_t i = 0; i < 200; ++i) {
        json += (i > 0 ? "," : "") + std::string("[") + std::to_string(i) + "]";
    }
    json += "]";
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), json).ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/16));
    AssertReadWithBatchSizes(path, schema, expected, {1, 7, 64, 200});
}

// Regression: writing a top-level struct column in slices must not abort. arrow-rs cannot import a
// sliced (offset > 0) top-level struct (the parent offset is re-applied to already-offset children,
// tripping an arrow-data slice assertion), so VortexFormatWriter::AddBatch rebases such a batch to
// offset 0 before handing it to Vortex. batch_size=2 over 4 rows produces a slice at offset 2.
TEST_F(VortexFileFormatTest, WriteThenReadSlicedStructColumn) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "sliced-struct.vortex");
    std::shared_ptr<arrow::DataType> r_type =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("r", r_type)};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields),
            R"([[1,{"a":10,"b":"x"}],[2,{"a":20,"b":null}],[3,{"a":30,"b":"z"}],[4,{"a":40,"b":"w"}]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 4});
}

// The IO callback bridge must surface a paimon IO failure as that paimon error, not Vortex's
// opaque "stream error": the callback can only hand a status code back across the FFI boundary, so
// the reader/writer stashes the real error and prefers it. `IOHook` injects a failure at the Nth
// LocalFile IO; the loop walks the injection point across every IO of the whole read (or write)
// workflow, so each failure position is checked, and stops once the point falls past the last IO
// and the workflow runs clean. `CHECK_HOOK_STATUS` continues the loop when the surfaced error is
// the injected one (proving it propagated), and fails the test on any other error.
TEST_F(VortexFileFormatTest, ReadSurfacesInjectedIOError) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "read-io-error.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([[1,"one"],[2,null],[3,"three"]])")
            .ValueOrDie();
    // Write the fixture with the hook disarmed so only the read path is exercised below.
    ASSERT_OK(WriteFile(path, schema, written, /*batch_size=*/2));

    IOHook* io_hook = IOHook::GetInstance();
    bool run_complete = false;
    for (size_t i = 0; i < 200; i++) {
        ScopeGuard guard([io_hook]() { io_hook->Clear(); });
        io_hook->Reset(static_cast<int64_t>(i), IOHook::Mode::RETURN_ERROR);

        Result<std::unique_ptr<InputStream>> input = file_system_->Open(path);
        CHECK_HOOK_STATUS(input.status(), i);
        Result<std::unique_ptr<ReaderBuilder>> reader_builder =
            format_->CreateReaderBuilder(/*batch_size=*/2);
        CHECK_HOOK_STATUS(reader_builder.status(), i);
        reader_builder.value()->WithMemoryPool(pool_);

        std::shared_ptr<InputStream> input_stream = std::move(input).value();
        Result<std::unique_ptr<FileBatchReader>> reader =
            reader_builder.value()->Build(input_stream);
        CHECK_HOOK_STATUS(reader.status(), i);

        ::ArrowSchema ffi_schema = {};
        ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
        CHECK_HOOK_STATUS(reader.value()->SetReadSchema(&ffi_schema, /*predicate=*/nullptr,
                                                        /*selection_bitmap=*/std::nullopt),
                          i);

        bool eof = false;
        while (!eof) {
            Result<BatchReader::ReadBatch> batch = reader.value()->NextBatch();
            CHECK_HOOK_STATUS(batch.status(), i);
            eof = BatchReader::IsEofBatch(batch.value());
        }
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_F(VortexFileFormatTest, WriteSurfacesInjectedIOError) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "write-io-error.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false)};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([[1],[2],[3]])")
            .ValueOrDie();

    IOHook* io_hook = IOHook::GetInstance();
    bool run_complete = false;
    for (size_t i = 0; i < 200; i++) {
        ScopeGuard guard([io_hook]() { io_hook->Clear(); });
        io_hook->Reset(static_cast<int64_t>(i), IOHook::Mode::RETURN_ERROR);

        ::ArrowSchema ffi_schema = {};
        ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
        Result<std::unique_ptr<WriterBuilder>> writer_builder =
            format_->CreateWriterBuilder(&ffi_schema, /*batch_size=*/2);
        CHECK_HOOK_STATUS(writer_builder.status(), i);
        writer_builder.value()->WithMemoryPool(pool_);

        Result<std::unique_ptr<OutputStream>> output =
            file_system_->Create(path, /*overwrite=*/true);
        CHECK_HOOK_STATUS(output.status(), i);
        std::shared_ptr<OutputStream> output_stream = std::move(output).value();

        Result<std::unique_ptr<FormatWriter>> writer =
            writer_builder.value()->Build(output_stream, "zstd");
        CHECK_HOOK_STATUS(writer.status(), i);

        ::ArrowArray ffi_array = {};
        ASSERT_TRUE(arrow::ExportArray(*data, &ffi_array).ok());
        CHECK_HOOK_STATUS(writer.value()->AddBatch(&ffi_array), i);
        CHECK_HOOK_STATUS(writer.value()->Finish(), i);
        CHECK_HOOK_STATUS(output_stream->Close(), i);
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

}  // namespace paimon::vortex::test

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

#include "paimon/format/orc/orc_file_batch_reader.h"

#include <limits>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/io/cache_input_stream.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/defs.h"
#include "paimon/format/orc/orc_adapter.h"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/format/orc/orc_format_writer.h"
#include "paimon/format/orc/orc_input_stream_impl.h"
#include "paimon/format/orc/orc_memory_pool.h"
#include "paimon/format/orc/orc_metrics.h"
#include "paimon/format/orc/orc_output_stream_impl.h"
#include "paimon/format/orc/orc_reader_builder.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"

namespace paimon::orc::test {

TEST(OrcReaderBuilderTest, RejectsNullMemoryPool) {
    OrcReaderBuilder builder(/*options=*/{}, /*batch_size=*/10);
    builder.WithMemoryPool(nullptr);
    ASSERT_NOK_WITH_MSG(builder.Build(nullptr), "ORC reader memory pool is nullptr");
}

std::string SerializeSchemaToString(const std::shared_ptr<arrow::Schema>& schema) {
    std::shared_ptr<arrow::Buffer> serialized = arrow::ipc::SerializeSchema(*schema).ValueOrDie();
    return std::string(reinterpret_cast<const char*>(serialized->data()),
                       static_cast<size_t>(serialized->size()));
}

struct TestParam {
    uint64_t natural_read_size;
    bool enable_tz;
};

class OrcFileBatchReaderTest : public ::testing::Test,
                               public ::testing::WithParamInterface<TestParam> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        batch_size_ = 10;

        arrow::FieldVector fields = {
            arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};

        struct_array_ = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["Bob", 10, 0, 12.1], ["Emily", 10, 0, 13.1], ["Tony", 10, 0, 14.1], ["Emily", 10, 0, 15.1],
        ["Bob", 10, 0, 12.1], ["Alex", 10, 0, 16.1], ["David", 10, 0, 17.1], ["Lily", 10, 0, 17.1]
    ])")
                .ValueOrDie());
    }
    void TearDown() override {}

    std::shared_ptr<arrow::ChunkedArray> ReadBatchWithCustomizedData(
        const std::shared_ptr<arrow::Array>& src_array, int32_t write_batch_size,
        int32_t write_stripe_size, int32_t write_row_index_stride, const arrow::Schema* read_schema,
        const std::shared_ptr<Predicate>& predicate,
        const std::optional<RoaringBitmap32>& selection_bitmap, int32_t read_batch_size,
        double dict_key_size_threshold, bool enable_lazy_decoding) const {
        arrow::Schema src_schema(src_array->type()->fields());
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<::orc::Type> orc_type,
                             OrcAdapter::GetOrcType(src_schema));
        EXPECT_TRUE(orc_type);
        auto dir = paimon::test::UniqueTestDirectory::Create();
        EXPECT_TRUE(dir);
        auto fs = dir->GetFileSystem();
        std::string data_path = dir->Str() + "/test.data";
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<paimon::OutputStream> output_stream,
                             fs->Create(data_path, /*overwrite=*/true));
        EXPECT_TRUE(output_stream);
        EXPECT_OK_AND_ASSIGN(auto orc_output_stream, OrcOutputStreamImpl::Create(output_stream));
        EXPECT_TRUE(orc_output_stream);
        ::orc::WriterOptions writer_options;
        writer_options.setDictionaryKeySizeThreshold(dict_key_size_threshold);
        if (write_stripe_size != -1) {
            writer_options.setStripeSize(write_stripe_size);
        }
        if (write_row_index_stride != -1) {
            writer_options.setRowIndexStride(write_row_index_stride);
        }
        std::unique_ptr<::orc::Writer> writer =
            ::orc::createWriter(*orc_type, orc_output_stream.get(), writer_options);
        // for simple case, assume that src_array.length() %  write_batch_size == 0
        EXPECT_EQ(src_array->length() % write_batch_size, 0);
        int32_t write_batch_count = src_array->length() / write_batch_size;
        for (int32_t i = 0; i < write_batch_count; i++) {
            auto src_slice = src_array->Slice(i * write_batch_size, write_batch_size);
            auto write_batch = writer->createRowBatch(src_slice->length());
            // Convert from arrow array to orc batch
            EXPECT_OK(OrcAdapter::WriteBatch(src_slice, write_batch.get()));
            writer->add(*write_batch);
        }
        writer->close();
        EXPECT_OK(output_stream->Close());

        EXPECT_OK_AND_ASSIGN(std::shared_ptr<paimon::InputStream> input_stream,
                             fs->Open(data_path));
        EXPECT_TRUE(input_stream);
        EXPECT_OK_AND_ASSIGN(auto orc_input_stream,
                             OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
        EXPECT_TRUE(orc_input_stream);
        std::string enable_lazy_decoding_str = enable_lazy_decoding ? "true" : "false";
        std::map<std::string, std::string> options = {
            {ORC_READ_ENABLE_LAZY_DECODING, enable_lazy_decoding_str}};
        auto orc_batch_reader =
            PrepareOrcFileBatchReader(std::move(orc_input_stream), options, read_schema, predicate,
                                      selection_bitmap, read_batch_size);
        EXPECT_OK_AND_ASSIGN(auto result, paimon::test::ReadResultCollector::CollectResult(
                                              std::move(orc_batch_reader)));
        return result;
    }

    std::unique_ptr<OrcFileBatchReader> PrepareOrcFileBatchReader(
        const std::string& file_name, const arrow::Schema* read_schema, int32_t batch_size,
        uint64_t natural_read_size) const {
        std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system->Open(file_name));
        EXPECT_TRUE(input_stream);
        EXPECT_OK_AND_ASSIGN(auto in_stream,
                             OrcInputStreamImpl::Create(input_stream, natural_read_size));
        EXPECT_TRUE(in_stream);
        std::map<std::string, std::string> options = {{ORC_READ_ENABLE_LAZY_DECODING, "true"},
                                                      {ORC_READ_ENABLE_METRICS, "true"},
                                                      {"orc.timestamp-ltz.legacy.type", "false"}};
        return PrepareOrcFileBatchReader(std::move(in_stream), options, read_schema,
                                         /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt,
                                         batch_size);
    }

    std::unique_ptr<paimon::orc::OrcFileBatchReader> PrepareOrcFileBatchReader(
        std::unique_ptr<::orc::InputStream>&& in_stream,
        const std::map<std::string, std::string>& options, const arrow::Schema* read_schema,
        const std::shared_ptr<Predicate>& predicate,
        const std::optional<RoaringBitmap32>& selection_bitmap, int32_t batch_size,
        const std::shared_ptr<OrcReadMemory>& read_memory = nullptr) const {
        std::shared_ptr<OrcReadMemory> actual_read_memory =
            read_memory ? read_memory : std::make_shared<OrcReadMemory>(pool_);
        EXPECT_OK_AND_ASSIGN(auto orc_batch_reader,
                             OrcFileBatchReader::Create(std::move(in_stream), actual_read_memory,
                                                        options, batch_size));
        EXPECT_TRUE(orc_batch_reader);
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        auto arrow_status = arrow::ExportSchema(*read_schema, c_schema.get());
        EXPECT_TRUE(arrow_status.ok());
        EXPECT_OK(orc_batch_reader->SetReadSchema(c_schema.get(), predicate, selection_bitmap));
        return orc_batch_reader;
    }

    void WriteArray(const std::shared_ptr<FileSystem>& fs, const std::string& file_path,
                    const std::shared_ptr<arrow::Array>& src_array,
                    const std::shared_ptr<arrow::Schema>& arrow_schema,
                    const std::map<std::string, std::string>& options) const {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs->Create(file_path, /*overwrite=*/true));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcOutputStreamImpl> output_stream,
                             OrcOutputStreamImpl::Create(out));
        ASSERT_OK_AND_ASSIGN(
            auto format_writer,
            OrcFormatWriter::Create(std::move(output_stream), *arrow_schema, options, "zstd",
                                    /*batch_size=*/src_array->length(), pool_));
        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*src_array, arrow_array.get()).ok());
        ASSERT_OK(format_writer->AddBatch(arrow_array.get()));
        ASSERT_OK(format_writer->Flush());
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    int32_t batch_size_;
    std::shared_ptr<arrow::StructArray> struct_array_;
};

INSTANTIATE_TEST_SUITE_P(TestParam, OrcFileBatchReaderTest,
                         ::testing::Values(TestParam{128 * 1024, false}, TestParam{16, false},
                                           TestParam{16, true}));

TEST_F(OrcFileBatchReaderTest, BuilderEnablesCacheBeforeReadingMetadata) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    std::string file_path = dir->Str() + "/test.orc";
    auto schema = arrow::schema(struct_array_->type()->fields());
    WriteArray(fs, file_path, struct_array_, schema, /*options=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> input, fs->Open(file_path));
    ASSERT_OK_AND_ASSIGN(int64_t file_size, input->Length());
    uint32_t factory_calls = 0;
    std::shared_ptr<ReadAheadCache> cache;
    auto stream = std::make_shared<CacheInputStream>(
        std::move(input), nullptr, [&]() -> Result<std::shared_ptr<ReadAheadCache>> {
            ++factory_calls;
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> cache_stream, fs->Open(file_path));
            cache = std::make_shared<ReadAheadCache>(cache_stream, CacheConfig(), file_size, pool_);
            return cache;
        });
    OrcReaderBuilder builder(/*options=*/{}, batch_size_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, builder.Build(stream));
    ASSERT_EQ(factory_calls, 1);
    ASSERT_NE(cache, nullptr);
    std::shared_ptr<Metrics> metrics = std::make_shared<MetricsImpl>();
    cache->CollectMetrics(&metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t block_fetches,
                         metrics->GetCounter(ReadAheadCacheMetrics::BLOCK_FETCHES));
    ASSERT_GT(block_fetches, 0);
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
    auto actual_result = arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(actual_result.ok()) << actual_result.status().ToString();
    ASSERT_TRUE(actual_result.ValueOrDie()->Equals(struct_array_));
    reader->Close();
}

class PhysicalProbeStream : public paimon::InputStream {
 public:
    explicit PhysicalProbeStream(std::shared_ptr<paimon::InputStream> inner)
        : inner_(std::move(inner)) {}
    Status Seek(int64_t offset, SeekOrigin origin) override {
        return inner_->Seek(offset, origin);
    }
    Result<int64_t> GetPos() const override {
        return inner_->GetPos();
    }
    Result<int64_t> Length() const override {
        return inner_->Length();
    }
    Result<std::string> GetUri() const override {
        return inner_->GetUri();
    }
    Status Close() override {
        return inner_->Close();
    }
    Result<int64_t> Read(char* data, int64_t size) override {
        return inner_->Read(data, size);
    }
    Result<int64_t> Read(char* data, int64_t size, int64_t offset) override {
        reads.emplace_back(offset, size);
        return inner_->Read(data, size, offset);
    }
    void ReadAsync(char* data, int64_t size, int64_t offset,
                   std::function<void(Status)>&& cb) override {
        reads.emplace_back(offset, size);
        inner_->ReadAsync(data, size, offset, std::move(cb));
    }
    uint64_t Overlap(uint64_t begin, uint64_t length) const {
        uint64_t bytes = 0;
        for (const auto& r : reads) {
            uint64_t lo = std::max(begin, r.first);
            uint64_t hi = std::min(begin + length, r.first + r.second);
            if (hi > lo) {
                bytes += hi - lo;
            }
        }
        return bytes;
    }
    std::vector<std::pair<uint64_t, uint64_t>> reads;

 private:
    std::shared_ptr<paimon::InputStream> inner_;
};

TEST_F(OrcFileBatchReaderTest, SeekPreparesTargetStripeBeforePhysicalReads) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto path = dir->Str() + "/seek.orc";
    {
        auto output = ::orc::writeLocalFile(path);
        auto type = ::orc::Type::buildTypeFromString("struct<a:int>");
        auto writer = ::orc::createWriter(*type, output.get(), ::orc::WriterOptions());
        for (int stripe = 0; stripe < 3; ++stripe) {
            auto batch = writer->createRowBatch(4);
            auto* root = dynamic_cast<::orc::StructVectorBatch*>(batch.get());
            ASSERT_NE(root, nullptr);
            root->numElements = 4;
            auto* values = dynamic_cast<::orc::LongVectorBatch*>(root->fields[0]);
            ASSERT_NE(values, nullptr);
            values->numElements = 4;
            for (int row = 0; row < 4; ++row) {
                values->data[row] = stripe * 4 + row;
            }
            writer->add(*batch);
            writer->writeIntermediateFooter();
        }
        writer->close();
    }
    auto metadata = ::orc::createReader(::orc::readLocalFile(path), ::orc::ReaderOptions());
    ASSERT_EQ(metadata->getNumberOfStripes(), 3);
    auto stripe = metadata->getStripe(2);
    uint64_t payload_offset = stripe->getOffset() + stripe->getIndexLength();
    uint64_t payload_length = stripe->getDataLength();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<paimon::InputStream> raw,
                         dir->GetFileSystem()->Open(path));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<paimon::InputStream> cache_raw,
                         dir->GetFileSystem()->Open(path));
    auto physical_owner = std::make_unique<PhysicalProbeStream>(raw);
    auto* physical = physical_owner.get();
    auto cache_physical = std::make_shared<PhysicalProbeStream>(cache_raw);
    auto pool = GetDefaultPool();
    CacheConfig config;
    config.SetBlockSize(0);
    ASSERT_OK_AND_ASSIGN(int64_t file_size, raw->Length());
    auto cache = std::make_shared<ReadAheadCache>(cache_physical, config, file_size, pool);
    auto context = FileReadAhead::Create(cache, path);
    auto cache_input = std::make_shared<CacheInputStream>(
        std::move(physical_owner), cache, CacheInputStream::CacheFactory(), context);
    ASSERT_OK_AND_ASSIGN(auto orc_input, OrcInputStreamImpl::Create(cache_input, 1));
    ASSERT_OK_AND_ASSIGN(auto reader, OrcFileBatchReader::Create(
                                          std::move(orc_input),
                                          std::make_shared<OrcReadMemory>(pool), {}, 2, context));
    ASSERT_OK(context->Warmup(reader.get()));
    physical->reads.clear();
    cache_physical->reads.clear();
    ASSERT_OK_AND_ASSIGN(uint64_t hits_before,
                         context->GetMetrics()->GetCounter(ReadAheadCacheMetrics::READ_HITS));
    ASSERT_OK(reader->SeekToRow(9));
    uint64_t seek_bytes = physical->Overlap(payload_offset, payload_length);
    uint64_t seek_cache_bytes = cache_physical->Overlap(payload_offset, payload_length);
    ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatch());
    uint64_t next_cache_bytes = cache_physical->Overlap(payload_offset, payload_length);
    ASSERT_EQ(seek_bytes, 0);
    ASSERT_EQ(seek_cache_bytes, payload_length);
    ASSERT_EQ(next_cache_bytes, seek_cache_bytes);
    ASSERT_OK_AND_ASSIGN(uint64_t hits,
                         context->GetMetrics()->GetCounter(ReadAheadCacheMetrics::READ_HITS));
    ASSERT_GT(hits, hits_before);
    ASSERT_EQ(physical->Overlap(payload_offset, payload_length), 0);
    ASSERT_OK_AND_ASSIGN(uint64_t file_row, reader->GetPreviousBatchFileRowId(0));
    ASSERT_EQ(file_row, 9);
    auto imported = arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(imported.ok());
    auto values = std::static_pointer_cast<arrow::Int32Array>(
        std::static_pointer_cast<arrow::StructArray>(imported.ValueOrDie())->field(0));
    ASSERT_EQ(values->Value(0), 9);
    reader->Close();
    context->ReleaseBuffers();
}

TEST_F(OrcFileBatchReaderTest, DiscoverStripeRangesOnDemand) {
    using ByteRanges = std::vector<std::pair<uint64_t, uint64_t>>;
    class CountingStream : public ::orc::InputStream {
     public:
        explicit CountingStream(std::unique_ptr<::orc::InputStream> inner)
            : inner_(std::move(inner)) {}
        uint64_t getLength() const override {
            return inner_->getLength();
        }
        uint64_t getNaturalReadSize() const override {
            return inner_->getNaturalReadSize();
        }
        const std::string& getName() const override {
            return inner_->getName();
        }
        void read(void* data, uint64_t size, uint64_t offset) override {
            reads.emplace_back(offset, size);
            inner_->read(data, size, offset);
        }
        std::vector<std::pair<uint64_t, uint64_t>> reads;

     private:
        std::unique_ptr<::orc::InputStream> inner_;
    };
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string path = dir->Str() + "/stripes.orc";
    {
        auto output = ::orc::writeLocalFile(path);
        auto type = ::orc::Type::buildTypeFromString("struct<a:int,b:int>");
        auto writer = ::orc::createWriter(*type, output.get(), ::orc::WriterOptions());
        for (int64_t stripe = 0; stripe < 4; ++stripe) {
            auto batch = writer->createRowBatch(4);
            auto* root = dynamic_cast<::orc::StructVectorBatch*>(batch.get());
            ASSERT_NE(root, nullptr);
            root->numElements = 4;
            for (uint32_t column = 0; column < 2; ++column) {
                auto* values = dynamic_cast<::orc::LongVectorBatch*>(root->fields[column]);
                ASSERT_NE(values, nullptr);
                values->numElements = 4;
                for (int64_t row = 0; row < 4; ++row) {
                    values->data[row] = stripe * 4 + row;
                }
            }
            writer->add(*batch);
            writer->writeIntermediateFooter();
        }
        writer->close();
    }
    auto metadata = ::orc::createReader(::orc::readLocalFile(path), ::orc::ReaderOptions());
    ASSERT_EQ(metadata->getNumberOfStripes(), 4);
    std::vector<std::pair<uint64_t, uint64_t>> stripe_bounds;
    std::vector<uint64_t> footer_offsets;
    for (uint64_t i = 0; i < 4; ++i) {
        auto stripe = metadata->getStripe(i);
        uint64_t footer = stripe->getOffset() + stripe->getIndexLength() + stripe->getDataLength();
        stripe_bounds.emplace_back(stripe->getOffset(), footer);
        footer_offsets.push_back(footer);
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> main_stream,
                         dir->GetFileSystem()->Open(path));
    ASSERT_OK_AND_ASSIGN(int64_t file_size, main_stream->Length());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> cache_stream,
                         dir->GetFileSystem()->Open(path));
    CacheConfig config;
    config.SetBlockSize(0);
    auto cache = std::make_shared<ReadAheadCache>(cache_stream, config, file_size, pool_);
    auto context = FileReadAhead::Create(cache, path);
    auto cached_stream = std::make_shared<CacheInputStream>(
        std::move(main_stream), cache, CacheInputStream::CacheFactory(), context);
    ASSERT_OK_AND_ASSIGN(auto adapter, OrcInputStreamImpl::Create(cached_stream, 1));
    auto input = std::make_unique<CountingStream>(std::move(adapter));
    auto* observed = input.get();
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<OrcFileBatchReader> reader,
        OrcFileBatchReader::Create(std::move(input), std::make_shared<OrcReadMemory>(pool_), {}, 2,
                                   context));
    observed->reads.clear();
    ASSERT_OK_AND_ASSIGN(ByteRanges initial, reader->PreBufferRange());
    ASSERT_FALSE(initial.empty());
    auto assert_ranges_in_stripe = [&](const auto& ranges, uint64_t stripe) {
        ASSERT_LT(stripe, stripe_bounds.size());
        ASSERT_FALSE(ranges.empty());
        for (const auto& range : ranges) {
            ASSERT_GE(range.first, stripe_bounds[stripe].first);
            ASSERT_LE(range.first + range.second, stripe_bounds[stripe].second);
        }
    };
    auto assert_no_later_footers = [&]() {
        for (const auto& read : observed->reads) {
            for (uint64_t i = 1; i < footer_offsets.size(); ++i) {
                ASSERT_FALSE(read.first <= footer_offsets[i] &&
                             footer_offsets[i] < read.first + read.second);
            }
        }
    };
    assert_ranges_in_stripe(initial, 0);
    assert_no_later_footers();
    ASSERT_OK(context->Warmup(reader.get()));
    assert_no_later_footers();
    ASSERT_OK_AND_ASSIGN(uint64_t prepared_bytes,
                         context->GetMetrics()->GetCounter(ReadAheadCacheMetrics::IO_BYTES));
    ASSERT_GT(prepared_bytes, 0);
    for (int64_t batch_index = 0; batch_index < 8; ++batch_index) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        auto imported = arrow::ImportArray(batch.first.get(), batch.second.get());
        ASSERT_TRUE(imported.ok());
        auto root = std::dynamic_pointer_cast<arrow::StructArray>(imported.ValueOrDie());
        ASSERT_NE(root, nullptr);
        auto values = std::dynamic_pointer_cast<arrow::Int32Array>(root->field(0));
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(values->length(), 2);
        ASSERT_EQ(values->Value(0), batch_index * 2);
        ASSERT_OK_AND_ASSIGN(uint64_t file_row, reader->GetPreviousBatchFileRowId(0));
        ASSERT_EQ(file_row, batch_index * 2);
        ASSERT_OK_AND_ASSIGN(uint64_t bytes,
                             context->GetMetrics()->GetCounter(ReadAheadCacheMetrics::IO_BYTES));
        if (batch_index > 0 && batch_index % 2 == 0) {
            ASSERT_GT(bytes, prepared_bytes);
        } else {
            ASSERT_EQ(bytes, prepared_bytes);
        }
        prepared_bytes = bytes;
        if (batch_index == 0) {
            assert_no_later_footers();
        }
    }
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof, reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof));
    context->EndGeneration();
    ASSERT_OK(reader->SeekToRow(4));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch after_seek, reader->NextBatch());
    ASSERT_OK_AND_ASSIGN(uint64_t file_row, reader->GetPreviousBatchFileRowId(0));
    ASSERT_EQ(file_row, 4);
    after_seek.first->release(after_seek.first.get());
    after_seek.second->release(after_seek.second.get());
    ASSERT_OK_AND_ASSIGN(uint64_t after_end,
                         context->GetMetrics()->GetCounter(ReadAheadCacheMetrics::IO_BYTES));
    ASSERT_EQ(after_end, prepared_bytes);
    context->Reset();
    auto projection = arrow::schema({arrow::field("a", arrow::int32())});
    ArrowSchema projected_schema;
    ASSERT_TRUE(arrow::ExportSchema(*projection, &projected_schema).ok());
    auto predicate = PredicateBuilder::GreaterOrEqual(0, "a", FieldType::INT, Literal(8));
    ASSERT_OK(reader->SetReadSchema(&projected_schema, predicate, std::nullopt));
    observed->reads.clear();
    ASSERT_OK_AND_ASSIGN(ByteRanges projected_ranges, reader->PreBufferRange());
    assert_ranges_in_stripe(projected_ranges, 0);
    assert_no_later_footers();
    ASSERT_OK(context->Warmup(reader.get()));
    uint64_t projected_rows = 0;
    while (true) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto imported = arrow::ImportArray(batch.first.get(), batch.second.get());
        ASSERT_TRUE(imported.ok());
        auto root = std::dynamic_pointer_cast<arrow::StructArray>(imported.ValueOrDie());
        ASSERT_NE(root, nullptr);
        ASSERT_EQ(root->num_fields(), 1);
        auto values = std::dynamic_pointer_cast<arrow::Int32Array>(root->field(0));
        ASSERT_NE(values, nullptr);
        for (int64_t i = 0; i < values->length(); ++i) {
            ASSERT_EQ(values->Value(i), 8 + projected_rows++);
        }
    }
    ASSERT_EQ(projected_rows, 8);
    reader->Close();
    context->ReleaseBuffers();
}

TEST_F(OrcFileBatchReaderTest, TestReadBinaryWrittenFromBinaryAndLargeBinary) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto file_system = dir->GetFileSystem();

    auto check_binary_read_result = [&](const std::shared_ptr<arrow::DataType>& write_type,
                                        const std::string& file_name) {
        std::string data_json = R"([
        ["descriptor-1"],
        [""],
        [null],
        ["descriptor-2"]
    ])";
        auto write_field = arrow::field("f0", write_type);
        auto write_schema = arrow::schema({write_field});
        auto write_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({write_field}), data_json)
                .ValueOrDie());

        std::string file_path = dir->Str() + "/" + file_name;
        WriteArray(file_system, file_path, write_array, write_schema, /*options=*/{});

        auto read_field = arrow::field("f0", arrow::binary());
        arrow::Schema read_schema({read_field});
        auto orc_batch_reader = PrepareOrcFileBatchReader(file_path, &read_schema, batch_size_,
                                                          DEFAULT_NATURAL_READ_SIZE);

        ASSERT_OK_AND_ASSIGN(auto c_file_schema, orc_batch_reader->GetFileSchema());
        auto file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOrDie();
        ASSERT_TRUE(file_schema->Equals(read_schema));

        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_field}), data_json)
                .ValueOrDie());
        auto expected_chunked_array = std::make_shared<arrow::ChunkedArray>(expected_array);
        ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                    std::move(orc_batch_reader)));
        ASSERT_TRUE(result_array->Equals(expected_chunked_array));
    };

    check_binary_read_result(arrow::binary(), "binary.orc");
    check_binary_read_result(arrow::large_binary(), "large-binary.orc");
}

TEST_F(OrcFileBatchReaderTest, TestSetReadSchema) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=10/bucket-1/"
                            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, file_system->Open(file_name));
    ASSERT_OK_AND_ASSIGN(auto in_stream,
                         OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
    std::map<std::string, std::string> options = {{ORC_READ_ENABLE_LAZY_DECODING, "true"}};
    ASSERT_OK_AND_ASSIGN(
        auto orc_batch_reader,
        OrcFileBatchReader::Create(std::move(in_stream), std::make_shared<OrcReadMemory>(pool_),
                                   options, batch_size_));
    // test GetFileSchema()
    ASSERT_OK_AND_ASSIGN(auto c_file_schema, orc_batch_reader->GetFileSchema());
    auto arrow_file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOrDie();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    arrow::Schema file_schema(fields);
    ASSERT_TRUE(arrow_file_schema->Equals(file_schema));
    // NextBatch() without SetReadSchema(), will return data with
    // file schema
    ASSERT_OK_AND_ASSIGN(auto result_with_file_schema,
                         paimon::test::ReadResultCollector::CollectResult(orc_batch_reader.get()));
    auto expected_file_chunk_array = std::make_shared<arrow::ChunkedArray>(struct_array_);
    ASSERT_TRUE(result_with_file_schema->Equals(expected_file_chunk_array));
    // NextBatch() with SetReadSchema(), will return data with read schema
    arrow::Schema read_schema({fields[0], fields[2]});
    std::unique_ptr<ArrowSchema> c_read_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(read_schema, c_read_schema.get()).ok());
    ASSERT_OK(orc_batch_reader->SetReadSchema(c_read_schema.get(),
                                              /*predicate=*/nullptr,
                                              /*selection_bitmap=*/std::nullopt));
    auto expected_read_array =
        arrow::StructArray::Make({struct_array_->field(0), struct_array_->field(2)},
                                 {fields[0], fields[2]})
            .ValueOrDie();
    auto expected_read_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_read_array);
    ASSERT_OK_AND_ASSIGN(auto result_with_read_schema,
                         paimon::test::ReadResultCollector::CollectResult(orc_batch_reader.get()));
    ASSERT_TRUE(result_with_read_schema->Equals(expected_read_chunk_array));

    // NextBatch() with predicate
    auto predicate = PredicateBuilder::GreaterThan(
        /*field_index=*/3, /*field_name=*/"f2", FieldType::INT, Literal(2));
    c_read_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(read_schema, c_read_schema.get()).ok());
    ASSERT_OK(orc_batch_reader->SetReadSchema(c_read_schema.get(), predicate,
                                              /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(result_with_read_schema, paimon::test::ReadResultCollector::CollectResult(
                                                      std::move(orc_batch_reader)));
    ASSERT_FALSE(result_with_read_schema);
}

TEST_F(OrcFileBatchReaderTest, TestCreateRowReaderOptions) {
    auto orc_pool = std::make_shared<OrcMemoryPool>(pool_);
    std::vector<uint64_t> target_column_ids;
    {
        // read all fields && default options
        std::map<std::string, std::string> options;
        std::string orc_schema = "struct<col1:int,col2:double,col3:string>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(orc_schema);
        std::unique_ptr<::orc::Type> target_type = ::orc::Type::buildTypeFromString(orc_schema);
        target_column_ids.clear();
        ASSERT_OK_AND_ASSIGN(auto row_reader_option,
                             OrcFileBatchReader::CreateRowReaderOptions(
                                 src_type.get(), target_type.get(),
                                 /*search_arg=*/nullptr, options, &target_column_ids));
        // col1(1), col2(2), col3(3) — struct container(0) is not included
        ASSERT_EQ(target_column_ids, (std::vector<uint64_t>{1, 2, 3}));
        ASSERT_EQ(row_reader_option.getEnableLazyDecoding(), false);
    }
    {
        // read partial fields && set options
        std::map<std::string, std::string> options = {{ORC_READ_ENABLE_LAZY_DECODING, "true"}};
        std::string src_orc_schema = "struct<col1:int,col2:double,col3:string>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema = "struct<col1:int,col3:string>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        target_column_ids.clear();
        ASSERT_OK_AND_ASSIGN(auto row_reader_option,
                             OrcFileBatchReader::CreateRowReaderOptions(
                                 src_type.get(), target_type.get(),
                                 /*search_arg=*/nullptr, options, &target_column_ids));
        // col1(1), col3(3) — struct container(0) not included, col2(2) skipped
        ASSERT_EQ(target_column_ids, (std::vector<uint64_t>{1, 3}));
        ASSERT_EQ(row_reader_option.getEnableLazyDecoding(), true);
    }
    {
        // read non exist column
        std::map<std::string, std::string> options;
        std::string src_orc_schema = "struct<col1:int,col2:double,col3:string>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema = "struct<col1:int,non_exist_col:double>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        target_column_ids.clear();
        ASSERT_NOK_WITH_MSG(OrcFileBatchReader::CreateRowReaderOptions(
                                src_type.get(), target_type.get(),
                                /*search_arg=*/nullptr, options, &target_column_ids),
                            "field non_exist_col not in file schema");
    }
    {
        // read partial top-level fields with nested type (all sub-fields)
        std::map<std::string, std::string> options;
        std::string src_orc_schema =
            "struct<col1:struct<sub1:int,sub2:array<array<double>>,sub3:int>,col2:double,col3:"
            "map<string,int>>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema =
            "struct<col1:struct<sub1:int,sub2:array<array<double>>,sub3:int>,col3:map<string,int>>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        target_column_ids.clear();
        ASSERT_OK_AND_ASSIGN(auto row_reader_option,
                             OrcFileBatchReader::CreateRowReaderOptions(
                                 src_type.get(), target_type.get(),
                                 /*search_arg=*/nullptr, options, &target_column_ids));
        // Struct IDs (0, 1) are not included. LIST/MAP containers include their complete
        // subtrees: sub1(2), sub2(3, 4, 5), sub3(6), col3(8, 9, 10).
        ASSERT_EQ(target_column_ids, (std::vector<uint64_t>{2, 3, 4, 5, 6, 8, 9, 10}));
    }
    {
        // read with type mismatch in nested field
        std::map<std::string, std::string> options;
        std::string src_orc_schema =
            "struct<col1:struct<sub1:int,sub2:int,sub3:int>,col2:double,col3:string>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema =
            "struct<col1:struct<sub1:int,sub2:double,sub3:int>,col2:double,col3:string>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        target_column_ids.clear();
        ASSERT_NOK_WITH_MSG(OrcFileBatchReader::CreateRowReaderOptions(
                                src_type.get(), target_type.get(),
                                /*search_arg=*/nullptr, options, &target_column_ids),
                            "type kind mismatch");
    }
    {
        // read partial sub-fields of nested struct (nested field projection)
        std::map<std::string, std::string> options;
        std::string src_orc_schema =
            "struct<col1:struct<sub1:int,sub2:double,sub3:string>,col2:int>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        // only read sub1 and sub3 from col1
        std::string target_orc_schema = "struct<col1:struct<sub1:int,sub3:string>>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        target_column_ids.clear();
        ASSERT_OK_AND_ASSIGN(auto row_reader_option,
                             OrcFileBatchReader::CreateRowReaderOptions(
                                 src_type.get(), target_type.get(),
                                 /*search_arg=*/nullptr, options, &target_column_ids));
        // src type IDs: struct(0), col1(1){sub1(2), sub2(3), sub3(4)}, col2(5)
        // Struct IDs (0, 1) not included. Selected: sub1(2), sub3(4)
        ASSERT_EQ(target_column_ids, (std::vector<uint64_t>{2, 4}));
    }
    {
        // nested struct sub-fields out-of-order should fail
        std::map<std::string, std::string> options;
        std::string src_orc_schema =
            "struct<col1:struct<sub1:int,sub2:double,sub3:string>,col2:int>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema = "struct<col1:struct<sub3:string,sub1:int>>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        target_column_ids.clear();
        ASSERT_NOK_WITH_MSG(
            OrcFileBatchReader::CreateRowReaderOptions(src_type.get(), target_type.get(),
                                                       /*search_arg=*/nullptr, options,
                                                       &target_column_ids),
            "The column id of the target field should be monotonically increasing in format "
            "reader");
    }
    {
        // top-level order correct but nested sub-fields out-of-order should fail
        std::map<std::string, std::string> options;
        std::string src_orc_schema =
            "struct<col1:struct<sub1:int,sub2:double,sub3:string>,col2:int>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        // col1 before col2 (correct top-level order), but sub2 before sub1 (wrong nested order)
        std::string target_orc_schema = "struct<col1:struct<sub2:double,sub1:int>,col2:int>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        target_column_ids.clear();
        ASSERT_NOK_WITH_MSG(
            OrcFileBatchReader::CreateRowReaderOptions(src_type.get(), target_type.get(),
                                                       /*search_arg=*/nullptr, options,
                                                       &target_column_ids),
            "The column id of the target field should be monotonically increasing in format "
            "reader");
    }
    {
        // decimal precision mismatch
        std::map<std::string, std::string> options;
        std::string src_orc_schema = "struct<col1:decimal(18,2),col2:int>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema = "struct<col1:decimal(20,2),col2:int>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        target_column_ids.clear();
        ASSERT_NOK_WITH_MSG(OrcFileBatchReader::CreateRowReaderOptions(
                                src_type.get(), target_type.get(),
                                /*search_arg=*/nullptr, options, &target_column_ids),
                            "type mismatch");
    }
    {
        std::map<std::string, std::string> options;
        std::string src_orc_schema = "struct<col1:array<struct<a:int,b:double>>>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema = "struct<col1:array<struct<a:int>>>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        target_column_ids.clear();
        // list/map sub-field partial projection is not supported; toString() mismatch is reported
        ASSERT_NOK_WITH_MSG(OrcFileBatchReader::CreateRowReaderOptions(
                                src_type.get(), target_type.get(),
                                /*search_arg=*/nullptr, options, &target_column_ids),
                            "type mismatch");
    }
    {
        std::map<std::string, std::string> options;
        std::string src_orc_schema = "struct<col1:map<string,struct<a:int,b:double>>>";
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(src_orc_schema);
        std::string target_orc_schema = "struct<col1:map<string,struct<a:int>>>";
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString(target_orc_schema);
        target_column_ids.clear();
        // list/map sub-field partial projection is not supported; toString() mismatch is reported
        ASSERT_NOK_WITH_MSG(OrcFileBatchReader::CreateRowReaderOptions(
                                src_type.get(), target_type.get(),
                                /*search_arg=*/nullptr, options, &target_column_ids),
                            "type mismatch");
    }
}

TEST_F(OrcFileBatchReaderTest, TestCollectTargetColumnIdsPrimitiveList) {
    std::unique_ptr<::orc::Type> src_type =
        ::orc::Type::buildTypeFromString("struct<items:array<int>,ignored:string>");
    std::unique_ptr<::orc::Type> target_type =
        ::orc::Type::buildTypeFromString("struct<items:array<int>>");
    std::vector<uint64_t> target_column_ids;

    ASSERT_OK(OrcFileBatchReader::CollectTargetColumnIds(src_type.get(), target_type.get(),
                                                         &target_column_ids));
    // root struct(0), items-list(1), element(2), ignored(3)
    ASSERT_EQ(target_column_ids, (std::vector<uint64_t>{1, 2}));
}

TEST_F(OrcFileBatchReaderTest, TestCollectTargetColumnIdsDeeplyNestedList) {
    std::string schema = "struct<items:array<array<struct<value:double,label:string>>>>";
    std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(schema);
    std::unique_ptr<::orc::Type> target_type = ::orc::Type::buildTypeFromString(schema);
    std::vector<uint64_t> target_column_ids;

    ASSERT_OK(OrcFileBatchReader::CollectTargetColumnIds(src_type.get(), target_type.get(),
                                                         &target_column_ids));
    // root struct(0), outer list(1), inner list(2), element struct(3), value(4), label(5)
    ASSERT_EQ(target_column_ids, (std::vector<uint64_t>{1, 2, 3, 4, 5}));
}

TEST_F(OrcFileBatchReaderTest, TestCollectTargetColumnIdsPrimitiveMap) {
    std::unique_ptr<::orc::Type> src_type =
        ::orc::Type::buildTypeFromString("struct<attributes:map<string,int>,ignored:double>");
    std::unique_ptr<::orc::Type> target_type =
        ::orc::Type::buildTypeFromString("struct<attributes:map<string,int>>");
    std::vector<uint64_t> target_column_ids;

    ASSERT_OK(OrcFileBatchReader::CollectTargetColumnIds(src_type.get(), target_type.get(),
                                                         &target_column_ids));
    // root struct(0), attributes-map(1), key(2), value(3), ignored(4)
    ASSERT_EQ(target_column_ids, (std::vector<uint64_t>{1, 2, 3}));
}

TEST_F(OrcFileBatchReaderTest, TestCollectTargetColumnIdsCompatibleStringKinds) {
    std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(
        "struct<char_value:char(3),varchar_value:varchar(64),"
        "nested:array<map<char(4),varchar(16)>>>");
    std::unique_ptr<::orc::Type> target_type = ::orc::Type::buildTypeFromString(
        "struct<char_value:string,varchar_value:string,nested:array<map<string,string>>>");
    std::vector<uint64_t> target_column_ids;

    ASSERT_OK(OrcFileBatchReader::CollectTargetColumnIds(src_type.get(), target_type.get(),
                                                         &target_column_ids));
    // char_value(1), varchar_value(2), nested-list(3), map(4), key(5), value(6)
    ASSERT_EQ(target_column_ids, (std::vector<uint64_t>{1, 2, 3, 4, 5, 6}));
}

TEST_F(OrcFileBatchReaderTest, TestCollectTargetColumnIdsDeeplyNestedMap) {
    std::string schema =
        "struct<attributes:map<string,array<struct<score:double,tags:array<string>>>>>";
    std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(schema);
    std::unique_ptr<::orc::Type> target_type = ::orc::Type::buildTypeFromString(schema);
    std::vector<uint64_t> target_column_ids;

    ASSERT_OK(OrcFileBatchReader::CollectTargetColumnIds(src_type.get(), target_type.get(),
                                                         &target_column_ids));
    // root struct(0), map(1), key(2), value-list(3), element struct(4), score(5),
    // tags-list(6), tag element(7)
    ASSERT_EQ(target_column_ids, (std::vector<uint64_t>{1, 2, 3, 4, 5, 6, 7}));
}

TEST_F(OrcFileBatchReaderTest, TestCollectTargetColumnIdsStructProjectionWithListAndMap) {
    std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(
        "struct<outer:struct<items:array<int>,plain:double,attributes:map<string,int>>,"
        "ignored:string>");
    std::unique_ptr<::orc::Type> target_type = ::orc::Type::buildTypeFromString(
        "struct<outer:struct<items:array<int>,attributes:map<string,int>>>");
    std::vector<uint64_t> target_column_ids;

    ASSERT_OK(OrcFileBatchReader::CollectTargetColumnIds(src_type.get(), target_type.get(),
                                                         &target_column_ids));
    // root struct(0) and outer struct(1) are not included. Selected: items(2, 3) and
    // attributes(5, 6, 7). plain(4) and ignored(8) are skipped.
    ASSERT_EQ(target_column_ids, (std::vector<uint64_t>{2, 3, 5, 6, 7}));
}

TEST_F(OrcFileBatchReaderTest, TestCollectTargetColumnIdsRejectsPartialListAndMapProjection) {
    {
        std::unique_ptr<::orc::Type> src_type =
            ::orc::Type::buildTypeFromString("struct<items:array<struct<a:int,b:double>>>");
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString("struct<items:array<struct<a:int>>>");
        std::vector<uint64_t> target_column_ids;
        ASSERT_NOK_WITH_MSG(OrcFileBatchReader::CollectTargetColumnIds(
                                src_type.get(), target_type.get(), &target_column_ids),
                            "type mismatch");
        ASSERT_TRUE(target_column_ids.empty());
    }
    {
        std::unique_ptr<::orc::Type> src_type = ::orc::Type::buildTypeFromString(
            "struct<attributes:map<string,struct<a:int,b:double>>>");
        std::unique_ptr<::orc::Type> target_type =
            ::orc::Type::buildTypeFromString("struct<attributes:map<string,struct<a:int>>>");
        std::vector<uint64_t> target_column_ids;
        ASSERT_NOK_WITH_MSG(OrcFileBatchReader::CollectTargetColumnIds(
                                src_type.get(), target_type.get(), &target_column_ids),
                            "type mismatch");
        ASSERT_TRUE(target_column_ids.empty());
    }
}

TEST_P(OrcFileBatchReaderTest, TestNextBatchSimple) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=10/bucket-1/"
                            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
    auto data_type = struct_array_->struct_type();
    arrow::Schema read_schema(data_type->fields());
    auto [natural_read_size, _] = GetParam();
    for (auto batch_size : {1, 2, 3, 5, 8, 10}) {
        auto orc_batch_reader =
            PrepareOrcFileBatchReader(file_name, &read_schema, batch_size, natural_read_size);
        ASSERT_NOK(orc_batch_reader->GetPreviousBatchFileRowId(0));
        int i = 0;
        while (true) {
            ASSERT_OK_AND_ASSIGN(
                auto result_array,
                paimon::test::ReadResultCollector::CollectResultOneBatch(orc_batch_reader.get()));
            if (!result_array) {
                ASSERT_NOK(orc_batch_reader->GetPreviousBatchFileRowId(0));
                break;
            }
            ASSERT_EQ(orc_batch_reader->GetPreviousBatchFileRowId(0).value(), i * batch_size);
            ASSERT_TRUE(result_array->Equals(std::make_shared<arrow::ChunkedArray>(
                struct_array_->Slice(i * batch_size, result_array->length()))));
            i++;
        }
        orc_batch_reader->Close();
        // test metrics
        auto reader_metrics = orc_batch_reader->GetReaderMetrics();
        ASSERT_OK_AND_ASSIGN(uint64_t io_count,
                             reader_metrics->GetCounter(OrcMetrics::READ_IO_COUNT));
        ASSERT_GT(io_count, 0);
        ASSERT_OK_AND_ASSIGN(uint64_t latency,
                             reader_metrics->GetCounter(OrcMetrics::READ_INCLUSIVE_LATENCY_US));
        ASSERT_GT(latency, 0);
    }
}

TEST_F(OrcFileBatchReaderTest, TestBatchRetainsOrcReadMemory) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<FileSystem> file_system = dir->GetFileSystem();
    std::string file_name = dir->Str() + "/dictionary.orc";
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    std::shared_ptr<arrow::StructArray> expected = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
                ["abc"], ["abc"], ["abc"], ["de"], ["de"], ["de"]
            ])")
            .ValueOrDie());
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    WriteArray(file_system, file_name, expected, schema,
               {{ORC_DICTIONARY_KEY_SIZE_THRESHOLD, "0.9"}});

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, file_system->Open(file_name));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrcInputStreamImpl> in_stream,
                         OrcInputStreamImpl::Create(input_stream, /*natural_read_size=*/16));

    std::shared_ptr<OrcReadMemory> read_memory = std::make_shared<OrcReadMemory>(pool_);
    std::weak_ptr<OrcReadMemory> weak_read_memory = read_memory;
    std::weak_ptr<arrow::MemoryPool> weak_arrow_pool = read_memory->arrow_pool;
    std::weak_ptr<::orc::MemoryPool> weak_orc_pool = read_memory->orc_pool;
    std::map<std::string, std::string> options = {{ORC_READ_ENABLE_LAZY_DECODING, "true"}};
    std::unique_ptr<OrcFileBatchReader> reader = PrepareOrcFileBatchReader(
        std::move(in_stream), options, schema.get(), /*predicate=*/nullptr,
        /*selection_bitmap=*/std::nullopt, /*batch_size=*/6, read_memory);
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());

    reader.reset();
    read_memory.reset();
    ASSERT_FALSE(weak_read_memory.expired());
    ASSERT_FALSE(weak_arrow_pool.expired());
    ASSERT_FALSE(weak_orc_pool.expired());

    arrow::Result<std::shared_ptr<arrow::Array>> import_result =
        arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
    std::shared_ptr<arrow::Array> array = std::move(import_result).ValueOrDie();
    ASSERT_FALSE(weak_read_memory.expired());
    ASSERT_FALSE(weak_arrow_pool.expired());
    ASSERT_FALSE(weak_orc_pool.expired());
    ASSERT_TRUE(array->ValidateFull().ok());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> converted,
        paimon::test::DictArrayConverter::ConvertDictArray(array, arrow::default_memory_pool()));
    array.reset();
    ASSERT_TRUE(converted->Equals(expected));
    converted.reset();
    ASSERT_TRUE(weak_read_memory.expired());
    ASSERT_TRUE(weak_arrow_pool.expired());
    ASSERT_TRUE(weak_orc_pool.expired());
}

TEST_P(OrcFileBatchReaderTest, TestNextBatchWithTargetSchema) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=10/bucket-1/"
                            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
    // read without f2
    auto data_type = struct_array_->struct_type();
    arrow::FieldVector fields = {data_type->GetFieldByName("f0"), data_type->GetFieldByName("f1"),
                                 data_type->GetFieldByName("f3")};
    auto [natural_read_size, _] = GetParam();
    arrow::Schema read_schema(fields);

    auto orc_batch_reader =
        PrepareOrcFileBatchReader(file_name, &read_schema, batch_size_, natural_read_size);
    ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                std::move(orc_batch_reader)));
    auto expected_array = std::make_shared<arrow::ChunkedArray>(
        arrow::StructArray::Make(
            {struct_array_->GetFieldByName("f0"), struct_array_->GetFieldByName("f1"),
             struct_array_->GetFieldByName("f3")},
            fields)
            .ValueOrDie());
    ASSERT_TRUE(result_array->Equals(expected_array));
}

TEST_F(OrcFileBatchReaderTest, TestNextBatchWithOutofOrderTargetSchema) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=10/bucket-1/"
                            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
    // file type is f0, f1, f2, f3
    // read with f3, f1, f0
    auto data_type = struct_array_->struct_type();
    arrow::FieldVector fields = {data_type->GetFieldByName("f3"), data_type->GetFieldByName("f1"),
                                 data_type->GetFieldByName("f0")};
    arrow::Schema read_schema(fields);
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, file_system->Open(file_name));
    ASSERT_OK_AND_ASSIGN(auto in_stream,
                         OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
    std::map<std::string, std::string> options = {{ORC_READ_ENABLE_LAZY_DECODING, "true"}};
    ASSERT_OK_AND_ASSIGN(
        auto orc_batch_reader,
        OrcFileBatchReader::Create(std::move(in_stream), std::make_shared<OrcReadMemory>(pool_),
                                   options, batch_size_));
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(read_schema, c_schema.get()).ok());
    ASSERT_NOK_WITH_MSG(orc_batch_reader->SetReadSchema(c_schema.get(), /*predicate=*/nullptr,
                                                        /*selection_bitmap=*/std::nullopt),
                        "The column id of the target field should be "
                        "monotonically increasing in "
                        "format reader");
}

TEST_P(OrcFileBatchReaderTest, TestNextBatchWithNullValue) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=20/bucket-0/"
                            "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc";
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto arrow_data_type = arrow::struct_(fields);
    arrow::Schema read_schema(fields);
    auto [natural_read_size, _] = GetParam();
    auto orc_batch_reader =
        PrepareOrcFileBatchReader(file_name, &read_schema, batch_size_, natural_read_size);
    ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                std::move(orc_batch_reader)));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    std::string json = R"([
      ["Paul", 20, 1, null]
    ])";
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {json}, &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(expected_array));
}

TEST_F(OrcFileBatchReaderTest, TestNextBatchWithDictionary) {
    auto f0 = arrow::field("f0", arrow::list(arrow::utf8()));
    auto f1 = arrow::field("f1", arrow::map(arrow::utf8(), arrow::binary()));
    auto f2 = arrow::field(
        "f2", arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::binary()),
                              field("sub3", arrow::utf8())}));

    arrow::FieldVector fields = {f0, f1, f2};
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [["a", "a", "b"], [["a", "q"], ["b", "w"]],             [10, "q", "a"]],
        [["a", "c"],      [["a", "e"], ["b", "r"], ["c", "e"]], [20, "w", "a"]],
        [["a", "d"],      [["d", "r"], ["e", "t"]],             [null, "e", "b"]],
        [["a"],           [["a", "q"]],                         [null, "w", "c"]],
        [null,            [["a", "w"], ["f", "y"]],             [50, "r", null]],
        [["a"],           null,                                 [60, "r", "b"]],
        [["a", "b", "e"], [["a", null], ["b", "w"]],            null]
    ])")
            .ValueOrDie());

    arrow::FieldVector read_fields = {f0, f1, f2};
    auto read_schema = arrow::schema(read_fields);
    auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(src_array);
    auto check_result = [&](double dict_key_size_threshold, bool enable_lazy_decoding) {
        auto target_array = ReadBatchWithCustomizedData(
            src_array, /*write_batch_size=*/src_array->length(), /*write_stripe_size=*/-1,
            /*write_row_index_stride=*/-1, read_schema.get(), /*predicate=*/nullptr,
            /*selection_bitmap=*/std::nullopt, 10, dict_key_size_threshold, enable_lazy_decoding);
        ASSERT_TRUE(target_array->Equals(expected_chunk_array));
        ASSERT_TRUE(expected_chunk_array->Equals(target_array));
    };
    // touch all conditions
    check_result(0.9, true);
    check_result(0.1, true);
    check_result(0.9, false);
    check_result(0.1, false);
}

TEST_P(OrcFileBatchReaderTest, TestComplexType) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_complex_data.db/append_complex_data/f1=10/bucket-0/"
                            "data-14a30421-7650-486c-9876-66a1fa4356ff-0.orc";
    arrow::FieldVector fields = {arrow::field("f1", arrow::int32()),
                                 arrow::field("f2", arrow::int32()),
                                 arrow::field("f3", arrow::date32()),
                                 arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
                                 arrow::field("f5", arrow::decimal128(23, 5)),
                                 arrow::field("f6", arrow::binary())};
    auto arrow_data_type = arrow::struct_(fields);
    arrow::Schema read_schema(fields);
    auto [natural_read_size, _] = GetParam();
    auto orc_batch_reader =
        PrepareOrcFileBatchReader(file_name, &read_schema, batch_size_, natural_read_size);
    ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                std::move(orc_batch_reader)));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow_data_type, {R"([
        [10, 1, 1234,  "2033-05-18 03:33:20.0",         "123456789987654321.45678", "add"],
        [10, 1, 19909, "2033-05-18 03:33:20.000001001", "12.30000", "cat"],
        [10, 1, 0,     "2008-12-28 00:00:00.000123456", null, "dad"],
        [10, 1, 100,   "2008-12-28 00:00:00.00012345",  "-123.45000", "eat"],
        [10, 1, null,  "1899-01-01 00:59:20.001001001", "0.00000", "fat"],
        [10, 1, 20006, "2024-10-10 10:10:10.1001001",   "1728551410100.10010", null]
    ])"},
                                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(result_array->Equals(*expected_array));
}

TEST_F(OrcFileBatchReaderTest, TestGetFileSchemaWithFieldId) {
    auto get_file_schema = [&](const std::string& file_name) -> std::shared_ptr<arrow::Schema> {
        std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system->Open(file_name));
        EXPECT_TRUE(input_stream);
        EXPECT_OK_AND_ASSIGN(auto in_stream,
                             OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
        EXPECT_TRUE(in_stream);
        std::map<std::string, std::string> options = {{ORC_READ_ENABLE_LAZY_DECODING, "true"}};
        EXPECT_OK_AND_ASSIGN(
            auto orc_batch_reader,
            OrcFileBatchReader::Create(std::move(in_stream), std::make_shared<OrcReadMemory>(pool_),
                                       options, batch_size_));
        EXPECT_TRUE(orc_batch_reader);
        auto c_file_schema = orc_batch_reader->GetFileSchema();
        EXPECT_TRUE(c_file_schema.ok());
        auto arrow_file_schema = arrow::ImportSchema(c_file_schema.value().get());
        EXPECT_TRUE(arrow_file_schema.ok());
        return arrow_file_schema.ValueOrDie();
    };
    {
        // test file without field id
        std::string file_name = paimon::test::GetDataDir() +
                                "/orc/append_09.db/append_09/f1=10/bucket-1/"
                                "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
        auto file_schema = get_file_schema(file_name);
        ASSERT_NOK(DataField::ConvertArrowSchemaToDataFields(file_schema));
    }
    {
        // test file with field id, simple schema
        std::string file_name = paimon::test::GetDataDir() +
                                "/orc/append_table_with_alter_table_build_in_fieldid.db/"
                                "append_table_with_alter_table_build_in_fieldid/key0=0/"
                                "key1=1/bucket-0/"
                                "data-35e7027e-b12a-4ebf-ae15-4c0fe8d6a895-0.orc";
        auto file_schema = get_file_schema(file_name);
        ASSERT_OK_AND_ASSIGN(auto data_fields,
                             DataField::ConvertArrowSchemaToDataFields(file_schema));
        std::vector<DataField> expected_data_fields = {
            DataField(0, arrow::field("key0", arrow::int32())),
            DataField(1, arrow::field("key1", arrow::int32())),
            DataField(2, arrow::field("a", arrow::int32())),
            DataField(3, arrow::field("b", arrow::int32())),
            DataField(4, arrow::field("c", arrow::int32())),
            DataField(5, arrow::field("d", arrow::int32())),
            DataField(6, arrow::field("k", arrow::int32()))};
        ASSERT_EQ(data_fields, expected_data_fields);
    }
    {
        // test file with field id, complex schema
        std::string file_name = paimon::test::GetDataDir() +
                                "/orc/append_complex_build_in_fieldid.db/"
                                "append_complex_build_in_fieldid/bucket-0/"
                                "data-6dac9052-36d8-4950-8f74-b2bbc082e489-0.orc";
        auto file_schema = get_file_schema(file_name);
        ASSERT_OK_AND_ASSIGN(auto data_fields,
                             DataField::ConvertArrowSchemaToDataFields(file_schema));
        // map type will lose meta info in c <-> c++ bridge (see
        // TestBridgeForMapType in OrcAdapterTest
        auto map_type = arrow::map(arrow::int8(), arrow::int16());
        auto list_type = arrow::list(DataField::ConvertDataFieldToArrowField(
            DataField(536871936, arrow::field("item", arrow::float32()))));
        std::vector<DataField> struct_fields = {DataField(3, arrow::field("f0", arrow::boolean())),
                                                DataField(4, arrow::field("f1", arrow::int64()))};
        auto struct_type = DataField::ConvertDataFieldsToArrowStructType(struct_fields);
        std::vector<DataField> expected_data_fields = {
            DataField(0, arrow::field("f1", map_type)),
            DataField(1, arrow::field("f2", list_type)),
            DataField(2, arrow::field("f3", struct_type)),
            DataField(5, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO))),
            DataField(6, arrow::field("f5", arrow::date32())),
            DataField(7, arrow::field("f6", arrow::decimal128(2, 2)))};
        ASSERT_EQ(data_fields, expected_data_fields);
    }
}

TEST_F(OrcFileBatchReaderTest, TestDictionaryWithMultiStripe) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto arrow_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_type, R"([
        ["abc"],
        ["abc"],
        ["abc"],
        ["de"],
        ["de"],
        ["de"]
    ])")
            .ValueOrDie());
    auto src_schema = arrow::schema(fields);
    // force generate two stripe in one file
    auto target_array = ReadBatchWithCustomizedData(
        src_array, /*write_batch_size=*/3, /*write_stripe_size=*/3,
        /*write_row_index_stride=*/3, /*read_schema=*/src_schema.get(), /*predicate=*/nullptr,
        /*selection_bitmap=*/std::nullopt, /*read_batch_size=*/10,
        /*dict_key_size_threshold=*/0.9,
        /*enable_lazy_decoding=*/true);
    auto expected_array = arrow::ChunkedArray::Make({src_array}).ValueOrDie();
    ASSERT_TRUE(target_array->Equals(expected_array));
    ASSERT_TRUE(expected_array->Equals(target_array));
}

TEST_F(OrcFileBatchReaderTest, TestReadNoField) {
    // if only read partition fields, format reader will set empty read schema
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/f1=10/bucket-1/"
                            "data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc";
    // read no field
    arrow::Schema read_schema({});
    auto orc_batch_reader = PrepareOrcFileBatchReader(file_name, &read_schema, /*batch_size=*/3,
                                                      /*natural_read_size=*/10);
    // read 3 rows
    ASSERT_NOK(orc_batch_reader->GetPreviousBatchFileRowId(0));
    ASSERT_OK_AND_ASSIGN(auto batch1, orc_batch_reader->NextBatch());
    ASSERT_EQ(orc_batch_reader->GetPreviousBatchFileRowId(0).value(), 0);
    // read 3 rows
    ASSERT_OK_AND_ASSIGN(auto batch2, orc_batch_reader->NextBatch());
    ASSERT_EQ(orc_batch_reader->GetPreviousBatchFileRowId(0).value(), 3);
    // read 2 rows
    ASSERT_OK_AND_ASSIGN(auto batch3, orc_batch_reader->NextBatch());
    ASSERT_EQ(orc_batch_reader->GetPreviousBatchFileRowId(0).value(), 6);
    // read rows with eof
    ASSERT_OK_AND_ASSIGN(auto batch4, orc_batch_reader->NextBatch());
    ASSERT_NOK(orc_batch_reader->GetPreviousBatchFileRowId(0));
    ASSERT_TRUE(BatchReader::IsEofBatch(batch4));
    orc_batch_reader->Close();
    orc_batch_reader.reset();

    arrow::FieldVector fields;
    auto arrow_type = arrow::struct_(fields);
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_type, R"([
[],
[],
[]
    ])")
            .ValueOrDie());

    auto check_batch = [](BatchReader::ReadBatch&& result_batch,
                          const std::shared_ptr<arrow::Array>& expected_array) {
        auto& [c_array, c_schema] = result_batch;
        auto result_array = arrow::ImportArray(c_array.get(), c_schema.get()).ValueOr(nullptr);
        ASSERT_TRUE(result_array);
        ASSERT_TRUE(result_array->Equals(expected_array));
    };

    check_batch(std::move(batch1), expected_array);
    check_batch(std::move(batch2), expected_array);
    check_batch(std::move(batch3), expected_array->Slice(0, 2));
}

TEST_P(OrcFileBatchReaderTest, TestTimestampType) {
    auto [natural_read_size, enable_tz] = GetParam();
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    std::string timezone_str = enable_tz ? "Asia/Tokyo" : timezone;
    paimon::test::TimezoneGuard tz_guard(timezone_str);
    arrow::FieldVector fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone_str)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone_str)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone_str)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone_str)),
    };

    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
["1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null, "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
["1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null, null, "1970-01-01 00:00:06", null, "1970-01-01 00:00:00.000006", null]
    ])")
            .ValueOrDie());
    auto arrow_schema = arrow::schema(fields);
    std::shared_ptr<arrow::ChunkedArray> expected_array =
        std::make_shared<arrow::ChunkedArray>(array);
    {
        // read data generated by Java Paimon
        std::string file_name = paimon::test::GetDataDir() +
                                "/orc/append_with_multiple_ts_precision_and_timezone.db/"
                                "append_with_multiple_ts_precision_and_timezone/bucket-0/"
                                "data-3f58c403-1672-49a3-93c0-d90cfff9bd8a-0.orc";
        auto orc_batch_reader = PrepareOrcFileBatchReader(file_name, arrow_schema.get(),
                                                          batch_size_, natural_read_size);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::ChunkedArray> result_array,
            paimon::test::ReadResultCollector::CollectResult(std::move(orc_batch_reader)));
        ASSERT_TRUE(result_array->Equals(*expected_array)) << result_array->ToString() << std::endl
                                                           << expected_array->ToString();
    }
    {
        // read data generated by C++ Paimon
        auto dir = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        std::string data_path = dir->Str() + "/test.data";
        WriteArray(dir->GetFileSystem(), data_path, array, arrow_schema,
                   /*options=*/{{"orc.timestamp-ltz.legacy.type", "false"}});
        auto orc_batch_reader = PrepareOrcFileBatchReader(data_path, arrow_schema.get(),
                                                          batch_size_, natural_read_size);
        // check file schema
        ASSERT_OK_AND_ASSIGN(auto c_file_schema, orc_batch_reader->GetFileSchema());
        auto result_file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOr(nullptr);
        ASSERT_TRUE(result_file_schema);

        arrow::FieldVector expected_fields = {
            arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::NANO)),
            arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::NANO)),
            arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::NANO)),
            arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
            arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::NANO, timezone_str)),
            arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::NANO, timezone_str)),
            arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::NANO, timezone_str)),
            arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone_str)),
        };
        auto expected_file_schema = arrow::schema(expected_fields);
        ASSERT_TRUE(result_file_schema->Equals(expected_file_schema));

        // check array
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::ChunkedArray> result_array,
            paimon::test::ReadResultCollector::CollectResult(std::move(orc_batch_reader)));
        ASSERT_TRUE(result_array->Equals(expected_array)) << result_array->ToString();
    }
}

// TODO(liancheng.lsz): TestBitmapPushDownWithMultiRowGroups, TestPredicateAndBitmapPushDown
// TODO(liancheng.lsz): TestGenReadRanges

TEST_F(OrcFileBatchReaderTest, TestNestedFieldProjection) {
    // Write data with nested struct: struct<col1:struct<sub1:int,sub2:double,sub3:string>,col2:int>
    auto sub1 = arrow::field("sub1", arrow::int32());
    auto sub2 = arrow::field("sub2", arrow::float64());
    auto sub3 = arrow::field("sub3", arrow::utf8());
    auto col1 = arrow::field("col1", arrow::struct_({sub1, sub2, sub3}));
    auto col2 = arrow::field("col2", arrow::int32());

    arrow::FieldVector write_fields = {col1, col2};
    auto write_schema = arrow::schema(write_fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(write_fields), R"([
        [[10, 1.1, "aaa"], 100],
        [[20, 2.2, "bbb"], 200],
        [[30, 3.3, "ccc"], 300],
        [null, 400],
        [[50, null, "eee"], null]
    ])")
            .ValueOrDie());

    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string data_path = dir->Str() + "/nested_test.orc";
    WriteArray(dir->GetFileSystem(), data_path, src_array, write_schema, /*options=*/{});

    {
        // Read partial sub-fields: col1.sub1 and col1.sub3 only
        auto read_col1 = arrow::field("col1", arrow::struct_({sub1, sub3}));
        arrow::Schema read_schema({read_col1});
        auto orc_batch_reader =
            PrepareOrcFileBatchReader(data_path, &read_schema,
                                      /*batch_size=*/10, DEFAULT_NATURAL_READ_SIZE);
        ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                    std::move(orc_batch_reader)));

        auto expected = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_col1}), R"([
            [[10, "aaa"]],
            [[20, "bbb"]],
            [[30, "ccc"]],
            [null],
            [[50, "eee"]]
        ])")
                .ValueOrDie());
        auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected);
        ASSERT_TRUE(result_array->Equals(expected_chunked))
            << "actual: " << result_array->ToString()
            << "\nexpected: " << expected_chunked->ToString();
    }
    {
        // Read partial sub-fields + top-level field: col1.sub2 and col2
        auto read_col1 = arrow::field("col1", arrow::struct_({sub2}));
        arrow::Schema read_schema({read_col1, col2});
        auto orc_batch_reader =
            PrepareOrcFileBatchReader(data_path, &read_schema,
                                      /*batch_size=*/10, DEFAULT_NATURAL_READ_SIZE);
        ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                    std::move(orc_batch_reader)));

        auto expected = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_col1, col2}), R"([
            [[1.1], 100],
            [[2.2], 200],
            [[3.3], 300],
            [null, 400],
            [[null], null]
        ])")
                .ValueOrDie());
        auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected);
        ASSERT_TRUE(result_array->Equals(expected_chunked))
            << "actual: " << result_array->ToString()
            << "\nexpected: " << expected_chunked->ToString();
    }
    {
        // Read single nested sub-field: col1.sub1 only
        auto read_col1 = arrow::field("col1", arrow::struct_({sub1}));
        arrow::Schema read_schema({read_col1});
        auto orc_batch_reader =
            PrepareOrcFileBatchReader(data_path, &read_schema,
                                      /*batch_size=*/10, DEFAULT_NATURAL_READ_SIZE);
        ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                    std::move(orc_batch_reader)));

        auto expected = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_col1}), R"([
            [[10]],
            [[20]],
            [[30]],
            [null],
            [[50]]
        ])")
                .ValueOrDie());
        auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected);
        ASSERT_TRUE(result_array->Equals(expected_chunked))
            << "actual: " << result_array->ToString()
            << "\nexpected: " << expected_chunked->ToString();
    }
}

TEST_F(OrcFileBatchReaderTest, TestDeepNestedFieldProjection) {
    // struct<a:struct<b:struct<c:int,d:string>,e:double>,f:int>
    auto field_c = arrow::field("c", arrow::int32());
    auto field_d = arrow::field("d", arrow::utf8());
    auto field_b = arrow::field("b", arrow::struct_({field_c, field_d}));
    auto field_e = arrow::field("e", arrow::float64());
    auto field_a = arrow::field("a", arrow::struct_({field_b, field_e}));
    auto field_f = arrow::field("f", arrow::int32());

    arrow::FieldVector write_fields = {field_a, field_f};
    auto write_schema = arrow::schema(write_fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(write_fields), R"([
        [[[1, "x"], 10.0], 100],
        [[[2, "y"], 20.0], 200],
        [null, 300]
    ])")
            .ValueOrDie());

    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string data_path = dir->Str() + "/deep_nested_test.orc";
    WriteArray(dir->GetFileSystem(), data_path, src_array, write_schema, /*options=*/{});

    {
        // Read a.b.c only (skip a.b.d and a.e)
        auto read_b = arrow::field("b", arrow::struct_({field_c}));
        auto read_a = arrow::field("a", arrow::struct_({read_b}));
        arrow::Schema read_schema({read_a});
        auto orc_batch_reader =
            PrepareOrcFileBatchReader(data_path, &read_schema,
                                      /*batch_size=*/10, DEFAULT_NATURAL_READ_SIZE);
        ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                    std::move(orc_batch_reader)));

        auto expected = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_a}), R"([
            [[[1]]],
            [[[2]]],
            [null]
        ])")
                .ValueOrDie());
        auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected);
        ASSERT_TRUE(result_array->Equals(expected_chunked))
            << "actual: " << result_array->ToString()
            << "\nexpected: " << expected_chunked->ToString();
    }
}

TEST_F(OrcFileBatchReaderTest, TestNestedFieldProjectionWithListAndMap) {
    // struct<col1:struct<sub1:int,sub2:array<int>,sub3:map<string,int>>,col2:string>
    auto sub1 = arrow::field("sub1", arrow::int32());
    auto sub2 = arrow::field("sub2", arrow::list(arrow::int32()));
    auto sub3 = arrow::field("sub3", arrow::map(arrow::utf8(), arrow::int32()));
    auto col1 = arrow::field("col1", arrow::struct_({sub1, sub2, sub3}));
    auto col2 = arrow::field("col2", arrow::utf8());

    arrow::FieldVector write_fields = {col1, col2};
    auto write_schema = arrow::schema(write_fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(write_fields), R"([
        [[10, [1, 2, 3], [["a", 1], ["b", 2]]], "hello"],
        [[20, [4, 5],    [["c", 3]]],            "world"],
        [[30, null,      null],                   null]
    ])")
            .ValueOrDie());

    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string data_path = dir->Str() + "/nested_list_map_test.orc";
    WriteArray(dir->GetFileSystem(), data_path, src_array, write_schema, /*options=*/{});

    {
        // Read col1.sub2 (list type) only — nested field projection skipping sub1 and sub3
        auto read_col1 = arrow::field("col1", arrow::struct_({sub2}));
        arrow::Schema read_schema({read_col1});
        auto orc_batch_reader =
            PrepareOrcFileBatchReader(data_path, &read_schema,
                                      /*batch_size=*/10, DEFAULT_NATURAL_READ_SIZE);
        ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                    std::move(orc_batch_reader)));

        auto expected = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_col1}), R"([
            [[[1, 2, 3]]],
            [[[4, 5]]],
            [[null]]
        ])")
                .ValueOrDie());
        auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected);
        ASSERT_TRUE(result_array->Equals(expected_chunked))
            << "actual: " << result_array->ToString()
            << "\nexpected: " << expected_chunked->ToString();
    }
    {
        // Read col1.sub1 + col1.sub3(map) — skip sub2
        auto read_col1 = arrow::field("col1", arrow::struct_({sub1, sub3}));
        arrow::Schema read_schema({read_col1, col2});
        auto orc_batch_reader =
            PrepareOrcFileBatchReader(data_path, &read_schema,
                                      /*batch_size=*/10, DEFAULT_NATURAL_READ_SIZE);
        ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                    std::move(orc_batch_reader)));

        auto expected = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({read_col1, col2}), R"([
            [[10, [["a", 1], ["b", 2]]], "hello"],
            [[20, [["c", 3]]],            "world"],
            [[30, null],                   null]
        ])")
                .ValueOrDie());
        auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected);
        ASSERT_TRUE(result_array->Equals(expected_chunked))
            << "actual: " << result_array->ToString()
            << "\nexpected: " << expected_chunked->ToString();
    }
}

TEST_F(OrcFileBatchReaderTest, TestListStructPartialProjection) {
    // Verify that array<struct<a:int, b:double>> read as array<struct<a:int>> is rejected.
    // Partial projection inside list/map elements is not supported; the list type toString()
    // comparison catches the mismatch and returns an error before any ORC batch is read.
    auto field_a = arrow::field("a", arrow::int32());
    auto field_b = arrow::field("b", arrow::float64());
    auto col1 = arrow::field("col1", arrow::list(arrow::struct_({field_a, field_b})));

    arrow::FieldVector write_fields = {col1};
    auto write_schema = arrow::schema(write_fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(write_fields), R"([
        [[[1, 1.1], [2, 2.2]]],
        [[[3, 3.3]]],
        [[null]],
        [null]
    ])")
            .ValueOrDie());

    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string data_path = dir->Str() + "/list_struct_projection_test.orc";
    WriteArray(dir->GetFileSystem(), data_path, src_array, write_schema, /*options=*/{});

    auto read_col1 = arrow::field("col1", arrow::list(arrow::struct_({field_a})));
    arrow::Schema read_schema({read_col1});
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, file_system->Open(data_path));
    ASSERT_OK_AND_ASSIGN(auto in_stream,
                         OrcInputStreamImpl::Create(input_stream, DEFAULT_NATURAL_READ_SIZE));
    ASSERT_OK_AND_ASSIGN(
        auto orc_batch_reader,
        OrcFileBatchReader::Create(std::move(in_stream), std::make_shared<OrcReadMemory>(pool_),
                                   /*options=*/{},
                                   /*batch_size=*/10));
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(read_schema, c_schema.get()).ok());
    ASSERT_NOK_WITH_MSG(orc_batch_reader->SetReadSchema(c_schema.get(), /*predicate=*/nullptr,
                                                        /*selection_bitmap=*/std::nullopt),
                        "type mismatch");
}

TEST_F(OrcFileBatchReaderTest, TestAddMetadataPerFieldMetadata) {
    // Write a simple ORC file, call AddMetadata to inject per-field metadata
    // before Finish, then read back and verify the file schema carries the metadata.
    auto write_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("name", arrow::utf8()),
        arrow::field("score", arrow::float64()),
    });

    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    std::string file_path = dir->Str() + "/update_schema_test.orc";

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs->Create(file_path, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(auto orc_output_stream, OrcOutputStreamImpl::Create(out));
    ASSERT_OK_AND_ASSIGN(auto format_writer,
                         OrcFormatWriter::Create(std::move(orc_output_stream), *write_schema,
                                                 /*options=*/{}, "zstd",
                                                 /*batch_size=*/10, pool_));

    // Write one batch of data.
    auto data = arrow::ipc::internal::json::ArrayFromJSON(
                    arrow::struct_(write_schema->fields()),
                    R"([[1, "alice", 95.5], [2, "bob", 88.0], [3, "charlie", 72.3]])")
                    .ValueOrDie();
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*data, &c_array).ok());
    ASSERT_OK(format_writer->AddBatch(&c_array));
    ASSERT_OK(format_writer->Flush());

    // Build an updated schema with per-field metadata on "name" and "score".
    auto name_meta = std::make_shared<arrow::KeyValueMetadata>();
    name_meta->Append("shredding.field_mapping", "0:alice,1:bob,2:charlie");
    name_meta->Append("shredding.num_columns", "3");
    auto score_meta = std::make_shared<arrow::KeyValueMetadata>();
    score_meta->Append("custom.unit", "percent");

    auto updated_schema = arrow::schema({
        write_schema->field(0),                            // id — no metadata
        write_schema->field(1)->WithMetadata(name_meta),   // name — shredding metadata
        write_schema->field(2)->WithMetadata(score_meta),  // score — custom metadata
    });

    // AddMetadata must be called before Finish.
    ASSERT_OK(format_writer->AddMetadata(
        {{ArrowUtils::kArrowSchemaMetadataKey, SerializeSchemaToString(updated_schema)}}));
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());

    // Read back: GetFileSchema should reflect the updated per-field metadata.
    auto orc_batch_reader = PrepareOrcFileBatchReader(file_path, write_schema.get(), batch_size_,
                                                      DEFAULT_NATURAL_READ_SIZE);

    ASSERT_OK_AND_ASSIGN(auto c_file_schema, orc_batch_reader->GetFileSchema());
    auto file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOrDie();

    // Field 0 "id": no metadata.
    ASSERT_EQ("id", file_schema->field(0)->name());

    // Field 1 "name": should have the shredding metadata we set.
    ASSERT_EQ("name", file_schema->field(1)->name());
    auto read_name_meta = file_schema->field(1)->metadata();
    ASSERT_NE(nullptr, read_name_meta);
    auto field_mapping_val = read_name_meta->Get("shredding.field_mapping").ValueOrDie();
    ASSERT_EQ("0:alice,1:bob,2:charlie", field_mapping_val);
    auto num_columns_val = read_name_meta->Get("shredding.num_columns").ValueOrDie();
    ASSERT_EQ("3", num_columns_val);

    // Field 2 "score": should have the custom metadata.
    ASSERT_EQ("score", file_schema->field(2)->name());
    auto read_score_meta = file_schema->field(2)->metadata();
    ASSERT_NE(nullptr, read_score_meta);
    auto unit_val = read_score_meta->Get("custom.unit").ValueOrDie();
    ASSERT_EQ("percent", unit_val);

    // Also verify data integrity — read it back and compare content.
    ASSERT_OK_AND_ASSIGN(auto result_array, paimon::test::ReadResultCollector::CollectResult(
                                                std::move(orc_batch_reader)));
    ASSERT_EQ(result_array->num_chunks(), 1);
    ASSERT_TRUE(data->Equals(*result_array->chunk(0))) << result_array->ToString();
}

}  // namespace paimon::orc::test

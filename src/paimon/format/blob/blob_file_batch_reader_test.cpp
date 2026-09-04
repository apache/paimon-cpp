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

#include "paimon/format/blob/blob_file_batch_reader.h"

#include <string_view>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/reader/blob_fallback_batch_reader.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/data/blob.h"
#include "paimon/format/blob/blob_format_writer.h"
#include "paimon/format/blob/blob_reader_builder.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::blob::test {
namespace {

std::string HexToBytes(std::string_view hex) {
    auto hex_value = [](char c) -> uint8_t {
        return c <= '9' ? static_cast<uint8_t>(c - '0') : static_cast<uint8_t>(c - 'a' + 10);
    };
    std::string bytes;
    bytes.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2) {
        bytes.push_back(static_cast<char>((hex_value(hex[i]) << 4) | hex_value(hex[i + 1])));
    }
    return bytes;
}

std::string MapBlobGoldenBytes() {
    // Java-compatible MAP<STRING, BLOB> golden file. Rows are:
    // {alpha: "hello", empty: "", missing: null}, null, {}, {omega: "world"}.
    return HexToBytes(
        "cf114e584243424d0103000000616c706861656d7074796d697373696e676865"
        "6c6c6f0a00040a090103000000030000003d000000000000002a64aaabcf114e"
        "584243424d0100000000000000000000000021000000000000008360591ecf11"
        "4e584243424d01010000006f6d656761776f726c640a0a01000000010000002d"
        "00000000000000248fe4237a7b44180400000001");
}

}  // namespace

TEST(BlobReaderBuilderTest, RejectsNullMemoryPool) {
    BlobReaderBuilder builder(/*batch_size=*/10, /*options=*/{});
    builder.WithMemoryPool(nullptr);
    ASSERT_NOK_WITH_MSG(builder.Build(nullptr), "Blob reader memory pool is nullptr");
}

class BlobFileBatchReaderTest : public testing::Test, public ::testing::WithParamInterface<bool> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void CheckResult(const std::string& table_path, const std::string& paimon_blob_file,
                     const std::vector<std::string>& original_blob_files, bool blob_as_descriptor,
                     const std::optional<RoaringBitmap32>& selection_bitmap = std::nullopt) {
        auto schema = arrow::schema({BlobUtils::ToArrowField(blob_field_name_, false)});
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             fs->Open(table_path + "/bucket-0/" + paimon_blob_file));
        ASSERT_OK_AND_ASSIGN(auto reader,
                             BlobFileBatchReader::Create(
                                 input_stream, /*batch_size=*/1024, blob_as_descriptor,
                                 /*emit_placeholder_sentinel=*/false, pool_, GetArrowPool(pool_)));
        ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, selection_bitmap));
        ASSERT_OK_AND_ASSIGN(auto chunked_array,
                             paimon::test::ReadResultCollector::CollectResult(std::move(reader)));
        if (chunked_array == nullptr) {
            ASSERT_EQ(0, original_blob_files.size());
            return;
        }

        std::shared_ptr<arrow::Array> combined_array =
            arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
        if (original_blob_files.size() == 0) {
            ASSERT_EQ(0, combined_array->length());
            return;
        }
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(combined_array);
        ASSERT_TRUE(struct_array);
        auto blob_array =
            std::dynamic_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
        ASSERT_EQ(blob_array->length(), original_blob_files.size());
        for (size_t i = 0; i < original_blob_files.size(); i++) {
            ASSERT_OK_AND_ASSIGN(auto origin_input_stream,
                                 fs->Open(table_path + "/" + original_blob_files[i]));
            ASSERT_OK_AND_ASSIGN(auto origin_length, origin_input_stream->Length());
            auto origin_bytes = Bytes::AllocateBytes(origin_length, pool_.get());
            ASSERT_OK_AND_ASSIGN(auto actual_read_length,
                                 origin_input_stream->Read(origin_bytes->data(), origin_length));
            ASSERT_EQ(actual_read_length, origin_length);
            if (blob_as_descriptor) {
                auto blob_descriptor = blob_array->GetString(i);
                ASSERT_OK_AND_ASSIGN(auto blob, Blob::FromDescriptor(blob_descriptor.data(),
                                                                     blob_descriptor.size()));
                ASSERT_OK_AND_ASSIGN(auto input_stream, blob->NewInputStream(fs));
                ASSERT_OK_AND_ASSIGN(auto pos, input_stream->GetPos());
                ASSERT_EQ(pos, 0);
                ASSERT_OK_AND_ASSIGN(auto length, input_stream->Length());
                auto bytes = Bytes::AllocateBytes(length, pool_.get());
                ASSERT_OK_AND_ASSIGN(auto actual_read_length,
                                     input_stream->Read(bytes->data(), length));
                ASSERT_EQ(actual_read_length, length);
                ASSERT_EQ(length, origin_length);
                ASSERT_EQ(*bytes, *origin_bytes);
            } else {
                auto blob_data = blob_array->GetString(i);
                ASSERT_EQ(blob_data.size(), origin_length);
                std::string origin_data(origin_bytes->data(), origin_length);
                ASSERT_EQ(blob_data, origin_data);
            }
        }
    }

    Result<std::string> ReadMapBlobValue(const std::shared_ptr<arrow::LargeBinaryArray>& blob_array,
                                         int64_t index, bool blob_as_descriptor,
                                         const std::shared_ptr<FileSystem>& file_system) {
        std::string stored_value = blob_array->GetString(index);
        if (!blob_as_descriptor) {
            return stored_value;
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Blob> blob,
                               Blob::FromDescriptor(stored_value.data(), stored_value.size()));
        PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> value, blob->ToData(file_system, pool_));
        if (value->size() == 0) {
            return std::string();
        }
        return std::string(value->data(), value->size());
    }

 private:
    std::string blob_field_name_;
    std::shared_ptr<MemoryPool> pool_;
};

TEST_P(BlobFileBatchReaderTest, TestSimple) {
    std::string test_data_path = paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    bool blob_as_descriptor = GetParam();
    ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob",
                {"blob_0_811d5dab.bin", "blob_1_b81cf9f4.bin", "blob_2_470e1dfe.bin"},
                blob_as_descriptor);
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-2.blob",
                {"blob_3_07b08c4d.bin", "blob_4_67007c96.bin"}, blob_as_descriptor);
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-3.blob",
                {"blob_5_f7099dea.bin", "blob_6_6b6706ef.bin", "blob_7_6bcae65e.bin",
                 "blob_8_5fba0737.bin"},
                blob_as_descriptor);
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-4.blob",
                {"blob_9_f54d253c.bin"}, blob_as_descriptor);
}

TEST_P(BlobFileBatchReaderTest, TestMapBlob) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::string file_path = dir->Str() + "/map-blob.blob";
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();

    const std::string file_bytes = MapBlobGoldenBytes();
    ASSERT_OK(file_system->WriteFile(file_path, file_bytes, /*overwrite=*/true));

    std::shared_ptr<arrow::Field> blob_item = BlobUtils::ToArrowField("value", true);
    auto key_field = arrow::field("key", arrow::utf8(), false);
    auto map_type = std::make_shared<arrow::MapType>(key_field, blob_item);
    ASSERT_TRUE(BlobUtils::IsBlobField(map_type->item_field()));
    auto map_field = arrow::field("blob_map", map_type, true);
    auto schema = arrow::schema({map_field});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, file_system->Open(file_path));
    const bool blob_as_descriptor = GetParam();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                         BlobFileBatchReader::Create(
                             input_stream, /*batch_size=*/2, blob_as_descriptor,
                             /*emit_placeholder_sentinel=*/false, pool_, GetArrowPool(pool_)));
    ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> chunked_array,
                         paimon::test::ReadResultCollector::CollectResult(std::move(reader)));
    std::shared_ptr<arrow::Array> combined_array =
        arrow::Concatenate(chunked_array->chunks()).ValueOrDie();

    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(combined_array);
    ASSERT_TRUE(struct_array);
    auto map_array = std::dynamic_pointer_cast<arrow::MapArray>(struct_array->field(0));
    ASSERT_TRUE(map_array);
    auto values = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(map_array->items());
    ASSERT_TRUE(values);
    arrow::LargeBinaryBuilder normalized_values_builder;
    for (int64_t i = 0; i < values->length(); ++i) {
        if (values->IsNull(i)) {
            ASSERT_TRUE(normalized_values_builder.AppendNull().ok());
            continue;
        }
        ASSERT_OK_AND_ASSIGN(std::string value,
                             ReadMapBlobValue(values, i, blob_as_descriptor, file_system));
        ASSERT_TRUE(normalized_values_builder.Append(value).ok());
    }
    std::shared_ptr<arrow::Array> normalized_values;
    ASSERT_TRUE(normalized_values_builder.Finish(&normalized_values).ok());
    auto normalized_map = std::make_shared<arrow::MapArray>(
        map_type, map_array->length(), map_array->value_offsets(), map_array->keys(),
        normalized_values, map_array->null_bitmap(), map_array->null_count(), map_array->offset());
    std::shared_ptr<arrow::Array> expected = arrow::ipc::internal::json::ArrayFromJSON(map_type,
                                                                                       R"json([
            [["alpha", "hello"], ["empty", ""], ["missing", null]],
            null,
            [],
            [["omega", "world"]]
        ])json")
                                                 .ValueOrDie();
    ASSERT_TRUE(expected->Equals(normalized_map))
        << "expected: " << expected->ToString() << "\nactual: " << normalized_map->ToString();
}

TEST_P(BlobFileBatchReaderTest, MapBlobFallbackAcrossSequenceLayers) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    const std::string old_file_path = dir->Str() + "/old-map.blob";
    const std::string new_file_path = dir->Str() + "/new-placeholder.blob";

    const std::string old_bytes = MapBlobGoldenBytes();
    ASSERT_OK(file_system->WriteFile(old_file_path, old_bytes, /*overwrite=*/true));

    // Generate four genuine -2 outer-file entries through the scalar writer. The outer blob
    // index is type-independent; the map reader turns them into its map placeholder sentinel.
    std::shared_ptr<arrow::Field> scalar_blob_field = BlobUtils::ToArrowField("blob_map", true);
    auto scalar_struct_type = arrow::struct_({scalar_blob_field});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> new_output,
                         file_system->Create(new_file_path, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFormatWriter> writer,
                         BlobFormatWriter::Create(new_output, scalar_struct_type,
                                                  /*write_null_on_missing_file=*/false,
                                                  /*write_null_on_fetch_failure=*/false,
                                                  /*write_placeholder=*/true, file_system, pool_));
    arrow::LargeBinaryBuilder scalar_builder;
    const std::string sentinel(BlobDefs::PlaceholderSentinelView());
    for (int32_t i = 0; i < 4; i++) {
        ASSERT_TRUE(scalar_builder.Append(sentinel).ok());
    }
    std::shared_ptr<arrow::Array> scalar_values;
    ASSERT_TRUE(scalar_builder.Finish(&scalar_values).ok());
    std::shared_ptr<arrow::StructArray> scalar_rows =
        arrow::StructArray::Make({scalar_values}, {scalar_blob_field}).ValueOrDie();
    for (int32_t i = 0; i < 4; i++) {
        ::ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*scalar_rows->Slice(i, 1), &c_array).ok());
        ASSERT_OK(writer->AddBatch(&c_array));
    }
    ASSERT_OK(writer->Finish());
    ASSERT_OK(new_output->Close());

    auto map_type = std::make_shared<arrow::MapType>(arrow::field("key", arrow::utf8(), false),
                                                     BlobUtils::ToArrowField("value", true));
    std::shared_ptr<arrow::Field> map_field = arrow::field("blob_map", map_type, true);
    auto map_schema = arrow::schema({map_field});
    const bool blob_as_descriptor = GetParam();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> new_input, file_system->Open(new_file_path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> new_reader,
                         BlobFileBatchReader::Create(
                             new_input, /*batch_size=*/2, blob_as_descriptor,
                             /*emit_placeholder_sentinel=*/true, pool_, GetArrowPool(pool_)));
    ::ArrowSchema new_schema;
    ASSERT_TRUE(arrow::ExportSchema(*map_schema, &new_schema).ok());
    ASSERT_OK(new_reader->SetReadSchema(&new_schema, nullptr, std::nullopt));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> old_input, file_system->Open(old_file_path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> old_reader,
                         BlobFileBatchReader::Create(
                             old_input, /*batch_size=*/2, blob_as_descriptor,
                             /*emit_placeholder_sentinel=*/true, pool_, GetArrowPool(pool_)));
    ::ArrowSchema old_schema;
    ASSERT_TRUE(arrow::ExportSchema(*map_schema, &old_schema).ok());
    ASSERT_OK(old_reader->SetReadSchema(&old_schema, nullptr, std::nullopt));

    std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups(2);
    groups[0].push_back({std::move(new_reader), {}});
    groups[1].push_back({std::move(old_reader), {}});
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<BlobFallbackBatchReader> fallback,
        BlobFallbackBatchReader::Create(std::move(groups), map_schema, /*read_batch_size=*/2,
                                        GetArrowPool(pool_)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         paimon::test::ReadResultCollector::CollectResult(std::move(fallback)));
    std::shared_ptr<arrow::Array> combined = arrow::Concatenate(result->chunks()).ValueOrDie();
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(combined);
    ASSERT_TRUE(struct_array);
    auto map_array = std::dynamic_pointer_cast<arrow::MapArray>(struct_array->field(0));
    ASSERT_TRUE(map_array);
    ASSERT_EQ(4, map_array->length());
    ASSERT_EQ(3, map_array->value_length(0));
    ASSERT_TRUE(map_array->IsNull(1));
    ASSERT_EQ(0, map_array->value_length(2));
    ASSERT_EQ(1, map_array->value_length(3));
    auto keys = std::dynamic_pointer_cast<arrow::StringArray>(map_array->keys());
    auto values = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(map_array->items());
    ASSERT_EQ("alpha", keys->GetString(0));
    ASSERT_EQ("omega", keys->GetString(3));
    ASSERT_OK_AND_ASSIGN(std::string first_value,
                         ReadMapBlobValue(values, 0, blob_as_descriptor, file_system));
    ASSERT_OK_AND_ASSIGN(std::string last_value,
                         ReadMapBlobValue(values, 3, blob_as_descriptor, file_system));
    ASSERT_EQ("hello", first_value);
    ASSERT_EQ("world", last_value);
}

TEST_F(BlobFileBatchReaderTest, RejectsNonCanonicalDecimalMapKey) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::string file_path = dir->Str() + "/bad-decimal-map.blob";
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    // The two raw keys 00 and 0000 both decode to decimal zero. The second is not the shortest
    // Java BigInteger two's-complement representation and must not bypass duplicate detection.
    const std::string file_bytes = HexToBytes(
        "cf114e584243424d010200000000000002020000020000000200000028000000"
        "0000000000000000d0000200000001");
    ASSERT_OK(file_system->WriteFile(file_path, file_bytes, /*overwrite=*/true));

    auto map_type =
        std::make_shared<arrow::MapType>(arrow::field("key", arrow::decimal128(20, 0), false),
                                         BlobUtils::ToArrowField("value", true));
    auto schema = arrow::schema({arrow::field("blob_map", map_type, true)});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system->Open(file_path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                         BlobFileBatchReader::Create(
                             input, /*batch_size=*/1, /*blob_as_descriptor=*/false,
                             /*emit_placeholder_sentinel=*/false, pool_, GetArrowPool(pool_)));
    ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, std::nullopt));
    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "non-canonical decimal key");
}

TEST_F(BlobFileBatchReaderTest, RejectsInvalidUtf8StringMapKey) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::string file_path = dir->Str() + "/bad-string-map.blob";
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    std::string file_bytes = MapBlobGoldenBytes();
    ASSERT_GT(file_bytes.size(), 13);
    // The first key starts after the outer magic and the map header.
    file_bytes[13] = static_cast<char>(0xFF);
    ASSERT_OK(file_system->WriteFile(file_path, file_bytes, /*overwrite=*/true));

    auto map_type = arrow::map(arrow::utf8(), BlobUtils::ToArrowField("value", /*nullable=*/true));
    auto schema = arrow::schema({arrow::field("blob_map", map_type)});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system->Open(file_path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                         BlobFileBatchReader::Create(
                             input, /*batch_size=*/1, /*blob_as_descriptor=*/false,
                             /*emit_placeholder_sentinel=*/false, pool_, GetArrowPool(pool_)));
    ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, std::nullopt));
    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "invalid UTF-8");
}

TEST_F(BlobFileBatchReaderTest, RejectsNullMapKey) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::string file_path = dir->Str() + "/null-key-map.blob";
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    std::string file_bytes = MapBlobGoldenBytes();
    const std::string key_index = HexToBytes("0a0004");
    const size_t key_index_offset = file_bytes.find(key_index);
    ASSERT_NE(std::string::npos, key_index_offset);
    // The first delta-varint changes from key length 5 to Java's null marker -1.
    file_bytes[key_index_offset] = static_cast<char>(0x01);
    ASSERT_OK(file_system->WriteFile(file_path, file_bytes, /*overwrite=*/true));

    auto map_type = arrow::map(arrow::utf8(), BlobUtils::ToArrowField("value", /*nullable=*/true));
    auto schema = arrow::schema({arrow::field("blob_map", map_type)});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system->Open(file_path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                         BlobFileBatchReader::Create(
                             input, /*batch_size=*/1, /*blob_as_descriptor=*/false,
                             /*emit_placeholder_sentinel=*/false, pool_, GetArrowPool(pool_)));
    ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, std::nullopt));
    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "MAP<..., BLOB> keys cannot be null");
}

TEST_P(BlobFileBatchReaderTest, TestPushdownBitmap) {
    std::string test_data_path = paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    bool blob_as_descriptor = GetParam();
    ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));
    RoaringBitmap32 roaring_0;
    roaring_0.Add(0);
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob",
                {"blob_0_811d5dab.bin"}, blob_as_descriptor, roaring_0);
    RoaringBitmap32 roaring_1;
    roaring_1.Add(1);

    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-2.blob",
                {"blob_4_67007c96.bin"}, blob_as_descriptor, roaring_1);
    RoaringBitmap32 roaring_2;
    roaring_2.Add(0);
    roaring_2.Add(1);
    roaring_2.Add(3);
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-3.blob",
                {"blob_5_f7099dea.bin", "blob_6_6b6706ef.bin", "blob_8_5fba0737.bin"},
                blob_as_descriptor, roaring_2);
    RoaringBitmap32 roaring_3;
    CheckResult(table_path, "data-d7816e8e-6c6d-4e28-9137-837cdf706350-4.blob", {},
                blob_as_descriptor, roaring_3);
}

TEST_F(BlobFileBatchReaderTest, TestRowNumbers) {
    auto schema = arrow::schema({BlobUtils::ToArrowField("my_blob_field", false)});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

    std::string test_data_path = paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<InputStream> input_stream,
        fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         BlobFileBatchReader::Create(input_stream,
                                                     /*batch_size=*/1, /*blob_as_descriptor=*/true,
                                                     /*emit_placeholder_sentinel=*/false, pool_,
                                                     GetArrowPool(pool_)));

    ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto number_of_rows, reader->GetNumberOfRows());
    ASSERT_EQ(3, number_of_rows);
    ASSERT_NOK(reader->GetPreviousBatchFileRowId(0));
    ASSERT_OK_AND_ASSIGN(auto batch1, reader->NextBatch());
    ArrowArrayRelease(batch1.first.get());
    ArrowSchemaRelease(batch1.second.get());
    ASSERT_EQ(0, reader->GetPreviousBatchFileRowId(0).value());
    ASSERT_OK_AND_ASSIGN(auto batch2, reader->NextBatch());
    ASSERT_EQ(1, reader->GetPreviousBatchFileRowId(0).value());
    ArrowArrayRelease(batch2.first.get());
    ArrowSchemaRelease(batch2.second.get());
    ASSERT_OK_AND_ASSIGN(auto batch3, reader->NextBatch());
    ASSERT_EQ(2, reader->GetPreviousBatchFileRowId(0).value());
    ArrowArrayRelease(batch3.first.get());
    ArrowSchemaRelease(batch3.second.get());
    ASSERT_OK_AND_ASSIGN(auto batch4, reader->NextBatch());
    ASSERT_NOK(reader->GetPreviousBatchFileRowId(0));
    ASSERT_TRUE(BatchReader::IsEofBatch(batch4));
}

TEST_F(BlobFileBatchReaderTest, TestRowNumbersWithSelectionBitmap) {
    // a selection bitmap removes rows, but batch positions must still map back to the original
    // file row indexes so _ROW_ID completion works under row-range pushdown
    auto schema = arrow::schema({BlobUtils::ToArrowField("my_blob_field", false)});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

    std::string test_data_path = paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<InputStream> input_stream,
        fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         BlobFileBatchReader::Create(input_stream,
                                                     /*batch_size=*/1, /*blob_as_descriptor=*/true,
                                                     /*emit_placeholder_sentinel=*/false, pool_,
                                                     GetArrowPool(pool_)));

    RoaringBitmap32 selection;
    selection.Add(0);
    selection.Add(2);
    ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, selection));
    ASSERT_NOK_WITH_MSG(reader->GetPreviousBatchFileRowId(0), "No batch has been read yet");
    ASSERT_OK_AND_ASSIGN(auto batch1, reader->NextBatch());
    ArrowArrayRelease(batch1.first.get());
    ArrowSchemaRelease(batch1.second.get());
    ASSERT_EQ(0, reader->GetPreviousBatchFileRowId(0).value());
    ASSERT_OK_AND_ASSIGN(auto batch2, reader->NextBatch());
    ArrowArrayRelease(batch2.first.get());
    ArrowSchemaRelease(batch2.second.get());
    ASSERT_EQ(2, reader->GetPreviousBatchFileRowId(0).value());
    ASSERT_OK_AND_ASSIGN(auto batch3, reader->NextBatch());
    ASSERT_NOK_WITH_MSG(reader->GetPreviousBatchFileRowId(0), "Last batch was EOF");
    ASSERT_TRUE(BatchReader::IsEofBatch(batch3));
}

TEST_F(BlobFileBatchReaderTest, InvalidScenario) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto file_system = std::make_shared<LocalFileSystem>();
    std::string test_data_path = paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
    std::string table_path = dir->Str();
    ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<InputStream> input_stream,
        fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
    {
        ASSERT_NOK_WITH_MSG(
            BlobFileBatchReader::Create(input_stream,
                                        /*batch_size=*/0, /*blob_as_descriptor=*/true,
                                        /*emit_placeholder_sentinel=*/false, pool_,
                                        GetArrowPool(pool_)),
            "blob file batch reader create failed: read batch size '0' should be larger than zero");
    }
    {
        ASSERT_NOK_WITH_MSG(BlobFileBatchReader::Create(
                                /*input_stream=*/nullptr,
                                /*batch_size=*/1, /*blob_as_descriptor=*/true,
                                /*emit_placeholder_sentinel=*/false, pool_, GetArrowPool(pool_)),
                            "blob file batch reader create failed: input stream is nullptr");
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto reader, BlobFileBatchReader::Create(/*input_stream=*/input_stream,
                                                     /*batch_size=*/1, /*blob_as_descriptor=*/true,
                                                     /*emit_placeholder_sentinel=*/false, pool_,
                                                     GetArrowPool(pool_)));
        ASSERT_NOK_WITH_MSG(reader->GetFileSchema(),
                            "blob file has no self-describing file schema");
        ASSERT_TRUE(reader->GetReaderMetrics());
        ASSERT_NOK_WITH_MSG(reader->NextBatch(),
                            "target type is nullptr, call SetReadSchema first");
        reader->Close();
        ASSERT_NOK_WITH_MSG(reader->NextBatch(), "blob file batch reader is closed");
    }
}

TEST_P(BlobFileBatchReaderTest, EmptyFile) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> output_stream,
                         file_system->Create(dir->Str() + "/file.blob", /*overwrite=*/true));
    std::shared_ptr<arrow::Field> blob_field = BlobUtils::ToArrowField("blob_col");
    auto struct_type = arrow::struct_({blob_field});
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream, struct_type, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, file_system, pool_));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());
    ASSERT_OK(output_stream->Flush());
    auto schema = arrow::schema({blob_field});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system->Open(dir->Str() + "/file.blob"));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         BlobFileBatchReader::Create(input_stream,
                                                     /*batch_size=*/1, /*blob_as_descriptor=*/true,
                                                     /*emit_placeholder_sentinel=*/false, pool_,
                                                     GetArrowPool(pool_)));

    ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto number_of_rows, reader->GetNumberOfRows());
    ASSERT_EQ(0, number_of_rows);
    ASSERT_NOK(reader->GetPreviousBatchFileRowId(0));
    ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
}

TEST_F(BlobFileBatchReaderTest, SetReadSchemaWithInvalidInputs) {
    {
        std::string test_data_path =
            paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
        auto dir = paimon::test::UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

        std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<InputStream> input_stream,
            fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
        ASSERT_OK_AND_ASSIGN(
            auto reader, BlobFileBatchReader::Create(input_stream,
                                                     /*batch_size=*/1, /*blob_as_descriptor=*/true,
                                                     /*emit_placeholder_sentinel=*/false, pool_,
                                                     GetArrowPool(pool_)));
        ASSERT_NOK_WITH_MSG(reader->SetReadSchema(/*read_schema=*/nullptr, /*predicate=*/nullptr,
                                                  /*selection_bitmap=*/std::nullopt),
                            "SetReadSchema failed: read schema cannot be nullptr");
    }
    {
        auto schema = arrow::schema({BlobUtils::ToArrowField("my_blob_field", false),
                                     BlobUtils::ToArrowField("my_blob_field_2", false)});

        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

        std::string test_data_path =
            paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
        auto dir = paimon::test::UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

        std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<InputStream> input_stream,
            fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
        ASSERT_OK_AND_ASSIGN(
            auto reader, BlobFileBatchReader::Create(input_stream,
                                                     /*batch_size=*/1, /*blob_as_descriptor=*/true,
                                                     /*emit_placeholder_sentinel=*/false, pool_,
                                                     GetArrowPool(pool_)));
        ASSERT_NOK_WITH_MSG(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                                  /*selection_bitmap=*/std::nullopt),
                            "read schema field number 2 is not 1");
    }
    {
        auto blob_field = arrow::field("my_blob_field", arrow::large_binary());

        auto schema = arrow::schema({blob_field});
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

        std::string test_data_path =
            paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
        auto dir = paimon::test::UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

        std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<InputStream> input_stream,
            fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
        ASSERT_OK_AND_ASSIGN(
            auto reader, BlobFileBatchReader::Create(input_stream,
                                                     /*batch_size=*/1, /*blob_as_descriptor=*/true,
                                                     /*emit_placeholder_sentinel=*/false, pool_,
                                                     GetArrowPool(pool_)));
        ASSERT_NOK_WITH_MSG(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                                  /*selection_bitmap=*/std::nullopt),
                            "field my_blob_field: large_binary is not BLOB");
    }
    {
        auto schema = arrow::schema({BlobUtils::ToArrowField("my_blob_field", false)});
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

        std::string test_data_path =
            paimon::test::GetDataDir() + "/db_with_blob.db/table_with_blob/";
        auto dir = paimon::test::UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(paimon::test::TestUtil::CopyDirectory(test_data_path, table_path));

        std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<InputStream> input_stream,
            fs->Open(table_path + "/bucket-0/data-d7816e8e-6c6d-4e28-9137-837cdf706350-1.blob"));
        ASSERT_OK_AND_ASSIGN(
            auto reader, BlobFileBatchReader::Create(input_stream,
                                                     /*batch_size=*/1, /*blob_as_descriptor=*/true,
                                                     /*emit_placeholder_sentinel=*/false, pool_,
                                                     GetArrowPool(pool_)));
        RoaringBitmap32 roaring;
        roaring.Add(0);
        roaring.Add(1);
        roaring.Add(2);
        roaring.Add(3);
        roaring.Add(4);
        ASSERT_NOK_WITH_MSG(
            reader->SetReadSchema(&c_schema, /*predicate=*/nullptr, /*selection_bitmap=*/roaring),
            "Invalid: row index 3 is out of bound of total row number 3");
    }
}

INSTANTIATE_TEST_SUITE_P(BlobAsDescriptor, BlobFileBatchReaderTest, ::testing::Values(true, false));

}  // namespace paimon::blob::test

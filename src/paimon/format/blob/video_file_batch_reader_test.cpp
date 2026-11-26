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

#include "paimon/format/blob/video_file_batch_reader.h"

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/reader/blob_read_context.h"
#include "paimon/common/utils/delta_varint_compressor.h"
#include "paimon/data/blob.h"
#include "paimon/data/blob_descriptor.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

using blob::VideoFileBatchReader;

void Append32(std::string* data, uint32_t value) {
    for (int32_t i = 0; i < 4; ++i) {
        data->push_back(static_cast<char>(value >> (8 * i)));
    }
}

std::string MakeVideoFile(bool placeholder = false, int64_t first_moov_length = 3) {
    std::string data = "VIDEOfilm";
    const std::vector<std::vector<int64_t>> indexes = {
        {5, 4},   {2, 0}, {first_moov_length, 0}, {2, 1, 1}, {0, placeholder ? -2 : -1, 1},
        {4, 0, 9}};
    std::vector<uint32_t> sizes;
    for (const auto& values : indexes) {
        const auto bytes = DeltaVarintCompressor::Compress(values);
        sizes.push_back(bytes.size());
        data.append(bytes.data(), bytes.size());
    }
    for (auto size : sizes) {
        Append32(&data, size);
    }
    Append32(&data, 0x4f454449);
    data.push_back(1);
    return data;
}

TEST(VideoFileBatchReaderTest, ReadsJavaCompatibleRunsAndSelection) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = std::make_shared<LocalFileSystem>();
    const std::string path = dir->Str() + "/part.video";
    ASSERT_OK(fs->WriteFile(path, MakeVideoFile(), true));
    ASSERT_OK_AND_ASSIGN(auto input, fs->Open(path));
    std::shared_ptr<InputStream> shared_input(std::move(input));
    ASSERT_OK_AND_ASSIGN(auto format, FileFormatFactory::Get("video", {}));
    ASSERT_OK_AND_ASSIGN(auto builder, format->CreateReaderBuilder(2));
    ASSERT_OK_AND_ASSIGN(auto reader, builder->Build(shared_input));
    ASSERT_OK_AND_ASSIGN(uint64_t row_count, reader->GetNumberOfRows());
    ASSERT_EQ(row_count, 4);

    auto schema = arrow::schema({BlobUtils::ToArrowField("video", true)});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatch());
    ASSERT_OK_AND_ASSIGN(uint64_t row_id, reader->GetPreviousBatchFileRowId(0));
    ASSERT_EQ(row_id, 0);
    auto imported = arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(imported.ok()) << imported.status();
    auto array = imported.ValueOrDie();
    auto rows = std::dynamic_pointer_cast<arrow::StructArray>(array);
    auto blobs = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(rows->field(0));
    ASSERT_EQ(blobs->length(), 2);
    for (int32_t i = 0; i < 2; ++i) {
        auto bytes = blobs->GetView(i);
        ASSERT_OK_AND_ASSIGN(auto blob, Blob::FromDescriptor(bytes.data(), bytes.size()));
        ASSERT_TRUE(blob->IsVideoFrame());
        ASSERT_EQ(blob->FrameIndex(), 4 + i);
        ASSERT_EQ(blob->MoovOffset(), 2);
        ASSERT_EQ(blob->MoovLength(), 3);
        ASSERT_OK_AND_ASSIGN(bool recognized,
                             BlobDescriptor::IsBlobDescriptor(bytes.data(), bytes.size()));
        ASSERT_TRUE(recognized);
        auto serialized = blob->ToDescriptor(GetDefaultPool());
        ASSERT_EQ(std::string_view(serialized->data(), serialized->size()), bytes);
        ASSERT_OK_AND_ASSIGN(auto payload, blob->ToData(fs, GetDefaultPool()));
        ASSERT_EQ(std::string_view(payload->data(), payload->size()), "VIDEO");
    }

    ASSERT_OK_AND_ASSIGN(batch, reader->NextBatch());
    imported = arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(imported.ok()) << imported.status();
    array = imported.ValueOrDie();
    rows = std::dynamic_pointer_cast<arrow::StructArray>(array);
    blobs = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(rows->field(0));
    ASSERT_TRUE(blobs->IsNull(0));
    auto bytes = blobs->GetView(1);
    ASSERT_OK_AND_ASSIGN(auto blob, Blob::FromDescriptor(bytes.data(), bytes.size()));
    ASSERT_EQ(blob->FrameIndex(), 9);
    ASSERT_EQ(blob->MoovOffset(), 0);
    ASSERT_EQ(blob->MoovLength(), 0);
    ASSERT_OK_AND_ASSIGN(auto second_payload, blob->ToData(fs, GetDefaultPool()));
    ASSERT_EQ(std::string_view(second_payload->data(), second_payload->size()), "film");

    ::ArrowSchema selected_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &selected_schema).ok());
    RoaringBitmap32 bitmap;
    bitmap.Add(0);
    bitmap.Add(3);
    ASSERT_OK(reader->SetReadSchema(&selected_schema, nullptr, bitmap));
    ASSERT_OK_AND_ASSIGN(batch, reader->NextBatch());
    ASSERT_OK_AND_ASSIGN(row_id, reader->GetPreviousBatchFileRowId(0));
    ASSERT_EQ(row_id, 0);
    ASSERT_OK_AND_ASSIGN(row_id, reader->GetPreviousBatchFileRowId(1));
    ASSERT_EQ(row_id, 3);

    ::ArrowSchema empty_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &empty_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&empty_schema, nullptr, RoaringBitmap32()));
    ASSERT_OK_AND_ASSIGN(auto empty_batch, reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(empty_batch));
    ASSERT_NOK(reader->GetPreviousBatchFileRowId(0));
    reader.reset();
    // The exported array keeps its Arrow allocator alive after the reader is destroyed.
    imported = arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(imported.ok()) << imported.status();
    ASSERT_EQ(imported.ValueOrDie()->length(), 2);
}

TEST(VideoFileBatchReaderTest, RejectsInvalidFooter) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = std::make_shared<LocalFileSystem>();
    const std::string path = dir->Str() + "/bad.video";
    auto bytes = MakeVideoFile();
    bytes.back() = 2;
    ASSERT_OK(fs->WriteFile(path, bytes, true));
    ASSERT_OK_AND_ASSIGN(auto input, fs->Open(path));
    std::shared_ptr<InputStream> shared_input(std::move(input));
    ASSERT_NOK(VideoFileBatchReader::Create(shared_input, 2, GetDefaultPool(), false, nullptr));
}

TEST(VideoFileBatchReaderTest, RejectsMoovRangeOutsideVideo) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = std::make_shared<LocalFileSystem>();
    const std::string path = dir->Str() + "/bad.video";
    ASSERT_OK(
        fs->WriteFile(path, MakeVideoFile(/*placeholder=*/false, /*first_moov_length=*/4), true));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, fs->Open(path));
    ASSERT_NOK_WITH_MSG(VideoFileBatchReader::Create(input, 2, GetDefaultPool(), false, nullptr),
                        "invalid moov range");
}

TEST(VideoFileBatchReaderTest, ExposesPlaceholdersOnlyForBlobFallback) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = std::make_shared<LocalFileSystem>();
    const std::string path = dir->Str() + "/part.video";
    ASSERT_OK(fs->WriteFile(path, MakeVideoFile(/*placeholder=*/true), true));
    auto schema = arrow::schema({BlobUtils::ToArrowField("video", true)});
    for (bool fallback : {false, true}) {
        std::map<std::string, std::string> options;
        if (fallback) {
            options[BlobDefs::kEmitPlaceholderSentinelKey] = "true";
        }
        ASSERT_OK_AND_ASSIGN(auto format, FileFormatFactory::Get("video", options));
        ASSERT_OK_AND_ASSIGN(auto builder, format->CreateReaderBuilder(4));
        auto* context_builder = dynamic_cast<BlobReadContextAwareBuilder*>(builder.get());
        ASSERT_TRUE(context_builder);
        auto context = std::make_shared<BlobReadContext>();
        context_builder->SetBlobReadContext(context);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, fs->Open(path));
        ASSERT_OK_AND_ASSIGN(auto reader, builder->Build(input));
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, std::nullopt));
        if (!fallback) {
            ASSERT_NOK(reader->NextBatch());
            continue;
        }
        context->request = BlobBatchRequest{2, /*read_payload=*/false};
        ASSERT_OK_AND_ASSIGN(auto skipped_batch, reader->NextBatch());
        auto skipped = arrow::ImportArray(skipped_batch.first.get(), skipped_batch.second.get());
        ASSERT_TRUE(skipped.ok()) << skipped.status();
        ASSERT_EQ(skipped.ValueOrDie()->length(), 2);
        context->request = BlobBatchRequest{1, /*read_payload=*/true};
        ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatch());
        auto imported = arrow::ImportArray(batch.first.get(), batch.second.get());
        ASSERT_TRUE(imported.ok()) << imported.status();
        auto rows = std::static_pointer_cast<arrow::StructArray>(imported.ValueOrDie());
        auto blobs = std::static_pointer_cast<arrow::LargeBinaryArray>(rows->field(0));
        ASSERT_EQ(blobs->length(), 1);
        ASSERT_EQ(blobs->GetView(0), BlobDefs::PlaceholderSentinelView());
    }
}

}  // namespace
}  // namespace paimon::test

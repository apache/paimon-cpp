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

#include <cstring>
#include <numeric>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/data/video_frame_descriptor.h"
#include "paimon/common/reader/blob_read_context.h"
#include "paimon/common/utils/delta_varint_compressor.h"
#include "paimon/data/blob.h"
#include "paimon/data/blob_descriptor.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/video_test_utils.h"

namespace paimon::test {
namespace {

using blob::VideoFileBatchReader;

void Append32(std::string* data, uint32_t value) {
    for (int32_t i = 0; i < 4; ++i) {
        data->push_back(static_cast<char>(value >> (8 * i)));
    }
}

void AppendVideoFooter(std::string* data, const std::vector<uint32_t>& index_lengths) {
    for (uint32_t length : index_lengths) {
        Append32(data, length);
    }
    Append32(data, 0x4f454449);
    data->push_back(1);
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
    AppendVideoFooter(&data, sizes);
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
        ASSERT_FALSE(recognized);
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

TEST(VideoFileBatchReaderTest, RoundTripsJavaFrameDescriptor) {
    ASSERT_OK_AND_ASSIGN(std::string bytes, ReadJavaVideoFixture("video-frame-descriptor-v1.hex"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Blob> blob,
                         Blob::FromDescriptor(bytes.data(), bytes.size()));
    ASSERT_TRUE(blob->IsVideoFrame());
    ASSERT_EQ(blob->Uri(), "s3://bucket/视频.mp4");
    ASSERT_EQ(blob->FrameIndex(), 42);
    ASSERT_EQ(blob->MoovOffset(), 0);
    ASSERT_EQ(blob->MoovLength(), 0);
    auto serialized = blob->ToDescriptor(GetDefaultPool());
    ASSERT_EQ(std::string_view(serialized->data(), serialized->size()), bytes);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<VideoFrameDescriptor> frame,
        VideoFrameDescriptor::Create("s3://bucket/视频.mp4", /*offset=*/7, /*length=*/99,
                                     /*frame_index=*/42, /*moov_offset=*/0, /*moov_length=*/0));
    auto encoded = frame->Serialize(GetDefaultPool());
    ASSERT_EQ(std::string_view(encoded->data(), encoded->size()), bytes);
    ASSERT_NOK(BlobDescriptor::Deserialize(bytes.data(), bytes.size()));
    ASSERT_NOK(Blob::FromDescriptor(bytes.data(), 5));
    ASSERT_NOK(Blob::FromDescriptor(bytes.data(), bytes.size() - 1));
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

TEST(VideoFileBatchReaderTest, RejectsLegacyFiveIndexLayout) {
    ASSERT_OK_AND_ASSIGN(std::string fixture, ReadJavaVideoFixture("video-v1-five-index.hex"));
    auto input = std::make_shared<ByteArrayInputStream>(fixture.data(), fixture.size());
    ASSERT_NOK_WITH_MSG(VideoFileBatchReader::Create(input, 2, GetDefaultPool(), false, nullptr),
                        "Legacy five-index .video files require reimport");

    // The old payload is nine bytes. Its trailing index bytes look like a valid first index
    // length of one in a six-index footer. At the guessed index start, 0x08 decodes to a
    // four-byte video that exactly covers the guessed payload. Only the old layout is valid.
    constexpr char old_file[] =
        "abcd\x08"
        "efgh"
        "\x12\x00"               // Video length 9, no keyframe index.
        "\x02\x00\x00\x00\x00"   // Five runs of one row.
        "\x00\x01\x02\x01\x02"   // References 0, NULL, 0, NULL, 0.
        "\x02\x01\x00\x00\x00";  // First frames 1, 0, 0, 0, 0.
    std::string bytes(old_file, sizeof(old_file) - 1);
    AppendVideoFooter(&bytes, {1, 1, 5, 5, 5});
    input = std::make_shared<ByteArrayInputStream>(bytes.data(), bytes.size());
    ASSERT_NOK_WITH_MSG(VideoFileBatchReader::Create(input, 2, GetDefaultPool(), false, nullptr),
                        "Legacy five-index .video files require reimport");
}

TEST(VideoFileBatchReaderTest, RejectsAmbiguousVideoLayout) {
    // Five indexes describe two 25-byte videos, no keyframe indexes and four
    // one-row runs. The same bytes also encode two 22-byte videos with no moov
    // ranges and four 25-row runs when read using the six-index footer.
    std::string bytes(50, '\0');
    bytes[44] = 44;  // Six-index video lengths: 22, 22.
    constexpr char indexes[] =
        "\x32\x00"           // Legacy video lengths: 25, 25.
        "\x00\x00"           // Legacy keyframe lengths: 0, 0.
        "\x02\x00\x00\x00"   // Legacy run lengths: 1, 1, 1, 1.
        "\x00\x02\x01\x02"   // Legacy references: 0, 1, 0, 1.
        "\x02\x00\x00\x00";  // Legacy first frames: 1, 1, 1, 1.
    bytes.append(indexes, sizeof(indexes) - 1);
    AppendVideoFooter(&bytes, {2, 2, 4, 4, 4});
    auto input = std::make_shared<ByteArrayInputStream>(bytes.data(), bytes.size());
    ASSERT_NOK_WITH_MSG(VideoFileBatchReader::Create(input, 2, GetDefaultPool(), false, nullptr),
                        "ambiguous v1 .video layout");
}

TEST(VideoFileBatchReaderTest, ReadsDenseSelectionInSmallBatches) {
    constexpr int32_t kRows = 512 * 1024 * 1024;
    const auto runs = DeltaVarintCompressor::Compress({kRows});
    std::string bytes(runs.data(), runs.size());
    bytes.append("\x01\x00", 2);  // One NULL reference and first frame 0.
    AppendVideoFooter(&bytes, {0, 0, 0, static_cast<uint32_t>(runs.size()), 1, 1});
    auto input = std::make_shared<ByteArrayInputStream>(bytes.data(), bytes.size());
    auto context = std::make_shared<BlobReadContext>();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<VideoFileBatchReader> reader,
                         VideoFileBatchReader::Create(input, 4, GetDefaultPool(), false, context));
    auto schema = arrow::schema({BlobUtils::ToArrowField("video", true)});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    std::optional<RoaringBitmap32> bitmap(std::in_place);
    bitmap->AddRange(1, kRows);
    bitmap->RemoveRange(2, 5);
    ASSERT_OK(reader->SetReadSchema(&c_schema, nullptr, bitmap));
    bitmap.reset();

    context->request = BlobBatchRequest{2, /*read_payload=*/false};
    ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatch());
    ASSERT_EQ(batch.first->length, 2);
    ASSERT_EQ(reader->GetPreviousBatchFileRowId(0).value(), 1);
    ASSERT_EQ(reader->GetPreviousBatchFileRowId(1).value(), 5);
    ArrowArrayRelease(batch.first.get());
    ArrowSchemaRelease(batch.second.get());

    context->request = BlobBatchRequest{3, /*read_payload=*/true};
    ASSERT_OK_AND_ASSIGN(batch, reader->NextBatch());
    ASSERT_EQ(batch.first->length, 3);
    ASSERT_EQ(reader->GetPreviousBatchFileRowId(2).value(), 8);
    ASSERT_EQ(reader->GetPreviousBatchFileRowId(0).value(), 6);
    ASSERT_NOK(reader->GetPreviousBatchFileRowId(3));
    ArrowArrayRelease(batch.first.get());
    ArrowSchemaRelease(batch.second.get());

    for (int32_t invalid : {kRows, -1}) {
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_NOK_WITH_MSG(
            reader->SetReadSchema(&c_schema, nullptr, RoaringBitmap32::From({1, invalid})),
            "video selection row exceeds row count");
    }
}

TEST(VideoFileBatchReaderTest, RejectsOversizedIndexesBeforeReadingThem) {
    class FooterOnlyStream : public MockInputStream {
     public:
        explicit FooterOnlyStream(const std::vector<uint32_t>& index_lengths)
            : footer_offset_(
                  std::accumulate(index_lengths.begin(), index_lengths.end(), int64_t{0})) {
            AppendVideoFooter(&footer_, index_lengths);
        }

        Result<int64_t> Length() const override {
            return footer_offset_ + footer_.size();
        }

        Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
            if (offset != footer_offset_ || size != static_cast<int64_t>(footer_.size())) {
                return Status::IOError("unexpected index read");
            }
            std::memcpy(buffer, footer_.data(), footer_.size());
            return size;
        }

     private:
        int64_t footer_offset_;
        std::string footer_;
    };

    constexpr uint32_t kMiB = 1024 * 1024;
    // A single 64 MiB index exceeds the decode budget. Three 1.5 MiB run indexes fit
    // during decoding but exceed the budget once the runs and decoded indexes coexist.
    constexpr uint32_t kRunIndexBytes = 3 * kMiB / 2;
    for (const std::vector<uint32_t>& lengths :
         {std::vector<uint32_t>{64 * kMiB, 0, 0, 0, 0, 0},
          std::vector<uint32_t>{0, 0, 0, kRunIndexBytes, kRunIndexBytes, kRunIndexBytes}}) {
        auto result = VideoFileBatchReader::Create(std::make_shared<FooterOnlyStream>(lengths), 2,
                                                   GetDefaultPool(), false, nullptr);
        ASSERT_TRUE(result.status().IsCapacityError()) << result.status();
    }
}

TEST(VideoFileBatchReaderTest, ReadsLargeIndexWithinMemoryBudget) {
    constexpr uint32_t kRuns = 1024 * 1024;
    // Each run contains one NULL row. The indexes encode deltas as single-byte varints.
    std::string bytes(3 * kRuns, '\0');
    bytes[0] = 2;      // Run length 1.
    bytes[kRuns] = 1;  // Video reference -1.
    AppendVideoFooter(&bytes, {0, 0, 0, kRuns, kRuns, kRuns});
    auto input = std::make_shared<ByteArrayInputStream>(bytes.data(), bytes.size());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<VideoFileBatchReader> reader,
                         VideoFileBatchReader::Create(input, 2, GetDefaultPool(), false, nullptr));
    ASSERT_OK_AND_ASSIGN(uint64_t rows, reader->GetNumberOfRows());
    ASSERT_EQ(rows, kRuns);
}

TEST(VideoFileBatchReaderTest, RejectsMoovRangeOutsideVideo) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = std::make_shared<LocalFileSystem>();
    const std::string path = dir->Str() + "/bad.video";
    ASSERT_OK(
        fs->WriteFile(path, MakeVideoFile(/*placeholder=*/false, /*first_moov_length=*/4), true));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, fs->Open(path));
    auto result = VideoFileBatchReader::Create(input, 2, GetDefaultPool(), false, nullptr);
    ASSERT_NOK_WITH_MSG(result, "corrupt or unsupported .video layout");
    ASSERT_NOK_WITH_MSG(result, "invalid moov range");
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

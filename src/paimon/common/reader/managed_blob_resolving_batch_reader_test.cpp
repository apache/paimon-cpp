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

#include "paimon/common/reader/managed_blob_resolving_batch_reader.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class ManagedBlobResolvingBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create("local");
        fields_ = {arrow::field("id", arrow::int32()),
                   BlobUtils::ToArrowField("b", /*nullable=*/true)};
    }

    void TearDown() override {
        dir_.reset();
    }

    class InMemoryBatchReader : public BatchReader {
     public:
        explicit InMemoryBatchReader(const std::shared_ptr<arrow::Array>& array) : array_(array) {}

        Result<ReadBatch> NextBatch() override {
            if (exhausted_) {
                return MakeEofBatch();
            }
            exhausted_ = true;
            auto c_array = std::make_unique<ArrowArray>();
            auto c_schema = std::make_unique<ArrowSchema>();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                arrow::ExportArray(*array_, c_array.get(), c_schema.get()));
            return std::make_pair(std::move(c_array), std::move(c_schema));
        }

        std::shared_ptr<Metrics> GetReaderMetrics() const override {
            return std::make_shared<MetricsImpl>();
        }

        void Close() override {}

     private:
        std::shared_ptr<arrow::Array> array_;
        bool exhausted_ = false;
    };

    /// Writes `payload` to a file and returns serialized descriptor bytes pointing at it.
    std::string WritePayloadAndMakeDescriptor(const std::string& file_name,
                                              const std::string& payload) {
        std::string path = PathUtil::JoinPath(dir_->Str(), file_name);
        auto out = dir_->GetFileSystem()->Create(path, /*overwrite=*/false).value();
        EXPECT_EQ(out->Write(payload.data(), payload.size()).value(),
                  static_cast<int64_t>(payload.size()));
        EXPECT_OK(out->Close());
        auto descriptor =
            BlobDescriptor::Create(path, /*offset=*/0, /*length=*/payload.size()).value();
        PAIMON_UNIQUE_PTR<Bytes> bytes = descriptor->Serialize(pool_);
        return std::string(bytes->data(), bytes->size());
    }

    std::shared_ptr<arrow::StructArray> BuildBatch(
        const std::vector<int32_t>& ids,
        const std::vector<std::optional<std::string>>& blob_values) {
        arrow::Int32Builder id_builder;
        arrow::LargeBinaryBuilder blob_builder;
        for (size_t i = 0; i < ids.size(); i++) {
            EXPECT_TRUE(id_builder.Append(ids[i]).ok());
            if (blob_values[i]) {
                EXPECT_TRUE(blob_builder.Append(blob_values[i].value()).ok());
            } else {
                EXPECT_TRUE(blob_builder.AppendNull().ok());
            }
        }
        std::shared_ptr<arrow::Array> id_array;
        std::shared_ptr<arrow::Array> blob_array;
        EXPECT_TRUE(id_builder.Finish(&id_array).ok());
        EXPECT_TRUE(blob_builder.Finish(&blob_array).ok());
        return arrow::StructArray::Make({id_array, blob_array}, fields_).ValueOrDie();
    }

    /// Reads the resolver's single batch back as a struct array.
    Result<std::shared_ptr<arrow::StructArray>> ResolveOneBatch(
        ManagedBlobResolvingBatchReader* reader) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            return Status::Invalid("unexpected eof");
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
        if (struct_array == nullptr) {
            return Status::Invalid("not a struct array");
        }
        return struct_array;
    }

    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> dir_;
    arrow::FieldVector fields_;
};

TEST_F(ManagedBlobResolvingBatchReaderTest, TestResolvesDescriptorsAndPreservesNulls) {
    std::string descriptor_bytes = WritePayloadAndMakeDescriptor("pack-1", "resolved-payload");
    auto batch = BuildBatch({1, 2}, {descriptor_bytes, std::nullopt});
    ManagedBlobResolvingBatchReader reader(std::make_unique<InMemoryBatchReader>(batch), {"b"},
                                           dir_->GetFileSystem(), pool_);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> resolved, ResolveOneBatch(&reader));
    auto blob_column =
        std::dynamic_pointer_cast<arrow::LargeBinaryArray>(resolved->GetFieldByName("b"));
    ASSERT_TRUE(blob_column != nullptr);
    ASSERT_EQ(blob_column->length(), 2);
    EXPECT_EQ(std::string(blob_column->GetView(0)), "resolved-payload");
    EXPECT_TRUE(blob_column->IsNull(1));

    // The resolution must not strip the blob extension metadata or the nullability off the
    // resolved field.
    auto resolved_field = resolved->struct_type()->GetFieldByName("b");
    ASSERT_TRUE(resolved_field != nullptr);
    EXPECT_TRUE(BlobUtils::IsBlobField(resolved_field));
    EXPECT_TRUE(resolved_field->nullable());

    // Untouched columns pass through.
    auto id_column = std::dynamic_pointer_cast<arrow::Int32Array>(resolved->GetFieldByName("id"));
    ASSERT_TRUE(id_column != nullptr);
    EXPECT_EQ(id_column->Value(0), 1);
    EXPECT_EQ(id_column->Value(1), 2);

    // End of input is handed through untouched rather than resolved.
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof, reader.NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof));
}

TEST_F(ManagedBlobResolvingBatchReaderTest, TestRejectsANonStructBatch) {
    // Every batch this reader wraps is a struct of columns; anything else has no managed blob
    // column to resolve and must not be handed on as if it had been resolved.
    arrow::Int32Builder id_builder;
    ASSERT_TRUE(id_builder.Append(1).ok());
    std::shared_ptr<arrow::Array> id_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());

    ManagedBlobResolvingBatchReader reader(std::make_unique<InMemoryBatchReader>(id_array), {"b"},
                                           dir_->GetFileSystem(), pool_);
    ASSERT_NOK_WITH_MSG(ResolveOneBatch(&reader).status(), "expects a StructArray batch");
}

TEST_F(ManagedBlobResolvingBatchReaderTest, TestRejectsAColumnThatIsNotLargeBinary) {
    // Descriptors are stored as large binary; a 32 bit binary column would be read through the
    // wrong accessors, so it is refused instead of silently misread.
    arrow::Int32Builder id_builder;
    arrow::BinaryBuilder blob_builder;
    ASSERT_TRUE(id_builder.Append(1).ok());
    ASSERT_TRUE(blob_builder.Append("not-large-binary").ok());
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> blob_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    ASSERT_TRUE(blob_builder.Finish(&blob_array).ok());
    arrow::FieldVector fields = {arrow::field("id", arrow::int32()),
                                 arrow::field("b", arrow::binary())};
    std::shared_ptr<arrow::StructArray> batch =
        arrow::StructArray::Make({id_array, blob_array}, fields).ValueOrDie();

    ManagedBlobResolvingBatchReader reader(std::make_unique<InMemoryBatchReader>(batch), {"b"},
                                           dir_->GetFileSystem(), pool_);
    ASSERT_NOK_WITH_MSG(ResolveOneBatch(&reader).status(), "to be a LargeBinaryArray");
}

TEST_F(ManagedBlobResolvingBatchReaderTest, TestFailsWhenTheReferencedPackIsMissing) {
    // A descriptor pointing at a pack that is not there must surface as an error; resolving
    // silently to nothing would hand the caller a row whose blob had been lost.
    std::unique_ptr<BlobDescriptor> descriptor =
        BlobDescriptor::Create(PathUtil::JoinPath(dir_->Str(), "absent.managed.blob"),
                               /*offset=*/0, /*length=*/4)
            .value();
    PAIMON_UNIQUE_PTR<Bytes> descriptor_bytes = descriptor->Serialize(pool_);
    auto batch = BuildBatch({1}, {std::string(descriptor_bytes->data(), descriptor_bytes->size())});

    ManagedBlobResolvingBatchReader reader(std::make_unique<InMemoryBatchReader>(batch), {"b"},
                                           dir_->GetFileSystem(), pool_);
    ASSERT_NOK(ResolveOneBatch(&reader).status());
}

TEST_F(ManagedBlobResolvingBatchReaderTest, TestRejectsNonDescriptorValue) {
    auto batch = BuildBatch({1}, {std::string("raw-bytes-not-a-descriptor")});
    ManagedBlobResolvingBatchReader reader(std::make_unique<InMemoryBatchReader>(batch), {"b"},
                                           dir_->GetFileSystem(), pool_);
    // The failure must come from descriptor deserialization, not from a later resolution step.
    ASSERT_NOK_WITH_MSG(ResolveOneBatch(&reader).status(), "BlobDescriptor");
}

TEST_F(ManagedBlobResolvingBatchReaderTest, TestPassThroughWithoutManagedFields) {
    auto batch = BuildBatch({1}, {std::string("raw-bytes-left-alone")});
    ManagedBlobResolvingBatchReader reader(std::make_unique<InMemoryBatchReader>(batch), {},
                                           dir_->GetFileSystem(), pool_);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> resolved, ResolveOneBatch(&reader));
    auto blob_column =
        std::dynamic_pointer_cast<arrow::LargeBinaryArray>(resolved->GetFieldByName("b"));
    ASSERT_TRUE(blob_column != nullptr);
    EXPECT_EQ(std::string(blob_column->GetView(0)), "raw-bytes-left-alone");
}

TEST_F(ManagedBlobResolvingBatchReaderTest, TestResolvesOnlyTheDeclaredColumns) {
    // A table may keep one blob column inline - a blob-descriptor or blob-view field - next to a
    // managed one, and only the managed columns are declared to this reader. The inline column
    // has to reach the caller exactly as it was stored: its descriptor bytes are what the caller
    // asked for, and resolving them here would hand back payload bytes instead.
    std::string managed_bytes = WritePayloadAndMakeDescriptor("pack-1", "managed-payload");
    std::string inline_bytes = WritePayloadAndMakeDescriptor("pack-2", "inline-payload");

    arrow::Int32Builder id_builder;
    arrow::LargeBinaryBuilder managed_builder;
    arrow::LargeBinaryBuilder inline_builder;
    ASSERT_TRUE(id_builder.Append(1).ok());
    ASSERT_TRUE(managed_builder.Append(managed_bytes).ok());
    ASSERT_TRUE(inline_builder.Append(inline_bytes).ok());
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> managed_array;
    std::shared_ptr<arrow::Array> inline_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    ASSERT_TRUE(managed_builder.Finish(&managed_array).ok());
    ASSERT_TRUE(inline_builder.Finish(&inline_array).ok());
    arrow::FieldVector fields = {arrow::field("id", arrow::int32()),
                                 BlobUtils::ToArrowField("b", /*nullable=*/true),
                                 BlobUtils::ToArrowField("d", /*nullable=*/true)};
    std::shared_ptr<arrow::StructArray> batch =
        arrow::StructArray::Make({id_array, managed_array, inline_array}, fields).ValueOrDie();

    ManagedBlobResolvingBatchReader reader(std::make_unique<InMemoryBatchReader>(batch), {"b"},
                                           dir_->GetFileSystem(), pool_);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> resolved, ResolveOneBatch(&reader));

    auto managed_column =
        std::dynamic_pointer_cast<arrow::LargeBinaryArray>(resolved->GetFieldByName("b"));
    ASSERT_TRUE(managed_column != nullptr);
    EXPECT_EQ(std::string(managed_column->GetView(0)), "managed-payload");

    auto inline_column =
        std::dynamic_pointer_cast<arrow::LargeBinaryArray>(resolved->GetFieldByName("d"));
    ASSERT_TRUE(inline_column != nullptr);
    EXPECT_EQ(std::string(inline_column->GetView(0)), inline_bytes);
}

}  // namespace paimon::test

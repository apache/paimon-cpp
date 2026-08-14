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

#include "paimon/core/io/managed_blob_reference_collector.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/managed_blob_reference_file.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class ManagedBlobReferenceCollectorTest : public testing::Test {
 protected:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create("local");
        // The key-value batch schema: special fields followed by the value fields.
        std::vector<DataField> write_fields = {SpecialFields::SequenceNumber(),
                                               SpecialFields::ValueKind()};
        write_fields.emplace_back(0, arrow::field("k", arrow::int32()));
        write_fields.emplace_back(1, BlobUtils::ToArrowField("b", /*nullable=*/true));
        write_schema_ = DataField::ConvertDataFieldsToArrowSchema(write_fields);
        data_file_path_ = PathUtil::JoinPath(dir_->Str(), "data-1.parquet");
    }

    void TearDown() override {
        dir_.reset();
    }

    std::string SerializedDescriptor(const std::string& uri) {
        auto descriptor = BlobDescriptor::Create(uri, /*offset=*/4, /*length=*/10).value();
        PAIMON_UNIQUE_PTR<Bytes> bytes = descriptor->Serialize(GetDefaultPool());
        return std::string(bytes->data(), bytes->size());
    }

    /// Builds a KeyValueBatch whose blob column holds the given cells; a nullopt cell is NULL.
    /// `value_kinds` are RowKind byte values, aligned with the rows.
    KeyValueBatch MakeBatch(const std::vector<int8_t>& value_kinds,
                            const std::vector<std::optional<std::string>>& blob_values) {
        arrow::Int64Builder sequence_builder;
        arrow::Int8Builder value_kind_builder;
        arrow::Int32Builder key_builder;
        arrow::LargeBinaryBuilder blob_builder;
        for (size_t i = 0; i < value_kinds.size(); i++) {
            EXPECT_TRUE(sequence_builder.Append(static_cast<int64_t>(i)).ok());
            EXPECT_TRUE(value_kind_builder.Append(value_kinds[i]).ok());
            EXPECT_TRUE(key_builder.Append(static_cast<int32_t>(i)).ok());
            if (blob_values[i]) {
                EXPECT_TRUE(blob_builder.Append(blob_values[i].value()).ok());
            } else {
                EXPECT_TRUE(blob_builder.AppendNull().ok());
            }
        }
        std::shared_ptr<arrow::Array> sequence_array;
        std::shared_ptr<arrow::Array> value_kind_array;
        std::shared_ptr<arrow::Array> key_array;
        std::shared_ptr<arrow::Array> blob_array;
        EXPECT_TRUE(sequence_builder.Finish(&sequence_array).ok());
        EXPECT_TRUE(value_kind_builder.Finish(&value_kind_array).ok());
        EXPECT_TRUE(key_builder.Finish(&key_array).ok());
        EXPECT_TRUE(blob_builder.Finish(&blob_array).ok());
        auto struct_array =
            arrow::StructArray::Make({sequence_array, value_kind_array, key_array, blob_array},
                                     write_schema_->fields())
                .ValueOrDie();
        KeyValueBatch batch;
        batch.batch = std::make_unique<ArrowArray>();
        EXPECT_TRUE(arrow::ExportArray(*struct_array, batch.batch.get()).ok());
        return batch;
    }

    std::unique_ptr<ManagedBlobReferenceCollector> CreateCollector() {
        return ManagedBlobReferenceCollector::Create(dir_->GetFileSystem(), data_file_path_,
                                                     write_schema_, {"b"})
            .value();
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<arrow::Schema> write_schema_;
    std::string data_file_path_;
};

TEST_F(ManagedBlobReferenceCollectorTest, TestCollectsManagedReferences) {
    auto collector = CreateCollector();
    // Row kinds: INSERT(0), INSERT, DELETE(3), UPDATE_BEFORE(1), INSERT, INSERT.
    KeyValueBatch batch =
        MakeBatch({0, 0, 3, 1, 0, 0},
                  {SerializedDescriptor("/warehouse/bucket-0/data-a.managed.blob"),
                   SerializedDescriptor("/other/bucket-1/data-b.managed.blob"),
                   SerializedDescriptor("/warehouse/bucket-0/data-retracted.managed.blob"),
                   SerializedDescriptor("/warehouse/bucket-0/data-retracted.managed.blob"),
                   SerializedDescriptor("/warehouse/bucket-0/data-c.blob"),  // not a managed pack
                   std::nullopt});
    ASSERT_OK(collector->Collect(&batch));
    // The batch stays writable after collecting: its arrow array is still exported.
    ASSERT_TRUE(batch.batch != nullptr);
    ASSERT_NE(batch.batch->release, nullptr);

    ASSERT_OK(collector->Close());
    ASSERT_OK_AND_ASSIGN(std::string result_file_name, collector->ResultFileName());
    ASSERT_EQ(result_file_name, "data-1.parquet.blobref");

    // Retract rows and non-managed URIs contribute nothing; cross-directory managed packs are
    // both recorded, sorted.
    ASSERT_OK_AND_ASSIGN(
        std::vector<ManagedBlobReferenceFile::Reference> references,
        ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), collector->SidecarPath()));
    ASSERT_EQ(references.size(), 2);
    EXPECT_EQ(references[0].ToString(), "/other/bucket-1/data-b.managed.blob");
    EXPECT_EQ(references[1].ToString(), "/warehouse/bucket-0/data-a.managed.blob");
}

TEST_F(ManagedBlobReferenceCollectorTest, TestCollectsOnlyDeclaredColumns) {
    // The sidecar must be built from the declared managed columns, not from the shape of the
    // values: an inline blob column is skipped even when its descriptor happens to point at a
    // path that looks exactly like a managed pack.
    std::vector<DataField> write_fields = {SpecialFields::SequenceNumber(),
                                           SpecialFields::ValueKind()};
    write_fields.emplace_back(0, arrow::field("k", arrow::int32()));
    write_fields.emplace_back(1, BlobUtils::ToArrowField("b", /*nullable=*/true));
    write_fields.emplace_back(2, BlobUtils::ToArrowField("d", /*nullable=*/true));
    write_schema_ = DataField::ConvertDataFieldsToArrowSchema(write_fields);

    arrow::Int64Builder sequence_builder;
    arrow::Int8Builder value_kind_builder;
    arrow::Int32Builder key_builder;
    arrow::LargeBinaryBuilder managed_builder;
    arrow::LargeBinaryBuilder inline_builder;
    ASSERT_TRUE(sequence_builder.Append(0).ok());
    ASSERT_TRUE(value_kind_builder.Append(0).ok());
    ASSERT_TRUE(key_builder.Append(0).ok());
    ASSERT_TRUE(
        managed_builder.Append(SerializedDescriptor("/warehouse/bucket-0/data-a.managed.blob"))
            .ok());
    ASSERT_TRUE(
        inline_builder.Append(SerializedDescriptor("/warehouse/bucket-0/data-inline.managed.blob"))
            .ok());
    std::vector<std::shared_ptr<arrow::Array>> columns(5);
    ASSERT_TRUE(sequence_builder.Finish(&columns[0]).ok());
    ASSERT_TRUE(value_kind_builder.Finish(&columns[1]).ok());
    ASSERT_TRUE(key_builder.Finish(&columns[2]).ok());
    ASSERT_TRUE(managed_builder.Finish(&columns[3]).ok());
    ASSERT_TRUE(inline_builder.Finish(&columns[4]).ok());
    auto struct_array = arrow::StructArray::Make(columns, write_schema_->fields()).ValueOrDie();
    KeyValueBatch batch;
    batch.batch = std::make_unique<ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*struct_array, batch.batch.get()).ok());

    // Only "b" is declared managed; "d" is an inline blob column.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManagedBlobReferenceCollector> collector,
                         ManagedBlobReferenceCollector::Create(
                             dir_->GetFileSystem(), data_file_path_, write_schema_, {"b"}));
    ASSERT_OK(collector->Collect(&batch));
    ASSERT_OK(collector->Close());

    ASSERT_OK_AND_ASSIGN(
        std::vector<ManagedBlobReferenceFile::Reference> references,
        ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), collector->SidecarPath()));
    ASSERT_EQ(references.size(), 1);
    EXPECT_EQ(references[0].ToString(), "/warehouse/bucket-0/data-a.managed.blob");
}

TEST_F(ManagedBlobReferenceCollectorTest, TestNonDescriptorValuesIgnored) {
    auto collector = CreateCollector();
    // Inline payload bytes are not descriptors and carry their payload with the row.
    KeyValueBatch batch = MakeBatch({0}, {std::string("raw-inline-bytes")});
    ASSERT_OK(collector->Collect(&batch));
    ASSERT_OK(collector->Close());
    ASSERT_OK_AND_ASSIGN(
        std::vector<ManagedBlobReferenceFile::Reference> references,
        ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), collector->SidecarPath()));
    ASSERT_TRUE(references.empty());
}

TEST_F(ManagedBlobReferenceCollectorTest, TestEmptySidecarWrittenWithoutReferences) {
    auto collector = CreateCollector();
    ASSERT_OK(collector->Close());
    // The sidecar is written even with nothing to reference, so the data file always has the
    // extra file its meta declares.
    ASSERT_TRUE(dir_->GetFileSystem()->GetFileStatus(collector->SidecarPath()).ok());
    ASSERT_OK_AND_ASSIGN(
        std::vector<ManagedBlobReferenceFile::Reference> references,
        ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), collector->SidecarPath()));
    ASSERT_TRUE(references.empty());
}

TEST_F(ManagedBlobReferenceCollectorTest, TestAbortDeletesSidecar) {
    auto collector = CreateCollector();
    KeyValueBatch batch =
        MakeBatch({0}, {SerializedDescriptor("/warehouse/bucket-0/data-a.managed.blob")});
    ASSERT_OK(collector->Collect(&batch));
    ASSERT_OK(collector->Close());
    ASSERT_TRUE(dir_->GetFileSystem()->GetFileStatus(collector->SidecarPath()).ok());

    collector->Abort();
    ASSERT_FALSE(dir_->GetFileSystem()->GetFileStatus(collector->SidecarPath()).ok());
}

TEST_F(ManagedBlobReferenceCollectorTest, TestCollectAfterCloseRejected) {
    // The sidecar is already written at Close, so a later batch could never reach it.
    auto collector = CreateCollector();
    ASSERT_OK(collector->Close());
    KeyValueBatch batch =
        MakeBatch({0}, {SerializedDescriptor("/warehouse/bucket-0/data-a.managed.blob")});
    ASSERT_NOK_WITH_MSG(collector->Collect(&batch), "already closed");
}

TEST_F(ManagedBlobReferenceCollectorTest, TestResultFileNameRequiresClose) {
    // The name is only meaningful once the sidecar it points at exists.
    auto collector = CreateCollector();
    ASSERT_NOK_WITH_MSG(collector->ResultFileName().status(),
                        "requires the collector to be closed");
}

TEST_F(ManagedBlobReferenceCollectorTest, TestCreateRejectsUnknownField) {
    ASSERT_NOK_WITH_MSG(ManagedBlobReferenceCollector::Create(
                            dir_->GetFileSystem(), data_file_path_, write_schema_, {"missing"})
                            .status(),
                        "is not part of the write schema");
}

}  // namespace paimon::test

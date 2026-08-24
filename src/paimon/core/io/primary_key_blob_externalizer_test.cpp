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

#include "paimon/core/io/primary_key_blob_externalizer.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/data/blob.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

/// Wraps a local output stream so a test can observe the explicit Close() call and inject
/// write failures at seal time. Only an explicit Close() is counted — the destructor would
/// release the file anyway, which is exactly what must not mask a dropped Close in
/// production code.
class CloseTrackingOutputStream : public OutputStream {
 public:
    CloseTrackingOutputStream(std::unique_ptr<OutputStream> inner, const bool* fail_writes,
                              int* close_count)
        : inner_(std::move(inner)), fail_writes_(fail_writes), close_count_(close_count) {}

    Result<int64_t> Write(const char* buffer, int64_t size) override {
        if (*fail_writes_) {
            return Status::IOError("injected write failure");
        }
        return inner_->Write(buffer, size);
    }

    Status Flush() override {
        return inner_->Flush();
    }

    Result<int64_t> GetPos() const override {
        return inner_->GetPos();
    }

    Result<std::string> GetUri() const override {
        return inner_->GetUri();
    }

    Status Close() override {
        (*close_count_)++;
        return inner_->Close();
    }

 private:
    std::unique_ptr<OutputStream> inner_;
    const bool* fail_writes_;
    int* close_count_;
};

/// A local file system whose output streams count explicit Close() calls and can start
/// failing writes on demand.
class CloseTrackingFileSystem : public LocalFileSystem {
 public:
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> inner,
                               LocalFileSystem::Create(path, overwrite));
        return std::make_unique<CloseTrackingOutputStream>(std::move(inner), &fail_writes_,
                                                           &close_count_);
    }

    mutable bool fail_writes_ = false;
    mutable int close_count_ = 0;
};

class PrimaryKeyBlobExternalizerTest : public testing::Test {
 protected:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create("local");
        value_schema_ = arrow::schema(
            {arrow::field("id", arrow::int32()), BlobUtils::ToArrowField("b", /*nullable=*/true)});
        path_factory_ = std::make_shared<DataFilePathFactory>();
        ASSERT_OK(path_factory_->Init(/*parent=*/dir_->Str(), /*format_identifier=*/"parquet",
                                      /*data_file_prefix=*/"data-",
                                      /*external_path_provider=*/nullptr));
    }

    void TearDown() override {
        dir_.reset();
    }

    CoreOptions CreateOptions(const std::string& blob_target_file_size) {
        return CoreOptions::FromMap({{Options::FILE_SYSTEM, "local"},
                                     {Options::BLOB_TARGET_FILE_SIZE, blob_target_file_size}})
            .value();
    }

    /// An externalizer over the fixture's (id, blob) schema. The result is null when the
    /// schema holds no managed blob field.
    Result<std::unique_ptr<PrimaryKeyBlobExternalizer>> MakeExternalizer(
        const std::string& blob_target_file_size) {
        CoreOptions options = CreateOptions(blob_target_file_size);
        return PrimaryKeyBlobExternalizer::Create(options, value_schema_, path_factory_,
                                                  GetDefaultPool());
    }

    /// Builds a two-column (id, blob) record batch; a nullopt blob value is a null cell.
    std::unique_ptr<RecordBatch> CreateBatch(
        const std::vector<int32_t>& ids, const std::vector<std::optional<std::string>>& blob_values,
        const std::vector<RecordBatch::RowKind>& row_kinds) {
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
        auto struct_array =
            arrow::StructArray::Make({id_array, blob_array}, value_schema_->fields()).ValueOrDie();
        ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*struct_array, &c_array).ok());
        return std::make_unique<RecordBatch>(std::map<std::string, std::string>(), /*bucket=*/0,
                                             row_kinds, &c_array);
    }

    /// Imports the batch and returns the blob column.
    std::shared_ptr<arrow::LargeBinaryArray> BlobColumn(const RecordBatch& batch) {
        auto arrow_array =
            arrow::ImportArray(batch.GetData(), arrow::struct_(value_schema_->fields()))
                .ValueOrDie();
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
        EXPECT_TRUE(struct_array != nullptr);
        if (struct_array == nullptr) {
            return nullptr;
        }
        return std::dynamic_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(1));
    }

    Result<std::string> ReadPayload(std::string_view descriptor_bytes, std::string* uri) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<Blob> blob,
            Blob::FromDescriptor(descriptor_bytes.data(), descriptor_bytes.size()));
        *uri = blob->Uri();
        PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> payload,
                               blob->ToData(dir_->GetFileSystem(), GetDefaultPool()));
        return std::string(payload->data(), payload->size());
    }

    bool FileExists(const std::string& path) {
        return dir_->GetFileSystem()->GetFileStatus(path).ok();
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
};

TEST_F(PrimaryKeyBlobExternalizerTest, TestCreateReturnsNullWithoutManagedFields) {
    auto schema_without_blob = arrow::schema({arrow::field("id", arrow::int32())});
    CoreOptions options = CreateOptions("1024");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<PrimaryKeyBlobExternalizer> externalizer,
                         PrimaryKeyBlobExternalizer::Create(options, schema_without_blob,
                                                            path_factory_, GetDefaultPool()));
    ASSERT_TRUE(externalizer == nullptr);
}

TEST_F(PrimaryKeyBlobExternalizerTest, TestCreateRejectsNonPositiveTargetFileSize) {
    // SchemaValidation guards catalog-created tables; the externalizer checks again because
    // pack rolling cannot work with a non-positive target size.
    ASSERT_NOK_WITH_MSG(MakeExternalizer("0").status(), "target file size must be positive");
}

TEST_F(PrimaryKeyBlobExternalizerTest, TestExternalizeRejectsRowKindCountMismatch) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<PrimaryKeyBlobExternalizer> externalizer,
                         MakeExternalizer("1024"));
    ASSERT_TRUE(externalizer != nullptr);

    // Two rows but one row kind: the externalizer cannot tell which rows retract.
    auto batch =
        CreateBatch({1, 2}, {std::string("a"), std::string("b")}, {RecordBatch::RowKind::INSERT});
    ASSERT_NOK_WITH_MSG(externalizer->Externalize(std::move(batch)).status(),
                        "row kinds do not match the row count");
}

TEST_F(PrimaryKeyBlobExternalizerTest, TestExternalizeReplacesPayloadWithDescriptor) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<PrimaryKeyBlobExternalizer> externalizer,
                         MakeExternalizer("1024"));
    ASSERT_TRUE(externalizer != nullptr);

    auto batch = CreateBatch(
        {1, 2, 3}, {std::string("hello"), std::nullopt, std::string("world")},
        {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT});
    ASSERT_OK_AND_ASSIGN(batch, externalizer->Externalize(std::move(batch)));

    auto blob_column = BlobColumn(*batch);
    ASSERT_TRUE(blob_column != nullptr);
    ASSERT_EQ(blob_column->length(), 3);
    ASSERT_TRUE(blob_column->IsNull(1));

    std::string uri0;
    std::string_view value0 = blob_column->GetView(0);
    ASSERT_OK_AND_ASSIGN(bool is_descriptor,
                         BlobDescriptor::IsBlobDescriptor(value0.data(), value0.size()));
    ASSERT_TRUE(is_descriptor);
    ASSERT_OK_AND_ASSIGN(std::string payload0, ReadPayload(value0, &uri0));
    EXPECT_EQ(payload0, "hello");

    std::string uri2;
    ASSERT_OK_AND_ASSIGN(std::string payload2, ReadPayload(blob_column->GetView(2), &uri2));
    EXPECT_EQ(payload2, "world");
    // Both payloads fit into one pack below the target size.
    EXPECT_EQ(uri0, uri2);
    EXPECT_TRUE(FileExists(uri0));

    // Packs written before PrepareCommit are uncommitted: Abort removes them.
    externalizer->Abort();
    EXPECT_FALSE(FileExists(uri0));
}

TEST_F(PrimaryKeyBlobExternalizerTest, TestEachManagedFieldOwnsItsPack) {
    // Every managed blob column owns a pack writer, so two columns never share a pack even
    // when their values are written in the same batch.
    value_schema_ = arrow::schema({arrow::field("id", arrow::int32()),
                                   BlobUtils::ToArrowField("b1", /*nullable=*/true),
                                   BlobUtils::ToArrowField("b2", /*nullable=*/true)});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<PrimaryKeyBlobExternalizer> externalizer,
                         MakeExternalizer("1024"));
    ASSERT_TRUE(externalizer != nullptr);

    arrow::Int32Builder id_builder;
    arrow::LargeBinaryBuilder b1_builder;
    arrow::LargeBinaryBuilder b2_builder;
    ASSERT_TRUE(id_builder.Append(1).ok());
    ASSERT_TRUE(id_builder.Append(2).ok());
    ASSERT_TRUE(b1_builder.Append("first-column").ok());
    ASSERT_TRUE(b1_builder.AppendNull().ok());
    ASSERT_TRUE(b2_builder.Append("second-column").ok());
    ASSERT_TRUE(b2_builder.Append("second-column-row-2").ok());
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> b1_array;
    std::shared_ptr<arrow::Array> b2_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    ASSERT_TRUE(b1_builder.Finish(&b1_array).ok());
    ASSERT_TRUE(b2_builder.Finish(&b2_array).ok());
    auto struct_array =
        arrow::StructArray::Make({id_array, b1_array, b2_array}, value_schema_->fields())
            .ValueOrDie();
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*struct_array, &c_array).ok());
    auto batch = std::make_unique<RecordBatch>(std::map<std::string, std::string>(), /*bucket=*/0,
                                               std::vector<RecordBatch::RowKind>(), &c_array);
    ASSERT_OK_AND_ASSIGN(batch, externalizer->Externalize(std::move(batch)));
    ASSERT_OK(externalizer->PrepareCommit());

    auto resolved =
        arrow::ImportArray(batch->GetData(), arrow::struct_(value_schema_->fields())).ValueOrDie();
    auto resolved_struct = std::dynamic_pointer_cast<arrow::StructArray>(resolved);
    ASSERT_TRUE(resolved_struct != nullptr);
    auto b1_column = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(resolved_struct->field(1));
    auto b2_column = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(resolved_struct->field(2));
    ASSERT_TRUE(b1_column != nullptr);
    ASSERT_TRUE(b2_column != nullptr);
    // The null cell of the first column stays null and consumes no pack space.
    EXPECT_TRUE(b1_column->IsNull(1));

    std::string b1_uri;
    std::string b2_uri;
    ASSERT_OK_AND_ASSIGN(std::string b1_payload, ReadPayload(b1_column->GetView(0), &b1_uri));
    ASSERT_OK_AND_ASSIGN(std::string b2_payload, ReadPayload(b2_column->GetView(0), &b2_uri));
    EXPECT_EQ(b1_payload, "first-column");
    EXPECT_EQ(b2_payload, "second-column");
    EXPECT_NE(b1_uri, b2_uri);
    EXPECT_TRUE(FileExists(b1_uri));
    EXPECT_TRUE(FileExists(b2_uri));

    // The second column's other row resolves out of that column's own pack.
    std::string b2_row2_uri;
    ASSERT_OK_AND_ASSIGN(std::string b2_row2_payload,
                         ReadPayload(b2_column->GetView(1), &b2_row2_uri));
    EXPECT_EQ(b2_row2_payload, "second-column-row-2");
    EXPECT_EQ(b2_row2_uri, b2_uri);
    EXPECT_NE(b2_row2_uri, b1_uri);
}

TEST_F(PrimaryKeyBlobExternalizerTest, TestRetractRowsDropPayload) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<PrimaryKeyBlobExternalizer> externalizer,
                         MakeExternalizer("1024"));
    ASSERT_TRUE(externalizer != nullptr);

    auto batch = CreateBatch(
        {1, 2, 3}, {std::string("kept"), std::string("deleted"), std::string("update-before")},
        {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::DELETE,
         RecordBatch::RowKind::UPDATE_BEFORE});
    ASSERT_OK_AND_ASSIGN(batch, externalizer->Externalize(std::move(batch)));

    auto blob_column = BlobColumn(*batch);
    ASSERT_TRUE(blob_column != nullptr);
    EXPECT_FALSE(blob_column->IsNull(0));
    EXPECT_TRUE(blob_column->IsNull(1));
    EXPECT_TRUE(blob_column->IsNull(2));

    std::string uri;
    ASSERT_OK_AND_ASSIGN(std::string payload, ReadPayload(blob_column->GetView(0), &uri));
    EXPECT_EQ(payload, "kept");
}

TEST_F(PrimaryKeyBlobExternalizerTest, TestRetractOnlyBatchOpensNoPack) {
    // A batch holding nothing but retracts must not even open a pack: the previous test keeps
    // an INSERT alongside them, so it cannot tell "no pack written" from "pack written and
    // its descriptor dropped", which would leak storage and inflate the reference set.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<PrimaryKeyBlobExternalizer> externalizer,
                         MakeExternalizer("1024"));
    ASSERT_TRUE(externalizer != nullptr);

    auto batch = CreateBatch({1, 2}, {std::string("deleted"), std::string("update-before")},
                             {RecordBatch::RowKind::DELETE, RecordBatch::RowKind::UPDATE_BEFORE});
    ASSERT_OK_AND_ASSIGN(batch, externalizer->Externalize(std::move(batch)));

    auto blob_column = BlobColumn(*batch);
    ASSERT_TRUE(blob_column != nullptr);
    EXPECT_TRUE(blob_column->IsNull(0));
    EXPECT_TRUE(blob_column->IsNull(1));

    std::vector<BasicFileStatus> statuses;
    ASSERT_OK(dir_->GetFileSystem()->ListDir(dir_->Str(), &statuses));
    EXPECT_TRUE(statuses.empty());

    // Sealing and aborting an externalizer that never opened a pack stay no-ops.
    ASSERT_OK(externalizer->PrepareCommit());
    externalizer->Abort();
}

TEST_F(PrimaryKeyBlobExternalizerTest, TestPrepareCommitHandsOverPacks) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<PrimaryKeyBlobExternalizer> externalizer,
                         MakeExternalizer("1024"));
    ASSERT_TRUE(externalizer != nullptr);

    auto batch = CreateBatch({1}, {std::string("payload")}, {RecordBatch::RowKind::INSERT});
    ASSERT_OK_AND_ASSIGN(batch, externalizer->Externalize(std::move(batch)));
    auto blob_column = BlobColumn(*batch);
    ASSERT_TRUE(blob_column != nullptr);
    std::string uri;
    ASSERT_OK_AND_ASSIGN(std::string payload, ReadPayload(blob_column->GetView(0), &uri));
    EXPECT_EQ(payload, "payload");

    // The hand-over names the packs it gives up, which is what a rollback of the resulting
    // commit deletes; deriving that from the data file's references instead would also catch
    // packs the writer never created.
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> handed_over, externalizer->PrepareCommit());
    ASSERT_EQ(handed_over, std::vector<std::string>({uri}));
    // A handed-over pack survives both a later abort and the writer teardown, and is not
    // handed over twice.
    externalizer->Abort();
    EXPECT_TRUE(FileExists(uri));
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> nothing_left, externalizer->PrepareCommit());
    EXPECT_TRUE(nothing_left.empty());
}

TEST_F(PrimaryKeyBlobExternalizerTest, TestSealFailureStillClosesPackStream) {
    // A failing footer write must still close the pack stream exactly once. The wrapper counts
    // only explicit Close() calls, so destructor cleanup cannot mask a Close dropped from
    // CloseCurrent.
    auto fs = std::make_shared<CloseTrackingFileSystem>();
    ASSERT_OK_AND_ASSIGN(
        CoreOptions options,
        CoreOptions::FromMap(
            {{Options::FILE_SYSTEM, "local"}, {Options::BLOB_TARGET_FILE_SIZE, "1024"}}, fs));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<PrimaryKeyBlobExternalizer> externalizer,
                         PrimaryKeyBlobExternalizer::Create(options, value_schema_, path_factory_,
                                                            GetDefaultPool()));
    ASSERT_TRUE(externalizer != nullptr);

    std::unique_ptr<RecordBatch> batch =
        CreateBatch({1}, {std::string("payload")}, /*row_kinds=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> externalized,
                         externalizer->Externalize(std::move(batch)));
    ASSERT_TRUE(externalized != nullptr);
    // The pack is below the target size and stays open after the externalization.
    ASSERT_EQ(fs->close_count_, 0);

    // Every write fails from here on, so sealing the pack fails at the footer write.
    fs->fail_writes_ = true;
    Result<std::vector<std::string>> seal_result = externalizer->PrepareCommit();
    ASSERT_FALSE(seal_result.ok());
    // The failed Finish() must not leak the stream: CloseCurrent closed it exactly once, and
    // the abort path must not close it a second time.
    ASSERT_EQ(fs->close_count_, 1);
}

TEST_F(PrimaryKeyBlobExternalizerTest, TestIOException) {
    // IO-failure sweep in the repository's TestIOException convention: whichever IO
    // operation of the externalize-and-seal cycle fails — including the footer write of
    // PrepareCommit's seal — the failure must surface as the injected error without a crash
    // or hang. Once no injection point is hit, the run completes and the sealed pack
    // resolves. The explicit close guarantee is asserted separately above.
    auto io_hook = IOHook::GetInstance();
    bool run_complete = false;
    for (size_t i = 0; i < 100; i++) {
        auto dir = UniqueTestDirectory::Create("local");
        ASSERT_TRUE(dir);
        auto path_factory = std::make_shared<DataFilePathFactory>();
        ASSERT_OK(path_factory->Init(dir->Str(), "parquet", "data-", nullptr));
        CoreOptions options = CreateOptions("1024");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<PrimaryKeyBlobExternalizer> externalizer,
                             PrimaryKeyBlobExternalizer::Create(options, value_schema_,
                                                                path_factory, GetDefaultPool()));
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);

        std::unique_ptr<RecordBatch> batch =
            CreateBatch({1}, {std::string("seal-payload")}, /*row_kinds=*/{});
        Result<std::unique_ptr<RecordBatch>> externalized =
            externalizer->Externalize(std::move(batch));
        CHECK_HOOK_STATUS(externalized.status(), i);
        CHECK_HOOK_STATUS(externalizer->PrepareCommit().status(), i);

        // No injection point was hit: the sealed pack must resolve to the payload.
        io_hook->Clear();
        std::shared_ptr<arrow::LargeBinaryArray> blob_column = BlobColumn(*externalized.value());
        ASSERT_TRUE(blob_column != nullptr);
        std::string pack_uri;
        ASSERT_OK_AND_ASSIGN(std::string payload, ReadPayload(blob_column->GetView(0), &pack_uri));
        ASSERT_EQ(payload, "seal-payload");
        ASSERT_TRUE(FileExists(pack_uri));
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_F(PrimaryKeyBlobExternalizerTest, TestPacksRollByTargetFileSize) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<PrimaryKeyBlobExternalizer> externalizer,
                         MakeExternalizer("1"));
    ASSERT_TRUE(externalizer != nullptr);

    auto batch = CreateBatch({1, 2}, {std::string("first"), std::string("second")},
                             {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT});
    ASSERT_OK_AND_ASSIGN(batch, externalizer->Externalize(std::move(batch)));
    auto blob_column = BlobColumn(*batch);
    ASSERT_TRUE(blob_column != nullptr);

    std::string uri0;
    std::string uri1;
    ASSERT_OK_AND_ASSIGN(std::string payload0, ReadPayload(blob_column->GetView(0), &uri0));
    ASSERT_OK_AND_ASSIGN(std::string payload1, ReadPayload(blob_column->GetView(1), &uri1));
    EXPECT_EQ(payload0, "first");
    EXPECT_EQ(payload1, "second");
    // A one-byte target size seals the pack after every value.
    EXPECT_NE(uri0, uri1);

    ASSERT_OK(externalizer->PrepareCommit());
    EXPECT_TRUE(FileExists(uri0));
    EXPECT_TRUE(FileExists(uri1));
}

TEST_F(PrimaryKeyBlobExternalizerTest, TestInlineDescriptorFieldNotExternalized) {
    // An inline blob field (blob-descriptor-field) keeps its caller-provided bytes; only the
    // managed field is externalized.
    auto schema_with_inline = arrow::schema({arrow::field("id", arrow::int32()),
                                             BlobUtils::ToArrowField("b", /*nullable=*/true),
                                             BlobUtils::ToArrowField("d", /*nullable=*/true)});
    CoreOptions options = CoreOptions::FromMap({{Options::FILE_SYSTEM, "local"},
                                                {Options::BLOB_TARGET_FILE_SIZE, "1024"},
                                                {Options::BLOB_DESCRIPTOR_FIELD, "d"}})
                              .value();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<PrimaryKeyBlobExternalizer> externalizer,
                         PrimaryKeyBlobExternalizer::Create(options, schema_with_inline,
                                                            path_factory_, GetDefaultPool()));
    ASSERT_TRUE(externalizer != nullptr);

    const std::string inline_bytes = "caller-provided-inline-bytes";
    arrow::Int32Builder id_builder;
    arrow::LargeBinaryBuilder managed_builder;
    arrow::LargeBinaryBuilder inline_builder;
    ASSERT_TRUE(id_builder.Append(1).ok());
    ASSERT_TRUE(managed_builder.Append("managed-payload").ok());
    ASSERT_TRUE(inline_builder.Append(inline_bytes).ok());
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> managed_array;
    std::shared_ptr<arrow::Array> inline_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    ASSERT_TRUE(managed_builder.Finish(&managed_array).ok());
    ASSERT_TRUE(inline_builder.Finish(&inline_array).ok());
    auto struct_array = arrow::StructArray::Make({id_array, managed_array, inline_array},
                                                 schema_with_inline->fields())
                            .ValueOrDie();
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*struct_array, &c_array).ok());
    auto batch = std::make_unique<RecordBatch>(std::map<std::string, std::string>(), /*bucket=*/0,
                                               std::vector<RecordBatch::RowKind>(), &c_array);

    ASSERT_OK_AND_ASSIGN(batch, externalizer->Externalize(std::move(batch)));
    auto result_array =
        arrow::ImportArray(batch->GetData(), arrow::struct_(schema_with_inline->fields()))
            .ValueOrDie();
    auto result_struct = std::dynamic_pointer_cast<arrow::StructArray>(result_array);
    ASSERT_TRUE(result_struct != nullptr);

    auto managed_column =
        std::dynamic_pointer_cast<arrow::LargeBinaryArray>(result_struct->GetFieldByName("b"));
    ASSERT_TRUE(managed_column != nullptr);
    std::string uri;
    ASSERT_OK_AND_ASSIGN(std::string payload, ReadPayload(managed_column->GetView(0), &uri));
    EXPECT_EQ(payload, "managed-payload");

    auto inline_column =
        std::dynamic_pointer_cast<arrow::LargeBinaryArray>(result_struct->GetFieldByName("d"));
    ASSERT_TRUE(inline_column != nullptr);
    EXPECT_EQ(std::string(inline_column->GetView(0)), inline_bytes);
}

TEST_F(PrimaryKeyBlobExternalizerTest, TestRematerializesDescriptorInput) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<PrimaryKeyBlobExternalizer> externalizer,
                         MakeExternalizer("1024"));
    ASSERT_TRUE(externalizer != nullptr);

    // First write produces a descriptor pointing at a pack.
    auto batch = CreateBatch({1}, {std::string("original")}, {RecordBatch::RowKind::INSERT});
    ASSERT_OK_AND_ASSIGN(batch, externalizer->Externalize(std::move(batch)));
    auto blob_column = BlobColumn(*batch);
    ASSERT_TRUE(blob_column != nullptr);
    std::string source_uri;
    ASSERT_OK_AND_ASSIGN(std::string source_payload,
                         ReadPayload(blob_column->GetView(0), &source_uri));
    ASSERT_EQ(source_payload, "original");
    ASSERT_OK(externalizer->PrepareCommit());

    // Feeding that descriptor back in copies the payload into a fresh pack: the new
    // descriptor points elsewhere but resolves to the same bytes.
    std::string_view descriptor_bytes = blob_column->GetView(0);
    auto second_batch =
        CreateBatch({2}, {std::string(descriptor_bytes)}, {RecordBatch::RowKind::INSERT});
    ASSERT_OK_AND_ASSIGN(second_batch, externalizer->Externalize(std::move(second_batch)));
    auto second_column = BlobColumn(*second_batch);
    ASSERT_TRUE(second_column != nullptr);
    std::string second_uri;
    ASSERT_OK_AND_ASSIGN(std::string second_payload,
                         ReadPayload(second_column->GetView(0), &second_uri));
    EXPECT_EQ(second_payload, "original");
    EXPECT_NE(second_uri, source_uri);
}

}  // namespace paimon::test

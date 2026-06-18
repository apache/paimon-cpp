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


#include "paimon/file_store_write.h"

#include <map>
#include <optional>
#include <string>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/operation/key_value_file_store_write.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

TEST(FileStoreWriteTest, TestCreateWithInvalidInput) {
    auto dir = UniqueTestDirectory::Create();
    WriteContextBuilder builder(dir->Str(), "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> ctx1,
                         builder.WithMemoryPool(nullptr).Finish());
    ASSERT_NOK(FileStoreWrite::Create(std::move(ctx1)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> ctx2,
                         builder.WithExecutor(nullptr).Finish());
    ASSERT_NOK(FileStoreWrite::Create(std::move(ctx2)));
    ASSERT_NOK(FileStoreWrite::Create(/*context=*/nullptr));
}

TEST(FileStoreWriteTest, TestCreateAppendTable) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),    arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32())};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/true));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{"f0", "f3"}, /*primary_keys=*/{},
                                   /*options=*/{}, /*ignore_if_exists=*/false));
    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                        "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));
}

TEST(FileStoreWriteTest, TestCreateAppendTableWithInvalidBucket) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),    arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32())};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    std::map<std::string, std::string> options;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), options));
    ASSERT_OK(catalog->CreateDatabase("foo", options, /*ignore_if_exists=*/true));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{"f0", "f3"},
                                   /*primary_keys=*/{}, options, /*ignore_if_exists=*/false));
    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                        "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.AddOption(Options::BUCKET, "-2").Finish());
    ASSERT_NOK_WITH_MSG(FileStoreWrite::Create(std::move(write_context)),
                        "not support bucket -2 in append table");
}

TEST(FileStoreWriteTest, TestCreateAppendTableWithInvalidWriteType) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),    arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32())};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    std::map<std::string, std::string> options = {
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
    };
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), options));
    ASSERT_OK(catalog->CreateDatabase("foo", options, /*ignore_if_exists=*/true));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{},
                                   /*primary_keys=*/{}, options, /*ignore_if_exists=*/false));
    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                        "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.AddOption(Options::BUCKET, "-1")
                             .WithWriteSchema({"field_non_exist"})
                             .Finish());
    ASSERT_NOK_WITH_MSG(FileStoreWrite::Create(std::move(write_context)),
                        "write field field_non_exist does not exist in table schema");
}

TEST(FileStoreWriteTest, TestCreatePrimaryKeyTable) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),    arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32())};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/true));
    std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f1"}};
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{"f0"}, /*primary_keys=*/{"f0", "f1", "f4"},
                                   options, /*ignore_if_exists=*/false));
    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                        "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.AddOption(Options::BUCKET, "2").Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));

    auto fs = std::make_shared<LocalFileSystem>();
    SchemaManager schema_manager(fs, PathUtil::JoinPath(dir->Str(), "foo.db/bar"));
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.Latest());
    ASSERT_TRUE(table_schema);

    ASSERT_OK_AND_ASSIGN(auto trimmed_pk, table_schema.value()->TrimmedPrimaryKeys());
    auto key_value_file_store_write = dynamic_cast<KeyValueFileStoreWrite*>(file_store_write.get());
    ASSERT_TRUE(key_value_file_store_write);
}

TEST(FileStoreWriteTest, TestCreatePrimaryKeyTableWithInvalidBucket) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),    arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32())};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    std::map<std::string, std::string> options;
    options[Options::BUCKET] = "-1";
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), options));
    ASSERT_OK(catalog->CreateDatabase("foo", options, /*ignore_if_exists=*/true));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{"f0", "f3"},
                                   /*primary_keys=*/{"f1", "f4"}, options,
                                   /*ignore_if_exists=*/false));
    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                        "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_NOK_WITH_MSG(FileStoreWrite::Create(std::move(write_context)),
                        "not support bucket -1 in key value table");
}

}  // namespace paimon::test

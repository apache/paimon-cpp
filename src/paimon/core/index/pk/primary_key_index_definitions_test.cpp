/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/index/pk/primary_key_index_definitions.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/index/pk/primary_key_index_definition.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
/// Builds a primary-key table schema with fields id BIGINT (pk), price DOUBLE, age INT,
/// status STRING and emb FLOAT, merging the given options over a fixed bucket option.
Result<std::unique_ptr<TableSchema>> MakeSchema(std::map<std::string, std::string> options) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("id", arrow::int64(), /*nullable=*/false)),
        DataField(1, arrow::field("price", arrow::float64())),
        DataField(2, arrow::field("age", arrow::int32())),
        DataField(3, arrow::field("status", arrow::utf8())),
        DataField(4, arrow::field("emb", arrow::float32()))};
    options.emplace(Options::BUCKET, "1");
    return TableSchema::Create(/*schema_id=*/0, DataField::ConvertDataFieldsToArrowSchema(fields),
                               /*partition_keys=*/{}, /*primary_keys=*/{"id"}, options);
}
}  // namespace

TEST(PrimaryKeyIndexDefinitionsTest, NoIndexOptionsYieldsEmptyDefinitions) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema, MakeSchema({}));
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexDefinitions definitions,
                         PrimaryKeyIndexDefinitions::Create(*schema));
    ASSERT_TRUE(definitions.Definitions().empty());
    ASSERT_TRUE(definitions.ScalarDefinitions().empty());
}

TEST(PrimaryKeyIndexDefinitionsTest, ResolvesBTreeDefinitions) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema,
                         MakeSchema({{Options::PK_BTREE_INDEX_COLUMNS, "price,age"}}));
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexDefinitions definitions,
                         PrimaryKeyIndexDefinitions::Create(*schema));
    ASSERT_EQ(2, definitions.Definitions().size());
    const PrimaryKeyIndexDefinition& price = definitions.Definitions()[0];
    ASSERT_EQ("price", price.Column());
    ASSERT_EQ(1, price.FieldId());
    ASSERT_EQ("btree", price.IndexType());
    ASSERT_EQ(PrimaryKeyIndexDefinition::Family::BTREE, price.GetFamily());
    const PrimaryKeyIndexDefinition& age = definitions.Definitions()[1];
    ASSERT_EQ("age", age.Column());
    ASSERT_EQ(2, age.FieldId());
    ASSERT_EQ("btree", age.IndexType());
    ASSERT_EQ(PrimaryKeyIndexDefinition::Family::BTREE, age.GetFamily());
    ASSERT_EQ(2, definitions.ScalarDefinitions().size());
}

TEST(PrimaryKeyIndexDefinitionsTest, ResolvesBitmapDefinition) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema,
                         MakeSchema({{Options::PK_BITMAP_INDEX_COLUMNS, "status"}}));
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexDefinitions definitions,
                         PrimaryKeyIndexDefinitions::Create(*schema));
    ASSERT_EQ(1, definitions.Definitions().size());
    const PrimaryKeyIndexDefinition& status = definitions.Definitions()[0];
    ASSERT_EQ("status", status.Column());
    ASSERT_EQ(3, status.FieldId());
    ASSERT_EQ("bitmap", status.IndexType());
    ASSERT_EQ(PrimaryKeyIndexDefinition::Family::BITMAP, status.GetFamily());
    ASSERT_EQ(1, definitions.ScalarDefinitions().size());
}

TEST(PrimaryKeyIndexDefinitionsTest, IgnoresColumnAbsentFromSchema) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema,
                         MakeSchema({{Options::PK_BTREE_INDEX_COLUMNS, "not_in_schema"}}));
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexDefinitions definitions,
                         PrimaryKeyIndexDefinitions::Create(*schema));
    ASSERT_TRUE(definitions.Definitions().empty());
}

TEST(PrimaryKeyIndexDefinitionsTest, CoercesScalarJsonOptionValuesLikeJava) {
    // Java's parseJsonMap(..., String.class) accepts scalar JSON values and coerces them
    // to text, so numeric or boolean values written by a Java engine must stay readable.
    std::map<std::string, std::string> options = {
        {Options::PK_BTREE_INDEX_COLUMNS, "price"},
        {"fields.price.pk-btree.index.options",
         R"({"compression-level":3,"cache-enabled":true,"block-size":"64 kb"})"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema, MakeSchema(options));
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexDefinitions definitions,
                         PrimaryKeyIndexDefinitions::Create(*schema));
    const std::map<std::string, std::string>& resolved = definitions.Definitions()[0].Options();
    ASSERT_EQ("3", resolved.at("btree-index.compression-level"));
    ASSERT_EQ("true", resolved.at("btree-index.cache-enabled"));
    ASSERT_EQ("64 kb", resolved.at("btree-index.block-size"));

    // Null and nested values are rejected like in Java.
    std::map<std::string, std::string> null_options = {
        {Options::PK_BTREE_INDEX_COLUMNS, "price"},
        {"fields.price.pk-btree.index.options", R"({"block-size":null})"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> null_schema, MakeSchema(null_options));
    ASSERT_NOK(PrimaryKeyIndexDefinitions::Create(*null_schema));
    std::map<std::string, std::string> nested_options = {
        {Options::PK_BTREE_INDEX_COLUMNS, "price"},
        {"fields.price.pk-btree.index.options", R"({"block-size":{"v":"64 kb"}})"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> nested_schema, MakeSchema(nested_options));
    ASSERT_NOK(PrimaryKeyIndexDefinitions::Create(*nested_schema));
}

TEST(PrimaryKeyIndexDefinitionsTest, QualifiesFieldScopedJsonOptions) {
    std::map<std::string, std::string> options = {
        {Options::PK_BTREE_INDEX_COLUMNS, "price"},
        {"sorted-index.records-per-range", "4096"},
        {"fields.price.pk-btree.index.options",
         R"({"block-size":"64 kb","btree-index.cache-size":"32 mb","fields.foo.x":"y"})"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema, MakeSchema(options));
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexDefinitions definitions,
                         PrimaryKeyIndexDefinitions::Create(*schema));
    ASSERT_EQ(1, definitions.Definitions().size());
    const std::map<std::string, std::string>& resolved = definitions.Definitions()[0].Options();
    // Unqualified keys are prefixed with the algorithm prefix, qualified keys are kept as-is.
    ASSERT_EQ(1, resolved.count("btree-index.block-size"));
    ASSERT_EQ("64 kb", resolved.at("btree-index.block-size"));
    ASSERT_EQ(1, resolved.count("btree-index.cache-size"));
    ASSERT_EQ("32 mb", resolved.at("btree-index.cache-size"));
    ASSERT_EQ(1, resolved.count("fields.foo.x"));
    ASSERT_EQ("y", resolved.at("fields.foo.x"));
    // The per-range knob never leaks into the definition, other table options are retained.
    ASSERT_EQ(0, resolved.count("sorted-index.records-per-range"));
    ASSERT_EQ(1, resolved.count(Options::BUCKET));
    ASSERT_EQ("1", resolved.at(Options::BUCKET));
}

TEST(PrimaryKeyIndexDefinitionsTest, RejectsConflictingJsonOptionValue) {
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableSchema> schema,
        MakeSchema({{Options::PK_BTREE_INDEX_COLUMNS, "price"},
                    {"btree-index.block-size", "128 kb"},
                    {"fields.price.pk-btree.index.options", R"({"block-size":"64 kb"})"}}));
    ASSERT_NOK(PrimaryKeyIndexDefinitions::Create(*schema));
}

TEST(PrimaryKeyIndexDefinitionsTest, RejectsMalformedJsonOptions) {
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema,
                             MakeSchema({{Options::PK_BTREE_INDEX_COLUMNS, "price"},
                                         {"fields.price.pk-btree.index.options", "not-json"}}));
        ASSERT_NOK(PrimaryKeyIndexDefinitions::Create(*schema));
    }
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema,
                             MakeSchema({{Options::PK_BTREE_INDEX_COLUMNS, "price"},
                                         {"fields.price.pk-btree.index.options", R"({"":"v"})"}}));
        ASSERT_NOK(PrimaryKeyIndexDefinitions::Create(*schema));
    }
}

TEST(PrimaryKeyIndexDefinitionsTest, RejectsDuplicateColumnWithinFamily) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema,
                         MakeSchema({{Options::PK_BTREE_INDEX_COLUMNS, "price,price"}}));
    ASSERT_NOK(PrimaryKeyIndexDefinitions::Create(*schema));
}

TEST(PrimaryKeyIndexDefinitionsTest, RejectsColumnSharedAcrossFamilies) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema,
                         MakeSchema({{Options::PK_BTREE_INDEX_COLUMNS, "price"},
                                     {Options::PK_BITMAP_INDEX_COLUMNS, "price"}}));
    ASSERT_NOK(PrimaryKeyIndexDefinitions::Create(*schema));
}

TEST(PrimaryKeyIndexDefinitionsTest, ResolvesNonScalarFamiliesAndExcludesThemFromScalar) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema,
                         MakeSchema({{Options::PK_BTREE_INDEX_COLUMNS, "price"},
                                     {Options::PK_VECTOR_INDEX_COLUMNS, "emb"},
                                     {"fields.emb.pk-vector.index.type", "ivf-flat"},
                                     {Options::PK_FULL_TEXT_INDEX_COLUMNS, "status"}}));
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexDefinitions definitions,
                         PrimaryKeyIndexDefinitions::Create(*schema));
    ASSERT_EQ(3, definitions.Definitions().size());
    const PrimaryKeyIndexDefinition& full_text = definitions.Definitions()[1];
    ASSERT_EQ("status", full_text.Column());
    ASSERT_EQ("full-text", full_text.IndexType());
    ASSERT_EQ(PrimaryKeyIndexDefinition::Family::FULL_TEXT, full_text.GetFamily());
    const PrimaryKeyIndexDefinition& embedding = definitions.Definitions()[2];
    ASSERT_EQ("emb", embedding.Column());
    ASSERT_EQ(4, embedding.FieldId());
    ASSERT_EQ("ivf-flat", embedding.IndexType());
    ASSERT_EQ(PrimaryKeyIndexDefinition::Family::VECTOR, embedding.GetFamily());
    std::vector<PrimaryKeyIndexDefinition> scalar_definitions = definitions.ScalarDefinitions();
    ASSERT_EQ(1, scalar_definitions.size());
    ASSERT_EQ("price", scalar_definitions[0].Column());
}

}  // namespace paimon::test

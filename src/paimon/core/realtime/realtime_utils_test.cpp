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

#include "paimon/core/realtime/realtime_utils.h"

#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/defs.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(RealtimeUtilsTest, TestValidateBranch) {
    ASSERT_OK(RealtimeUtils::ValidateBranch(""));
    ASSERT_OK(RealtimeUtils::ValidateBranch(" "));
    ASSERT_OK(RealtimeUtils::ValidateBranch("main"));
    ASSERT_NOK_WITH_MSG(RealtimeUtils::ValidateBranch("dev"),
                        "real-time operations only support the main branch");
}

TEST(RealtimeUtilsTest, TestValidateOffsetField) {
    const std::shared_ptr<arrow::Field> offset_field =
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset());
    ASSERT_OK(RealtimeUtils::ValidateOffsetField(
        arrow::schema({arrow::field("id", arrow::int64()), offset_field})));

    ASSERT_NOK_WITH_MSG(RealtimeUtils::ValidateOffsetField(nullptr), "schema must not be null");
    ASSERT_NOK_WITH_MSG(
        RealtimeUtils::ValidateOffsetField(arrow::schema({arrow::field("id", arrow::int64())})),
        "requires non-null int64 _REALTIME_OFFSET");
    ASSERT_NOK_WITH_MSG(
        RealtimeUtils::ValidateOffsetField(arrow::schema({arrow::field(
            SpecialFields::RealtimeOffset().Name(), arrow::int32(), /*nullable=*/false)})),
        "requires non-null int64 _REALTIME_OFFSET");
    ASSERT_NOK_WITH_MSG(
        RealtimeUtils::ValidateOffsetField(arrow::schema({arrow::field(
            SpecialFields::RealtimeOffset().Name(), arrow::int64(), /*nullable=*/true)})),
        "requires non-null int64 _REALTIME_OFFSET");
}

TEST(RealtimeUtilsTest, TestGetDeduplicateKeyPosition) {
    const std::shared_ptr<arrow::Schema> schema = arrow::schema({
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()),
        arrow::field("id", arrow::int64()),
        arrow::field("value", arrow::utf8()),
    });
    ASSERT_OK_AND_ASSIGN(int32_t position,
                         RealtimeUtils::GetDeduplicateKeyPosition(schema, {"id"}));
    ASSERT_EQ(1, position);

    ASSERT_NOK_WITH_MSG(RealtimeUtils::GetDeduplicateKeyPosition(nullptr, {"id"}),
                        "schema must not be null");
    ASSERT_NOK_WITH_MSG(RealtimeUtils::GetDeduplicateKeyPosition(schema, {}),
                        "requires exactly one deduplicate key field");
    ASSERT_NOK_WITH_MSG(RealtimeUtils::GetDeduplicateKeyPosition(schema, {"id", "value"}),
                        "requires exactly one deduplicate key field");
    ASSERT_NOK_WITH_MSG(RealtimeUtils::GetDeduplicateKeyPosition(schema, {"missing"}),
                        "deduplicate key field does not exist");
}

TEST(RealtimeUtilsTest, TestValidateDeduplicateSchema) {
    const std::shared_ptr<arrow::Field> offset_field =
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset());
    ASSERT_OK(RealtimeUtils::ValidateDeduplicateSchema(
        arrow::schema({arrow::field("id", arrow::int64(), /*nullable=*/false), offset_field}),
        {"id"}));

    ASSERT_NOK_WITH_MSG(
        RealtimeUtils::ValidateDeduplicateSchema(
            arrow::schema({arrow::field("id", arrow::int64(), /*nullable=*/true), offset_field}),
            {"id"}),
        "deduplicate key field must be non-null");
    ASSERT_NOK_WITH_MSG(RealtimeUtils::ValidateDeduplicateSchema(arrow::schema({offset_field}),
                                                                 {offset_field->name()}),
                        "offset field cannot be used as the deduplicate key");
    ASSERT_NOK_WITH_MSG(
        RealtimeUtils::ValidateDeduplicateSchema(
            arrow::schema(
                {arrow::field("id", arrow::struct_({arrow::field("nested", arrow::int64())}),
                              /*nullable=*/false),
                 offset_field}),
            {"id"}),
        "deduplicate key field must not be nested");
}

TEST(RealtimeUtilsTest, TestValidateDeduplicateFileIndex) {
    ASSERT_OK_AND_ASSIGN(CoreOptions valid_options,
                         CoreOptions::FromMap({{"file-index.bitmap.columns",
                                                SpecialFields::RealtimeOffset().Name()}}));
    ASSERT_OK(RealtimeUtils::ValidateDeduplicateFileIndex(valid_options));

    for (const std::string& type : {"bsi", "range-bitmap"}) {
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{"file-index." + type + ".columns",
                                                    SpecialFields::RealtimeOffset().Name()}}));
        ASSERT_OK(RealtimeUtils::ValidateDeduplicateFileIndex(options));
    }
    ASSERT_OK_AND_ASSIGN(
        CoreOptions approximate_options,
        CoreOptions::FromMap({{"file-index.bloom-filter.columns", "_REALTIME_OFFSET"}}));
    ASSERT_NOK_WITH_MSG(RealtimeUtils::ValidateDeduplicateFileIndex(approximate_options),
                        "requires an index supporting exact BIGINT IN and NOT_IN row positions");

    ASSERT_OK_AND_ASSIGN(CoreOptions missing_index_options, CoreOptions::FromMap({}));
    ASSERT_NOK_WITH_MSG(RealtimeUtils::ValidateDeduplicateFileIndex(missing_index_options),
                        "requires an index supporting exact BIGINT IN and NOT_IN row positions on "
                        "_REALTIME_OFFSET");

    ASSERT_OK_AND_ASSIGN(CoreOptions wrong_column_options,
                         CoreOptions::FromMap({{"file-index.bitmap.columns", "id"}}));
    ASSERT_NOK_WITH_MSG(RealtimeUtils::ValidateDeduplicateFileIndex(wrong_column_options),
                        "requires an index supporting exact BIGINT IN and NOT_IN row positions on "
                        "_REALTIME_OFFSET");

    ASSERT_OK_AND_ASSIGN(
        CoreOptions disabled_read_options,
        CoreOptions::FromMap({{"file-index.bitmap.columns", SpecialFields::RealtimeOffset().Name()},
                              {Options::FILE_INDEX_READ_ENABLED, "false"}}));
    ASSERT_NOK_WITH_MSG(RealtimeUtils::ValidateDeduplicateFileIndex(disabled_read_options),
                        "requires 'file-index.read.enabled=true'");
}

}  // namespace paimon::test

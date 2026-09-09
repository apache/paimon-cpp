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

#include "paimon/core/options/table_type.h"

#include <map>
#include <string>

#include "gtest/gtest.h"
#include "paimon/defs.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(TableTypeTest, TestEveryNameReadsBackAsItsType) {
    // Every type paimon defines is named here, including the ones this library cannot open:
    // telling such a table from a managed one means knowing its name. `materialized-table` is the
    // one that matters most to get right, since it is the only type besides `table` that the
    // managed path accepts - a wrong name there would refuse every materialized table.
    const std::map<std::string, TableType> by_name = {
        {TableTypeDefine::kTable, TableType::TABLE},
        {TableTypeDefine::kFormatTable, TableType::FORMAT_TABLE},
        {TableTypeDefine::kMaterializedTable, TableType::MATERIALIZED_TABLE},
        {TableTypeDefine::kObjectTable, TableType::OBJECT_TABLE},
        {TableTypeDefine::kLanceTable, TableType::LANCE_TABLE},
        {TableTypeDefine::kIcebergTable, TableType::ICEBERG_TABLE}};
    for (const auto& [name, expected] : by_name) {
        SCOPED_TRACE(name);
        ASSERT_OK_AND_ASSIGN(TableType table_type,
                             TableTypeDefine::FromOptions({{Options::TYPE, name}}));
        ASSERT_EQ(table_type, expected);
    }
}

TEST(TableTypeTest, TestAnAbsentTypeIsAManagedTable) {
    // Almost every table ever written omits the option, so reading it as anything else would
    // refuse them all.
    ASSERT_OK_AND_ASSIGN(TableType table_type, TableTypeDefine::FromOptions({}));
    ASSERT_EQ(table_type, TableType::TABLE);
    ASSERT_OK_AND_ASSIGN(TableType with_others,
                         TableTypeDefine::FromOptions({{Options::FILE_FORMAT, "parquet"}}));
    ASSERT_EQ(with_others, TableType::TABLE);
}

TEST(TableTypeTest, TestATypeIsMatchedIgnoringCase) {
    for (const char* spelling : {"Format-Table", "FORMAT-TABLE", "format-TABLE"}) {
        SCOPED_TRACE(spelling);
        ASSERT_OK_AND_ASSIGN(TableType table_type,
                             TableTypeDefine::FromOptions({{Options::TYPE, spelling}}));
        ASSERT_EQ(table_type, TableType::FORMAT_TABLE);
    }
}

TEST(TableTypeTest, TestAValueNamingNoTypeIsRejected) {
    // Refused rather than read as a managed table: opening one as managed would look for
    // snapshots it never had. The value is quoted back as it was given.
    ASSERT_NOK_WITH_MSG(TableTypeDefine::FromOptions({{Options::TYPE, "nonesuch"}}),
                        "unknown table type: nonesuch");
    ASSERT_NOK_WITH_MSG(TableTypeDefine::FromOptions({{Options::TYPE, ""}}), "unknown table type");
}

TEST(TableTypeTest, TestIsFormatTableAnswersForTheFormatTypeAlone) {
    ASSERT_OK_AND_ASSIGN(bool is_format, TableTypeDefine::IsFormatTable(
                                             {{Options::TYPE, TableTypeDefine::kFormatTable}}));
    ASSERT_TRUE(is_format);
    for (const char* other : {TableTypeDefine::kTable, TableTypeDefine::kMaterializedTable,
                              TableTypeDefine::kObjectTable}) {
        SCOPED_TRACE(other);
        ASSERT_OK_AND_ASSIGN(bool answered,
                             TableTypeDefine::IsFormatTable({{Options::TYPE, other}}));
        ASSERT_FALSE(answered);
    }
    // The absent case goes through `FromOptions()` too, so it is a managed table here as well.
    ASSERT_OK_AND_ASSIGN(bool absent, TableTypeDefine::IsFormatTable({}));
    ASSERT_FALSE(absent);
}

}  // namespace paimon::test

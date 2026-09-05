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

#include "paimon/core/realtime/realtime_schema_layout.h"

#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(RealtimeSchemaLayoutTest, TestSchemaLayouts) {
    arrow::FieldVector value_fields = {arrow::field("key", arrow::int64(), false),
                                       arrow::field("value", arrow::utf8())};
    std::shared_ptr<arrow::Schema> user_schema = arrow::schema(value_fields);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RealtimeSchemaLayout> append_layout,
                         RealtimeSchemaLayout::Create(RealtimeStoreMode::APPEND_ONLY, user_schema));
    ASSERT_EQ((std::vector<std::string>{"_REALTIME_OFFSET", "key", "value"}),
              append_layout->InputSchema()->field_names());
    ASSERT_TRUE(append_layout->InputSchema()->Equals(*append_layout->StoreWriteSchema()));
    ASSERT_TRUE(append_layout->StoreWriteSchema()->Equals(*append_layout->StoreCommitSchema()));
    ASSERT_TRUE(user_schema->Equals(*append_layout->CommitSchema()));
    ASSERT_EQ((std::vector<std::string>{"_VALUE_KIND", "key", "value"}),
              append_layout->QuerySchema()->field_names());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RealtimeSchemaLayout> primary_key_layout,
                         RealtimeSchemaLayout::Create(RealtimeStoreMode::PRIMARY_KEY, user_schema));
    ASSERT_EQ(append_layout->InputSchema()->field_names(),
              primary_key_layout->InputSchema()->field_names());
    ASSERT_EQ((std::vector<std::string>{"_SEQUENCE_NUMBER", "_VALUE_KIND", "_REALTIME_OFFSET",
                                        "key", "value"}),
              primary_key_layout->StoreWriteSchema()->field_names());
    ASSERT_TRUE(
        primary_key_layout->StoreWriteSchema()->Equals(*primary_key_layout->StoreCommitSchema()));
    ASSERT_EQ((std::vector<std::string>{"_SEQUENCE_NUMBER", "_VALUE_KIND", "key", "value"}),
              primary_key_layout->CommitSchema()->field_names());
    ASSERT_TRUE(primary_key_layout->CommitSchema()->Equals(*primary_key_layout->QuerySchema()));
}

}  // namespace paimon::test

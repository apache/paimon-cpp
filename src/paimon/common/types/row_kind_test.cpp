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

#include "paimon/common/types/row_kind.h"

#include "gtest/gtest.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(RowKindTest, TestSimple) {
    ASSERT_OK_AND_ASSIGN(const RowKind* insert, RowKind::FromByteValue(0));
    ASSERT_OK_AND_ASSIGN(const RowKind* update_before, RowKind::FromByteValue(1));
    ASSERT_OK_AND_ASSIGN(const RowKind* update_after, RowKind::FromByteValue(2));
    ASSERT_OK_AND_ASSIGN(const RowKind* delete_kind, RowKind::FromByteValue(3));
    ASSERT_NOK(RowKind::FromByteValue(4));
    ASSERT_EQ(*insert, *insert);
    ASSERT_EQ(insert->ShortString(), "+I");
    ASSERT_EQ(update_before->ShortString(), "-U");
    ASSERT_EQ(update_after->ShortString(), "+U");
    ASSERT_EQ(delete_kind->ShortString(), "-D");
    ASSERT_EQ(insert->Name(), "INSERT");
    ASSERT_EQ(update_before->Name(), "UPDATE_BEFORE");
    ASSERT_EQ(update_after->Name(), "UPDATE_AFTER");
    ASSERT_EQ(delete_kind->Name(), "DELETE");
    ASSERT_FALSE(*insert == *delete_kind);
}

TEST(RowKindTest, TestFromShortStringIgnoresCase) {
    ASSERT_OK_AND_ASSIGN(const RowKind* insert, RowKind::FromShortString("+i"));
    ASSERT_OK_AND_ASSIGN(const RowKind* update_before, RowKind::FromShortString("-u"));
    ASSERT_OK_AND_ASSIGN(const RowKind* update_after, RowKind::FromShortString("+u"));
    ASSERT_OK_AND_ASSIGN(const RowKind* delete_kind, RowKind::FromShortString("-d"));

    ASSERT_EQ(insert, RowKind::Insert());
    ASSERT_EQ(update_before, RowKind::UpdateBefore());
    ASSERT_EQ(update_after, RowKind::UpdateAfter());
    ASSERT_EQ(delete_kind, RowKind::Delete());
}

}  // namespace paimon::test

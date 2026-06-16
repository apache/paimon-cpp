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
#include "paimon/core/manifest/file_kind.h"

#include <memory>

#include "gtest/gtest.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(FileKindTest, FromByteValueValidCases) {
    ASSERT_OK_AND_ASSIGN(auto add_result, FileKind::FromByteValue(0));
    ASSERT_EQ(add_result, FileKind::Add());

    ASSERT_OK_AND_ASSIGN(auto delete_result, FileKind::FromByteValue(1));
    ASSERT_EQ(delete_result, FileKind::Delete());
}

TEST(FileKindTest, FromByteValueInvalidCase) {
    ASSERT_NOK_WITH_MSG(FileKind::FromByteValue(2), "Unsupported byte value");
}

}  // namespace paimon::test

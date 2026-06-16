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
#include "paimon/core/manifest/file_source.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(FileSourceTest, TestSimple) {
    ASSERT_OK_AND_ASSIGN(FileSource append, FileSource::FromByteValue(0));
    ASSERT_OK_AND_ASSIGN(FileSource compact, FileSource::FromByteValue(1));
    ASSERT_NOK(FileSource::FromByteValue(2));
    ASSERT_EQ(append.ToString(), "APPEND");
    ASSERT_EQ(compact.ToString(), "COMPACT");
    ASSERT_NE(append, compact);
    ASSERT_EQ(append, append);
}

}  // namespace paimon::test

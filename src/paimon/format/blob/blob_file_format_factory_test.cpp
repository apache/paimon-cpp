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

#include "paimon/format/blob/blob_file_format_factory.h"

#include "gtest/gtest.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::blob::test {

TEST(BlobFileFormatFactoryTest, TestIdentifier) {
    BlobFileFormatFactory factory;
    ASSERT_EQ(std::string(factory.Identifier()), "blob");
    ASSERT_OK_AND_ASSIGN(auto file_format, factory.Create({}));
    ASSERT_EQ(file_format->Identifier(), "blob");
}

}  // namespace paimon::blob::test

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

#include "paimon/fs/jindo/jindo_file_system_factory.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::jindo::test {
TEST(JindoFileSystemFactoryTest, TestCreate) {
    auto fs_factory = std::make_unique<JindoFileSystemFactory>();
    ASSERT_EQ(fs_factory->Identifier(), std::string("jindo"));
    {
        // invalid options
        std::map<std::string, std::string> options;
        ASSERT_NOK_WITH_MSG(fs_factory->Create(paimon::test::GetJindoTestDir(), options),
                            "options must have 'fs.oss.user' key in JindoFileSystem");
    }
    {
        // options
        std::map<std::string, std::string> options = paimon::test::GetJindoTestOptions();
        ASSERT_OK_AND_ASSIGN(auto fs, fs_factory->Create(paimon::test::GetJindoTestDir(), options));
        ASSERT_TRUE(fs);
    }
}

}  // namespace paimon::jindo::test

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

#include "paimon/core/io/file_index_options.h"

#include <map>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/core/core_options.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

Result<FileIndexOptions> ParseOptions(const std::map<std::string, std::string>& index_options) {
    std::shared_ptr<LocalFileSystem> file_system = std::make_shared<LocalFileSystem>();
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(index_options, file_system));
    return FileIndexOptions::FromCoreOptions(core_options);
}

}  // namespace

TEST(FileIndexOptionsTest, TestRejectOverlappingPrefixAndSuffix) {
    ASSERT_NOK_WITH_MSG(ParseOptions({{"file-index.columns", "f0"}}),
                        "Invalid file index option file-index.columns");
}

TEST(FileIndexOptionsTest, TestNestedMapColumnSyntax) {
    ASSERT_OK_AND_ASSIGN(FileIndexOptions options,
                         ParseOptions({{"file-index.bitmap.columns", "col[key"}}));
    ASSERT_EQ(1, options.Definitions().size());
    ASSERT_EQ("col[key", options.Definitions()[0].column_name);

    ASSERT_NOK_WITH_MSG(ParseOptions({{"file-index.bitmap.columns", "col[key]"}}),
                        "nested map columns is not supported");
}

}  // namespace paimon::test

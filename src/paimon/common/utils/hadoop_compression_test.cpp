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

#include "paimon/common/utils/hadoop_compression.h"

#include <optional>
#include <string>

#include "gtest/gtest.h"

namespace paimon::test {

TEST(HadoopCompressionTest, TestEveryNameMapsToItsFileExtension) {
    // Each pair is fixed by what the rest of the paimon ecosystem writes and reads, not chosen
    // here: getting one wrong makes files written here unreadable elsewhere, and silently, since
    // the name is only ever appended to a file name. `zstd` spelled `zst` is the one that reads
    // like a typo and is not.
    struct Case {
        const char* name;
        HadoopCompression::Kind kind;
        const char* extension;
    };
    for (const Case& test_case : {Case{"gzip", HadoopCompression::Kind::GZIP, "gz"},
                                  Case{"bzip2", HadoopCompression::Kind::BZIP2, "bz2"},
                                  Case{"deflate", HadoopCompression::Kind::DEFLATE, "deflate"},
                                  Case{"snappy", HadoopCompression::Kind::SNAPPY, "snappy"},
                                  Case{"lz4", HadoopCompression::Kind::LZ4, "lz4"},
                                  Case{"zstd", HadoopCompression::Kind::ZSTD, "zst"}}) {
        SCOPED_TRACE(test_case.name);
        std::optional<HadoopCompression::Kind> kind = HadoopCompression::FromName(test_case.name);
        ASSERT_TRUE(kind.has_value());
        ASSERT_EQ(*kind, test_case.kind);
        ASSERT_EQ(HadoopCompression::ToFileExtension(test_case.kind), test_case.extension);
    }
}

TEST(HadoopCompressionTest, TestNoCompressionAddsNoExtension) {
    // An empty value and "none" both mean no compression, which adds nothing to a file name.
    for (const char* name : {"", "none", "NONE", "None"}) {
        SCOPED_TRACE(name);
        std::optional<HadoopCompression::Kind> kind = HadoopCompression::FromName(name);
        ASSERT_TRUE(kind.has_value());
        ASSERT_EQ(*kind, HadoopCompression::Kind::NONE);
    }
    ASSERT_TRUE(HadoopCompression::ToFileExtension(HadoopCompression::Kind::NONE).empty());
}

TEST(HadoopCompressionTest, TestANameIsMatchedIgnoringCase) {
    for (const char* name : {"ZSTD", "Zstd", "zStD"}) {
        SCOPED_TRACE(name);
        std::optional<HadoopCompression::Kind> kind = HadoopCompression::FromName(name);
        ASSERT_TRUE(kind.has_value());
        ASSERT_EQ(*kind, HadoopCompression::Kind::ZSTD);
    }
}

TEST(HadoopCompressionTest, TestAValueNamingNoCompressionIsNotOne) {
    // A caller writes such a value into the file name verbatim, so it must come back as nullopt
    // rather than as a compression whose extension would replace it. "uncompressed" is the one
    // that reads like it belongs here: hadoop does not name it, so it does not.
    for (const char* name : {"uncompressed", "zst", "gz", "brotli", "nonesuch"}) {
        SCOPED_TRACE(name);
        ASSERT_FALSE(HadoopCompression::FromName(name).has_value());
    }
}

}  // namespace paimon::test

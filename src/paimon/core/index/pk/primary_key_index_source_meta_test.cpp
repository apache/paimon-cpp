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

#include "paimon/core/index/pk/primary_key_index_source_meta.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/pk/primary_key_index_source_file.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class PrimaryKeyIndexSourceMetaTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    Result<std::string> SerializeToString(int32_t data_level,
                                          std::vector<PrimaryKeyIndexSourceFile> source_files) {
        PAIMON_ASSIGN_OR_RAISE(
            PrimaryKeyIndexSourceMeta meta,
            PrimaryKeyIndexSourceMeta::Create(data_level, std::move(source_files)));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, meta.Serialize(pool_.get()));
        return std::string(bytes->data(), bytes->size());
    }

    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(PrimaryKeyIndexSourceMetaTest, SerializeMatchesGoldenBytes) {
    std::vector<PrimaryKeyIndexSourceFile> files;
    files.emplace_back("a.parquet", 100);
    files.emplace_back("b.parquet", 200);
    ASSERT_OK_AND_ASSIGN(std::string serialized, SerializeToString(3, files));

    const uint8_t kExpected[] = {
        0x00, 0x00, 0x00, 0x01,  // version 1
        0x00, 0x00, 0x00, 0x03,  // data level 3
        0x00, 0x00, 0x00, 0x02,  // source file count 2
        0x00, 0x09,              // writeUTF byte length of "a.parquet"
        'a',  '.',  'p',  'a',  'r',  'q',  'u',  'e',  't',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64,  // row count 100
        0x00, 0x09,                                      // writeUTF byte length of "b.parquet"
        'b',  '.',  'p',  'a',  'r',  'q',  'u',  'e',  't',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC8,  // row count 200
    };
    std::string expected(reinterpret_cast<const char*>(kExpected), sizeof(kExpected));
    ASSERT_EQ(expected, serialized);

    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexSourceMeta meta, PrimaryKeyIndexSourceMeta::Deserialize(
                                                             serialized.data(), serialized.size()));
    ASSERT_EQ(3, meta.DataLevel());
    ASSERT_EQ(files, meta.SourceFiles());
}

TEST_F(PrimaryKeyIndexSourceMetaTest, RoundTripWithChineseNameAndLargeRowCount) {
    std::vector<PrimaryKeyIndexSourceFile> files;
    // Row count above 2^32 exercises the full big-endian int64 encoding.
    files.emplace_back("文件-0.parquet", (int64_t{1} << 40) + 7);
    files.emplace_back("data-1.parquet", 42);
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexSourceMeta meta,
                         PrimaryKeyIndexSourceMeta::Create(5, files));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, meta.Serialize(pool_.get()));
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexSourceMeta decoded,
                         PrimaryKeyIndexSourceMeta::Deserialize(bytes->data(), bytes->size()));
    ASSERT_EQ(5, decoded.DataLevel());
    ASSERT_EQ(files, decoded.SourceFiles());
}

TEST_F(PrimaryKeyIndexSourceMetaTest, DeserializeRejectsBadHeaders) {
    std::vector<PrimaryKeyIndexSourceFile> files;
    files.emplace_back("a.parquet", 100);
    ASSERT_OK_AND_ASSIGN(std::string valid, SerializeToString(3, files));
    // Layout: [0,4) version, [4,8) data level, [8,12) count, [12,14) name length,
    // [14,23) name bytes, [23,31) row count.
    ASSERT_EQ(static_cast<size_t>(31), valid.size());

    // Unsupported versions.
    std::string version_two = valid;
    version_two[3] = '\x02';
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::Deserialize(version_two.data(), version_two.size()));
    std::string version_zero = valid;
    version_zero[3] = '\x00';
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::Deserialize(version_zero.data(), version_zero.size()));

    // Source file count must be positive.
    std::string zero_count = valid;
    zero_count[11] = '\x00';
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::Deserialize(zero_count.data(), zero_count.size()));
    std::string negative_count = valid;
    for (size_t i = 8; i < 12; i++) {
        negative_count[i] = '\xFF';
    }
    ASSERT_NOK(
        PrimaryKeyIndexSourceMeta::Deserialize(negative_count.data(), negative_count.size()));

    // Claimed count 1000 exceeds the defensive cap allowed by the 19 remaining bytes.
    std::string huge_count = valid;
    huge_count[10] = '\x03';
    huge_count[11] = '\xE8';
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::Deserialize(huge_count.data(), huge_count.size()));
}

TEST_F(PrimaryKeyIndexSourceMetaTest, DeserializeRejectsBadPayloads) {
    std::vector<PrimaryKeyIndexSourceFile> files;
    files.emplace_back("a.parquet", 100);
    ASSERT_OK_AND_ASSIGN(std::string valid, SerializeToString(3, files));
    ASSERT_EQ(static_cast<size_t>(31), valid.size());

    // Trailing bytes after a valid payload.
    std::string trailing = valid + '\x00';
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::Deserialize(trailing.data(), trailing.size()));

    // Buffer cut in the middle of the file name: only 8 of the 9 name bytes remain.
    std::string cut_name = valid.substr(0, 22);
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::Deserialize(cut_name.data(), cut_name.size()));

    // Buffer cut in the middle of the row count: only 4 of the 8 bytes remain.
    std::string cut_row_count = valid.substr(0, 27);
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::Deserialize(cut_row_count.data(), cut_row_count.size()));

    std::string negative_row_count = valid;
    negative_row_count[23] = '\xFF';
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::Deserialize(negative_row_count.data(),
                                                      negative_row_count.size()));
}

TEST_F(PrimaryKeyIndexSourceMetaTest, CreateRejectsInvalidArguments) {
    std::vector<PrimaryKeyIndexSourceFile> files;
    files.emplace_back("a.parquet", 100);
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::Create(0, files));
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::Create(-1, files));
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::Create(3, {}));
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::Create(3, {{"a.parquet", -1}}));
}

TEST_F(PrimaryKeyIndexSourceMetaTest, FromIndexFileDecodesSourceMeta) {
    std::vector<PrimaryKeyIndexSourceFile> files;
    files.emplace_back("a.parquet", 100);
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexSourceMeta meta,
                         PrimaryKeyIndexSourceMeta::Create(7, files));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> source_meta, meta.Serialize(pool_.get()));
    std::shared_ptr<Bytes> index_meta = std::make_shared<Bytes>("index-payload", pool_.get());
    GlobalIndexMeta global_index_meta(/*_row_range_start=*/0, /*_row_range_end=*/100,
                                      /*_index_field_id=*/1, /*_extra_field_ids=*/std::nullopt,
                                      index_meta, source_meta);
    IndexFileMeta index_file("pk-btree", "index-file-0", /*file_size=*/64, /*row_count=*/100,
                             /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt,
                             global_index_meta);
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexSourceMeta decoded,
                         PrimaryKeyIndexSourceMeta::FromIndexFile(index_file));
    ASSERT_EQ(7, decoded.DataLevel());
    ASSERT_EQ(files, decoded.SourceFiles());
}

TEST_F(PrimaryKeyIndexSourceMetaTest, FromIndexFileRejectsMissingSourceMeta) {
    // Index file without any global index metadata.
    IndexFileMeta no_global_index("pk-btree", "index-file-1", /*file_size=*/64, /*row_count=*/100,
                                  /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt);
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::FromIndexFile(no_global_index));

    // Global index metadata whose source_meta is null.
    std::shared_ptr<Bytes> index_meta = std::make_shared<Bytes>("index-payload", pool_.get());
    GlobalIndexMeta null_source_meta(/*_row_range_start=*/0, /*_row_range_end=*/100,
                                     /*_index_field_id=*/1, /*_extra_field_ids=*/std::nullopt,
                                     index_meta, /*_source_meta=*/nullptr);
    IndexFileMeta no_source_meta("pk-btree", "index-file-2", /*file_size=*/64, /*row_count=*/100,
                                 /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt,
                                 null_source_meta);
    ASSERT_NOK(PrimaryKeyIndexSourceMeta::FromIndexFile(no_source_meta));
}

}  // namespace paimon::test

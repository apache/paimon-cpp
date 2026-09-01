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

#include "paimon/format/orc/orc_reader_wrapper.h"

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "gtest/gtest.h"
#include "orc/OrcFile.hh"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::orc::test {

class OrcReaderWrapperTest : public ::testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(OrcReaderWrapperTest, NextRowToRead) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    std::string file_path = dir->Str() + "/file.orc";
    {
        std::unique_ptr<::orc::OutputStream> outStream = ::orc::writeLocalFile(file_path);
        ::orc::WriterOptions options;
        std::unique_ptr<::orc::Type> schema =
            ::orc::Type::buildTypeFromString("struct<col1:int,col2:string>");
        std::unique_ptr<::orc::Writer> writer = createWriter(*schema, outStream.get(), options);
        auto col_batch = writer->createRowBatch(3);
        auto batch = dynamic_cast<::orc::StructVectorBatch*>(col_batch.get());
        auto* col1 = dynamic_cast<::orc::LongVectorBatch*>(batch->fields[0]);
        auto* col2 = dynamic_cast<::orc::StringVectorBatch*>(batch->fields[1]);
        batch->numElements = 3;
        col1->numElements = 3;
        col2->numElements = 3;
        col1->data[0] = 1;
        col1->data[1] = 2;
        col1->data[2] = 3;
        col2->data[0] = const_cast<char*>("a");
        col2->length[0] = 1;
        col2->data[1] = const_cast<char*>("b");
        col2->length[1] = 1;
        col2->data[2] = const_cast<char*>("c");
        col2->length[2] = 1;
        writer->add(*batch);
        writer->close();
    }

    std::shared_ptr<OrcReadMemory> read_memory = std::make_shared<OrcReadMemory>(GetDefaultPool());
    std::weak_ptr<OrcReadMemory> weak_read_memory = read_memory;
    std::weak_ptr<arrow::MemoryPool> weak_arrow_pool = read_memory->arrow_pool;
    std::weak_ptr<::orc::MemoryPool> weak_orc_pool = read_memory->orc_pool;
    ::orc::ReaderOptions reader_opts;
    reader_opts.setMemoryPool(*read_memory->orc_pool);
    std::unique_ptr<::orc::Reader> reader =
        ::orc::createReader(::orc::readLocalFile(file_path), reader_opts);
    std::map<std::string, std::string> options;
    ASSERT_OK_AND_ASSIGN(auto wrapper, OrcReaderWrapper::Create(
                                           /*reader=*/std::move(reader),
                                           /*file_name=*/file_path,
                                           /*batch_size=*/2,
                                           /*natural_read_size=*/0,
                                           /*options=*/options,
                                           /*read_memory=*/read_memory));
    read_memory.reset();
    auto data_types =
        arrow::struct_({arrow::field("col1", arrow::int64()), arrow::field("col2", arrow::utf8())});
    ::orc::RowReaderOptions row_opts;
    ASSERT_TRUE(wrapper->SetReadSchema(data_types, row_opts).ok());

    ASSERT_OK_AND_ASSIGN(auto batch1, wrapper->Next());
    EXPECT_EQ(wrapper->GetNextRowToRead(), 2u);  // batch_size=2
    ReaderUtils::ReleaseReadBatch(std::move(batch1));

    ASSERT_OK_AND_ASSIGN(auto batch2, wrapper->Next());
    EXPECT_EQ(wrapper->GetNextRowToRead(), 3u);  // only 1 row left

    ASSERT_OK_AND_ASSIGN(auto batch3, wrapper->Next());
    EXPECT_EQ(wrapper->GetNextRowToRead(), 3u);
    ReaderUtils::ReleaseReadBatch(std::move(batch3));

    wrapper.reset();
    EXPECT_FALSE(weak_read_memory.expired());
    EXPECT_FALSE(weak_arrow_pool.expired());
    EXPECT_FALSE(weak_orc_pool.expired());
    ReaderUtils::ReleaseReadBatch(std::move(batch2));
    EXPECT_TRUE(weak_read_memory.expired());
    EXPECT_TRUE(weak_arrow_pool.expired());
    EXPECT_TRUE(weak_orc_pool.expired());
}

}  // namespace paimon::orc::test

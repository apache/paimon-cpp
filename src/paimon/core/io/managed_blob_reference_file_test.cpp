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

#include "paimon/core/io/managed_blob_reference_file.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/util/crc32.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/fs/file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class ManagedBlobReferenceFileTest : public testing::Test {
 protected:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create("local");
    }

    void TearDown() override {
        dir_.reset();
    }

    static ManagedBlobReferenceFile::Reference NewReference(const std::string& root,
                                                            const std::string& name) {
        return ManagedBlobReferenceFile::Reference::Create(root, name).value();
    }

    static void AppendBe32(std::string* buffer, uint32_t value) {
        buffer->push_back(static_cast<char>((value >> 24) & 0xFF));
        buffer->push_back(static_cast<char>((value >> 16) & 0xFF));
        buffer->push_back(static_cast<char>((value >> 8) & 0xFF));
        buffer->push_back(static_cast<char>(value & 0xFF));
    }

    /// Writes `content` verbatim, replacing any file already at `path`.
    void WriteRawFile(const std::string& path, const std::string& content) {
        if (dir_->GetFileSystem()->GetFileStatus(path).ok()) {
            ASSERT_OK(dir_->GetFileSystem()->Delete(path));
        }
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> out,
                             dir_->GetFileSystem()->Create(path, /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(int64_t written, out->Write(content.data(), content.size()));
        ASSERT_EQ(written, static_cast<int64_t>(content.size()));
        ASSERT_OK(out->Close());
    }

    void ReadRawFile(const std::string& path, std::string* content) {
        ASSERT_OK_AND_ASSIGN(FileStatus status, dir_->GetFileSystem()->GetFileStatus(path));
        content->assign(status.GetLen(), '\0');
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> in, dir_->GetFileSystem()->Open(status));
        ASSERT_OK_AND_ASSIGN(int64_t read, in->Read(content->data(), content->size()));
        ASSERT_EQ(read, static_cast<int64_t>(content->size()));
        ASSERT_OK(in->Close());
    }

    /// A well-formed header carrying `count` in its count slot, followed by a matching CRC32
    /// over the checksum-covered region. Used to reach the header checks with a valid checksum.
    static std::string HeaderOnlyFile(uint32_t magic, char version, uint32_t count) {
        std::string content;
        AppendBe32(&content, magic);
        size_t covered_start = content.size();
        content.push_back(version);
        AppendBe32(&content, count);
        AppendBe32(&content, arrow::internal::crc32(0, content.data() + covered_start,
                                                    content.size() - covered_start));
        return content;
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
};

TEST_F(ManagedBlobReferenceFileTest, TestRoundTripSortsAndDeduplicates) {
    std::string path = PathUtil::JoinPath(dir_->Str(), "data-1.parquet.blobref");
    std::vector<ManagedBlobReferenceFile::Reference> references = {
        NewReference("/warehouse/bucket-0", "data-b.managed.blob"),
        NewReference("/warehouse/bucket-0", "data-a.managed.blob"),
        NewReference("/warehouse/bucket-0", "data-a.managed.blob"),
        NewReference("/other/bucket-0", "data-c.managed.blob"),
    };
    ASSERT_OK(ManagedBlobReferenceFile::Write(dir_->GetFileSystem(), path, references));

    ASSERT_OK_AND_ASSIGN(std::vector<ManagedBlobReferenceFile::Reference> read_back,
                         ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), path));
    ASSERT_EQ(read_back.size(), 3);
    EXPECT_EQ(read_back[0], NewReference("/other/bucket-0", "data-c.managed.blob"));
    EXPECT_EQ(read_back[1], NewReference("/warehouse/bucket-0", "data-a.managed.blob"));
    EXPECT_EQ(read_back[2], NewReference("/warehouse/bucket-0", "data-b.managed.blob"));
}

TEST_F(ManagedBlobReferenceFileTest, TestEmptyReferenceListIsValid) {
    std::string path = PathUtil::JoinPath(dir_->Str(), "data-2.parquet.blobref");
    ASSERT_OK(ManagedBlobReferenceFile::Write(dir_->GetFileSystem(), path, {}));
    ASSERT_OK_AND_ASSIGN(std::vector<ManagedBlobReferenceFile::Reference> read_back,
                         ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), path));
    ASSERT_TRUE(read_back.empty());
}

TEST_F(ManagedBlobReferenceFileTest, TestNonAsciiReferenceRoundTrip) {
    std::string path = PathUtil::JoinPath(dir_->Str(), "data-3.parquet.blobref");
    std::vector<ManagedBlobReferenceFile::Reference> references = {
        NewReference("/warehouse/p0=中文分区/bucket-0", "data-a.managed.blob")};
    ASSERT_OK(ManagedBlobReferenceFile::Write(dir_->GetFileSystem(), path, references));
    ASSERT_OK_AND_ASSIGN(std::vector<ManagedBlobReferenceFile::Reference> read_back,
                         ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), path));
    ASSERT_EQ(read_back.size(), 1);
    EXPECT_EQ(read_back[0], references[0]);
}

TEST_F(ManagedBlobReferenceFileTest, TestJavaGoldenBytes) {
    // The exact bytes the sidecar format prescribes for these two references (magic, version,
    // count and CRC32 big-endian, strings as a uint16 length plus modified UTF-8 bytes).
    // The checksum constant was computed independently of this codebase; the test locks the
    // on-disk format against both accidental layout changes and checksum-coverage changes.
    const std::string root = "/tmp/warehouse/foo.db/bar/bucket-0";
    const std::string pack0 = "data-2b1a7f4e-0.managed.blob";
    const std::string pack1 = "data-2b1a7f4e-1.managed.blob";
    std::string golden;
    auto append_utf = [&golden](const std::string& value) {
        golden.push_back(static_cast<char>((value.size() >> 8) & 0xFF));
        golden.push_back(static_cast<char>(value.size() & 0xFF));
        golden.append(value);
    };
    AppendBe32(&golden, 0x50424C52);  // magic "PBLR"
    golden.push_back(0x01);           // version
    AppendBe32(&golden, 2);           // reference count
    append_utf(root);
    append_utf(pack0);
    append_utf(root);
    append_utf(pack1);
    AppendBe32(&golden, 0x919BDB75);  // CRC32 of the covered region, computed with zlib
    ASSERT_EQ(golden.size(), 145);

    // A file carrying exactly those bytes is readable.
    std::string java_path = PathUtil::JoinPath(dir_->Str(), "data-java.parquet.blobref");
    WriteRawFile(java_path, golden);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_OK_AND_ASSIGN(std::vector<ManagedBlobReferenceFile::Reference> read_back,
                         ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), java_path));
    ASSERT_EQ(read_back.size(), 2);
    EXPECT_EQ(read_back[0], NewReference(root, pack0));
    EXPECT_EQ(read_back[1], NewReference(root, pack1));

    // A C++-written file is byte-identical, including the sort normalization.
    std::string cpp_path = PathUtil::JoinPath(dir_->Str(), "data-cpp.parquet.blobref");
    ASSERT_OK(ManagedBlobReferenceFile::Write(
        dir_->GetFileSystem(), cpp_path, {NewReference(root, pack1), NewReference(root, pack0)}));
    std::string cpp_content;
    ReadRawFile(cpp_path, &cpp_content);
    ASSERT_FALSE(HasFatalFailure());
    EXPECT_EQ(cpp_content, golden);
}

TEST_F(ManagedBlobReferenceFileTest, TestRejectsCorruptedFile) {
    std::string path = PathUtil::JoinPath(dir_->Str(), "data-4.parquet.blobref");
    ASSERT_OK(ManagedBlobReferenceFile::Write(
        dir_->GetFileSystem(), path, {NewReference("/warehouse/bucket-0", "data-a.managed.blob")}));

    // Flip one byte inside a reference string's content, leaving every length prefix intact:
    // the file still parses, so only the checksum can catch the change. Corrupting a length
    // prefix instead would trip the framing checks before the checksum is ever compared.
    std::string content;
    ReadRawFile(path, &content);
    ASSERT_FALSE(HasFatalFailure());
    size_t corrupt_at = content.find("bucket-0");
    ASSERT_NE(corrupt_at, std::string::npos);
    content[corrupt_at] = static_cast<char>(content[corrupt_at] ^ 0x1);
    WriteRawFile(path, content);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_NOK_WITH_MSG(ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), path).status(),
                        "checksum mismatch");
}

TEST_F(ManagedBlobReferenceFileTest, TestRejectsTrailingBytes) {
    std::string path = PathUtil::JoinPath(dir_->Str(), "data-5.parquet.blobref");
    ASSERT_OK(ManagedBlobReferenceFile::Write(dir_->GetFileSystem(), path, {}));
    std::string content;
    ReadRawFile(path, &content);
    ASSERT_FALSE(HasFatalFailure());
    content.push_back('x');
    WriteRawFile(path, content);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_NOK_WITH_MSG(ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), path).status(),
                        "trailing bytes");
}

TEST_F(ManagedBlobReferenceFileTest, TestRejectsBadMagic) {
    std::string path = PathUtil::JoinPath(dir_->Str(), "data-9.parquet.blobref");
    WriteRawFile(path, HeaderOnlyFile(/*magic=*/0x50424C53, /*version=*/0x01, /*count=*/0));
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_NOK_WITH_MSG(ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), path).status(),
                        "bad magic number");
}

TEST_F(ManagedBlobReferenceFileTest, TestRejectsUnsupportedVersion) {
    // A future writer bumps the version; this reader must refuse the file instead of parsing
    // it under the version 1 layout.
    std::string path = PathUtil::JoinPath(dir_->Str(), "data-10.parquet.blobref");
    WriteRawFile(path, HeaderOnlyFile(/*magic=*/0x50424C52, /*version=*/0x02, /*count=*/0));
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_NOK_WITH_MSG(ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), path).status(),
                        "unsupported version");
}

TEST_F(ManagedBlobReferenceFileTest, TestRejectsNegativeCount) {
    std::string path = PathUtil::JoinPath(dir_->Str(), "data-11.parquet.blobref");
    WriteRawFile(path,
                 HeaderOnlyFile(/*magic=*/0x50424C52, /*version=*/0x01, /*count=*/0xFFFFFFFF));
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_NOK_WITH_MSG(ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), path).status(),
                        "negative count");
}

TEST_F(ManagedBlobReferenceFileTest, TestRejectsOversizedCount) {
    // A corrupted or malicious file may declare a huge reference count with a valid checksum;
    // the reader must bound it by the remaining bytes before allocating for it.
    std::string path = PathUtil::JoinPath(dir_->Str(), "data-7.parquet.blobref");
    // Count far beyond what the payload can hold, under a valid checksum.
    WriteRawFile(path,
                 HeaderOnlyFile(/*magic=*/0x50424C52, /*version=*/0x01, /*count=*/0x7FFFFFFF));
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_NOK_WITH_MSG(ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), path).status(),
                        "more references than it can hold");
}

TEST_F(ManagedBlobReferenceFileTest, TestRejectsMalformedLowSurrogate) {
    // A "low surrogate" group whose middle byte is not a continuation byte (0x30, ASCII '0')
    // still masks into [0xDC00, 0xDFFF] under plain bit-masking. Such bytes are not valid
    // modified UTF-8, so the reader must reject them even under a valid checksum.
    std::string path = PathUtil::JoinPath(dir_->Str(), "data-8.parquet.blobref");
    std::string content;
    AppendBe32(&content, 0x50424C52);  // magic
    size_t covered_start = content.size();
    content.push_back(0x01);  // version
    AppendBe32(&content, 1);  // one reference
    // storage root id: a high surrogate (ED A0 80 = U+D800) followed by ED 30 80, whose
    // middle byte is not a continuation byte but masks to the low surrogate 0xDC00
    const uint8_t malformed_root[] = {0xED, 0xA0, 0x80, 0xED, 0x30, 0x80};
    content.push_back(0x00);
    content.push_back(static_cast<char>(sizeof(malformed_root)));
    for (uint8_t byte : malformed_root) {
        content.push_back(static_cast<char>(byte));
    }
    // relative path: a well-formed bare file name
    const std::string relative_path = "a.managed.blob";
    content.push_back(0x00);
    content.push_back(static_cast<char>(relative_path.size()));
    content += relative_path;
    AppendBe32(&content, arrow::internal::crc32(0, content.data() + covered_start,
                                                content.size() - covered_start));
    WriteRawFile(path, content);
    ASSERT_FALSE(HasFatalFailure());
    // The rejection must come from the surrogate pairing check, not from the framing around it.
    ASSERT_NOK_WITH_MSG(ManagedBlobReferenceFile::Read(dir_->GetFileSystem(), path).status(),
                        "unpaired surrogate");
}

TEST_F(ManagedBlobReferenceFileTest, TestFromDescriptorUri) {
    ASSERT_OK_AND_ASSIGN(std::optional<ManagedBlobReferenceFile::Reference> managed,
                         ManagedBlobReferenceFile::FromDescriptorUri(
                             "/warehouse/foo.db/bar/bucket-0/data-1.managed.blob"));
    ASSERT_TRUE(managed.has_value());
    EXPECT_EQ(managed->storage_root_id, "/warehouse/foo.db/bar/bucket-0");
    EXPECT_EQ(managed->relative_path, "data-1.managed.blob");

    ASSERT_OK_AND_ASSIGN(
        std::optional<ManagedBlobReferenceFile::Reference> not_managed,
        ManagedBlobReferenceFile::FromDescriptorUri("/warehouse/foo.db/bar/bucket-0/data-1.blob"));
    EXPECT_FALSE(not_managed.has_value());
}

TEST_F(ManagedBlobReferenceFileTest, TestReferenceRejectsNestedRelativePath) {
    // A nested path and a traversal are rejected as non-bare names; an empty part is a
    // distinct failure, so each case must surface its own reason.
    ASSERT_NOK_WITH_MSG(
        ManagedBlobReferenceFile::Reference::Create("/warehouse", "a/b.managed.blob").status(),
        "must be a bare file name");
    ASSERT_NOK_WITH_MSG(ManagedBlobReferenceFile::Reference::Create("/warehouse", "..").status(),
                        "must be a bare file name");
    ASSERT_NOK_WITH_MSG(ManagedBlobReferenceFile::Reference::Create("", "a.managed.blob").status(),
                        "must not be empty");
}

TEST_F(ManagedBlobReferenceFileTest, TestSidecarPath) {
    EXPECT_EQ(ManagedBlobReferenceFile::SidecarPath("/w/bucket-0/data-1.parquet"),
              "/w/bucket-0/data-1.parquet.blobref");
}

}  // namespace paimon::test

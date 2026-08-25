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

#include "paimon/core/table/format/format_file_listing.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

Status WriteAt(const std::shared_ptr<FileSystem>& file_system, const std::string& path) {
    std::string parent = PathUtil::GetParentDirPath(path);
    PAIMON_RETURN_NOT_OK(file_system->Mkdirs(parent));
    return file_system->WriteFile(path, "row\n", /*overwrite=*/true);
}

/// The listed files as paths relative to `root`, sorted so the order of a listing cannot matter.
Result<std::vector<std::string>> ListNames(const std::shared_ptr<FileSystem>& file_system,
                                           const std::string& root,
                                           const FormatDataFileListingOptions& options) {
    std::vector<FormatDataSplit::FileMeta> files;
    PAIMON_RETURN_NOT_OK(FormatFileListing::ListDataFiles(file_system, root, options, &files));
    std::vector<std::string> names;
    names.reserve(files.size());
    for (const FormatDataSplit::FileMeta& file : files) {
        names.push_back(file.file_path.substr(root.size() + 1));
    }
    std::sort(names.begin(), names.end());
    return names;
}

}  // namespace

TEST(FormatFileListingTest, TestDescendsIntoPlainSubdirectories) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    // `data-file.path-directory`, and other engines, put data files below the partition directory
    // rather than directly in it, so stopping at the top level would miss them.
    ASSERT_OK(WriteAt(file_system, dir->Str() + "/a.parquet"));
    ASSERT_OK(WriteAt(file_system, dir->Str() + "/nested/b.parquet"));
    ASSERT_OK(WriteAt(file_system, dir->Str() + "/nested/deeper/c.parquet"));

    ASSERT_OK_AND_ASSIGN(std::vector<std::string> names,
                         ListNames(file_system, dir->Str(), FormatDataFileListingOptions{}));
    ASSERT_EQ(names, (std::vector<std::string>{"a.parquet", "nested/b.parquet",
                                               "nested/deeper/c.parquet"}));
}

TEST(FormatFileListingTest, TestHiddenNamesAreSkippedAndNotDescendedInto) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    // A staging tree holds another job's uncommitted output under ordinary data file names, so
    // only the directory above them tells the two apart.
    ASSERT_OK(WriteAt(file_system, dir->Str() + "/a.parquet"));
    ASSERT_OK(WriteAt(file_system, dir->Str() + "/.b.parquet.tmp"));
    ASSERT_OK(WriteAt(file_system, dir->Str() + "/_temporary/c.parquet"));
    ASSERT_OK(WriteAt(file_system, dir->Str() + "/.hive-staging_1/d.parquet"));

    ASSERT_OK_AND_ASSIGN(std::vector<std::string> names,
                         ListNames(file_system, dir->Str(), FormatDataFileListingOptions{}));
    ASSERT_EQ(names, (std::vector<std::string>{"a.parquet"}));
}

TEST(FormatFileListingTest, TestDefaultPartitionDirectoryIsTheOneHiddenNameThatIsContent) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    // In the value-only layout a partition directory is the bare value, so a null partition is
    // named `__DEFAULT_PARTITION__` - a hidden name that nonetheless holds table data.
    ASSERT_OK(WriteAt(file_system, dir->Str() + "/__DEFAULT_PARTITION__/a.parquet"));
    ASSERT_OK(WriteAt(file_system, dir->Str() + "/_temporary/b.parquet"));

    FormatDataFileListingOptions options;
    options.partition_levels = 1;
    options.only_value_in_path = true;
    options.default_part_name = "__DEFAULT_PARTITION__";
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> names,
                         ListNames(file_system, dir->Str(), options));
    ASSERT_EQ(names, (std::vector<std::string>{"__DEFAULT_PARTITION__/a.parquet"}));

    // With no partition level below the root, that name is a staging tree like any other.
    FormatDataFileListingOptions no_partition_level = options;
    no_partition_level.partition_levels = 0;
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> without,
                         ListNames(file_system, dir->Str(), no_partition_level));
    ASSERT_TRUE(without.empty());
}

TEST(FormatFileListingTest, TestReservedDirectoriesAreSkippedOnlyWhenTheyAreMetadata) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK(WriteAt(file_system, dir->Str() + "/a.parquet"));
    ASSERT_OK(WriteAt(file_system, dir->Str() + "/schema/schema-0"));

    FormatDataFileListingOptions metadata_here;
    metadata_here.skip_reserved_directories = true;
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> skipped,
                         ListNames(file_system, dir->Str(), metadata_here));
    ASSERT_EQ(skipped, (std::vector<std::string>{"a.parquet"}));

    // For a table whose schema lives in a metastore, the location is nothing but data and a
    // directory of that name is a partition value or a data subdirectory.
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> kept,
                         ListNames(file_system, dir->Str(), FormatDataFileListingOptions{}));
    ASSERT_EQ(kept, (std::vector<std::string>{"a.parquet", "schema/schema-0"}));
}

TEST(FormatFileListingTest, TestMissingRootIsAnErrorButAVanishedSubdirectoryIsNot) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
    // A root that is not there means the location is wrong or the data is gone. The file systems
    // here report a missing directory as an empty listing, so this only works because the root is
    // asked about outright - passing it off as a table with no rows would hide a mistyped path.
    std::vector<FormatDataSplit::FileMeta> files;
    ASSERT_NOK(FormatFileListing::ListDataFiles(file_system, dir->Str() + "/absent",
                                                FormatDataFileListingOptions{}, &files));

    // A root that is a file, not a directory, is wrong in the same way. The table's own data file
    // stands in for it, so nothing is left behind to turn up in the listing below.
    ASSERT_OK(WriteAt(file_system, dir->Str() + "/a.parquet"));
    ASSERT_NOK_WITH_MSG(FormatFileListing::ListDataFiles(file_system, dir->Str() + "/a.parquet",
                                                         FormatDataFileListingOptions{}, &files),
                        "is not a directory");

    // A directory below it is another matter: it can be gone by the time the listing reaches it,
    // and the rest of the listing still stands.
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> names,
                         ListNames(file_system, dir->Str(), FormatDataFileListingOptions{}));
    ASSERT_EQ(names, (std::vector<std::string>{"a.parquet"}));
}

}  // namespace paimon::test

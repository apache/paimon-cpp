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

#include "paimon/common/global_index/sorted_file_meta_selector.h"

#include "fmt/format.h"
#include "paimon/common/memory/memory_slice.h"

namespace paimon {
Result<std::unique_ptr<SortedFileMetaSelector>> SortedFileMetaSelector::Create(
    const std::vector<GlobalIndexIOMeta>& files,
    const std::shared_ptr<KeySerializer>& key_serializer) {
    if (key_serializer == nullptr) {
        return Status::Invalid(
            "Cannot create a sorted file metadata selector without a key serializer.");
    }
    std::vector<std::pair<GlobalIndexIOMeta, std::shared_ptr<SortedIndexFileMeta>>> decoded_files;
    decoded_files.reserve(files.size());
    MemorySlice::SliceComparator comparator = key_serializer->CreateComparator();
    for (const auto& file : files) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<SortedIndexFileMeta> index_meta,
            SortedIndexFileMeta::Deserialize(file.metadata, key_serializer->GetMemoryPool()));
        bool has_first_key = index_meta->FirstKey() != nullptr;
        bool has_last_key = index_meta->LastKey() != nullptr;
        if (has_first_key != has_last_key) {
            return Status::Invalid(fmt::format(
                "Sorted index file metadata for {} must contain both boundary keys or neither.",
                file.file_path));
        }
        if (!has_first_key && !index_meta->HasNulls()) {
            return Status::Invalid(fmt::format(
                "Sorted index file metadata for {} has no boundary keys or null values.",
                file.file_path));
        }
        if (index_meta->FirstKey() != nullptr) {
            PAIMON_RETURN_NOT_OK(
                key_serializer->ValidateSerializedKey(WrapKeySlice(index_meta->FirstKey())));
        }
        if (index_meta->LastKey() != nullptr) {
            PAIMON_RETURN_NOT_OK(
                key_serializer->ValidateSerializedKey(WrapKeySlice(index_meta->LastKey())));
        }
        if (index_meta->FirstKey() != nullptr && index_meta->LastKey() != nullptr) {
            PAIMON_ASSIGN_OR_RAISE(int32_t comparison,
                                   comparator(WrapKeySlice(index_meta->FirstKey()),
                                              WrapKeySlice(index_meta->LastKey())));
            if (comparison > 0) {
                return Status::Invalid(fmt::format(
                    "Sorted index file metadata for {} has a first key greater than its last key.",
                    file.file_path));
            }
        }
        decoded_files.emplace_back(file, std::move(index_meta));
    }
    return std::unique_ptr<SortedFileMetaSelector>(
        new SortedFileMetaSelector(std::move(decoded_files), key_serializer));
}

SortedFileMetaSelector::SortedFileMetaSelector(
    std::vector<std::pair<GlobalIndexIOMeta, std::shared_ptr<SortedIndexFileMeta>>> files,
    std::shared_ptr<KeySerializer> key_serializer)
    : files_(std::move(files)),
      key_serializer_(std::move(key_serializer)),
      comparator_(key_serializer_->CreateComparator()) {}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitIsNotNull() {
    return Filter(
        [](const SortedIndexFileMeta& meta) -> Result<bool> { return !meta.OnlyNulls(); });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitIsNull() {
    return Filter([](const SortedIndexFileMeta& meta) -> Result<bool> { return meta.HasNulls(); });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitEqual(const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(MemorySlice literal_slice, SerializeLiteral(literal));
    return Filter([this, &literal_slice](const SortedIndexFileMeta& meta) -> Result<bool> {
        if (meta.OnlyNulls()) {
            return false;
        }
        return Overlaps(meta, literal_slice, literal_slice);
    });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitNotEqual(
    const Literal& literal) {
    return Filter([](const SortedIndexFileMeta& meta) -> Result<bool> { return true; });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitLessThan(
    const Literal& literal) {
    // file.minKey < literal
    PAIMON_ASSIGN_OR_RAISE(MemorySlice literal_slice, SerializeLiteral(literal));
    return Filter([this, &literal_slice](const SortedIndexFileMeta& meta) -> Result<bool> {
        if (meta.OnlyNulls()) {
            return false;
        }
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp, CompareFirstKey(meta, literal_slice));
        return cmp < 0;
    });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitLessOrEqual(
    const Literal& literal) {
    // file.minKey <= literal
    PAIMON_ASSIGN_OR_RAISE(MemorySlice literal_slice, SerializeLiteral(literal));
    return Filter([this, &literal_slice](const SortedIndexFileMeta& meta) -> Result<bool> {
        if (meta.OnlyNulls()) {
            return false;
        }
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp, CompareFirstKey(meta, literal_slice));
        return cmp <= 0;
    });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitGreaterThan(
    const Literal& literal) {
    // file.maxKey > literal
    PAIMON_ASSIGN_OR_RAISE(MemorySlice literal_slice, SerializeLiteral(literal));
    return Filter([this, &literal_slice](const SortedIndexFileMeta& meta) -> Result<bool> {
        if (meta.OnlyNulls()) {
            return false;
        }
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp, CompareLastKey(meta, literal_slice));
        return cmp > 0;
    });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitGreaterOrEqual(
    const Literal& literal) {
    // file.maxKey >= literal
    PAIMON_ASSIGN_OR_RAISE(MemorySlice literal_slice, SerializeLiteral(literal));
    return Filter([this, &literal_slice](const SortedIndexFileMeta& meta) -> Result<bool> {
        if (meta.OnlyNulls()) {
            return false;
        }
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp, CompareLastKey(meta, literal_slice));
        return cmp >= 0;
    });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitIn(
    const std::vector<Literal>& literals) {
    std::vector<MemorySlice> literal_slices;
    literal_slices.reserve(literals.size());
    for (const auto& literal : literals) {
        PAIMON_ASSIGN_OR_RAISE(MemorySlice slice, SerializeLiteral(literal));
        literal_slices.push_back(std::move(slice));
    }
    return Filter([this, &literal_slices](const SortedIndexFileMeta& meta) -> Result<bool> {
        if (meta.OnlyNulls()) {
            return false;
        }
        for (const auto& literal_slice : literal_slices) {
            PAIMON_ASSIGN_OR_RAISE(bool overlaps, Overlaps(meta, literal_slice, literal_slice));
            if (overlaps) {
                return true;
            }
        }
        return false;
    });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitNotIn(
    const std::vector<Literal>& literals) {
    // Cannot filter any file by NOT IN condition
    return Filter([](const SortedIndexFileMeta& meta) -> Result<bool> { return true; });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitStartsWith(
    const Literal& prefix) {
    return Filter([](const SortedIndexFileMeta& meta) -> Result<bool> { return true; });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitEndsWith(
    const Literal& suffix) {
    return Filter([](const SortedIndexFileMeta& meta) -> Result<bool> { return true; });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitContains(
    const Literal& literal) {
    return Filter([](const SortedIndexFileMeta& meta) -> Result<bool> { return true; });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::VisitLike(const Literal& literal) {
    return Filter([](const SortedIndexFileMeta& meta) -> Result<bool> { return true; });
}

Result<std::vector<GlobalIndexIOMeta>> SortedFileMetaSelector::Filter(
    const MetaPredicate& predicate) const {
    std::vector<GlobalIndexIOMeta> result;
    for (const auto& [io_meta, index_meta] : files_) {
        PAIMON_ASSIGN_OR_RAISE(bool matched, predicate(*index_meta));
        if (matched) {
            result.push_back(io_meta);
        }
    }
    return result;
}

Result<bool> SortedFileMetaSelector::Overlaps(const SortedIndexFileMeta& meta,
                                              const MemorySlice& from,
                                              const MemorySlice& to) const {
    if (meta.FirstKey()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp, comparator_(to, WrapKeySlice(meta.FirstKey())));
        if (cmp < 0) {
            return false;
        }
    }
    if (meta.LastKey()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t cmp, comparator_(from, WrapKeySlice(meta.LastKey())));
        if (cmp > 0) {
            return false;
        }
    }
    return true;
}

Result<int32_t> SortedFileMetaSelector::CompareFirstKey(const SortedIndexFileMeta& meta,
                                                        const MemorySlice& literal) const {
    if (!meta.FirstKey()) {
        return -1;
    }
    return comparator_(WrapKeySlice(meta.FirstKey()), literal);
}

Result<int32_t> SortedFileMetaSelector::CompareLastKey(const SortedIndexFileMeta& meta,
                                                       const MemorySlice& literal) const {
    if (!meta.LastKey()) {
        return 1;
    }
    return comparator_(WrapKeySlice(meta.LastKey()), literal);
}

MemorySlice SortedFileMetaSelector::WrapKeySlice(const std::shared_ptr<Bytes>& key) {
    return MemorySlice::Wrap(MemorySegment::WrapView(key->data(), key->size()));
}

Result<MemorySlice> SortedFileMetaSelector::SerializeLiteral(const Literal& literal) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, key_serializer_->Serialize(literal));
    MemorySlice slice = MemorySlice::Wrap(bytes);
    PAIMON_RETURN_NOT_OK(key_serializer_->ValidateSerializedKey(slice));
    return slice;
}

}  // namespace paimon

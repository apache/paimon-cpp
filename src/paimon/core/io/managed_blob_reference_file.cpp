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

#include <algorithm>
#include <cstdint>
#include <string_view>
#include <utility>

#include "arrow/util/crc32.h"
#include "fmt/format.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/status.h"

namespace paimon {

namespace {

constexpr int32_t kMagic = 0x50424C52;  // "PBLR"
constexpr int8_t kVersion = 1;

void AppendBigEndianInt32(int32_t value, std::string* out) {
    auto bits = static_cast<uint32_t>(value);
    out->push_back(static_cast<char>((bits >> 24) & 0xFF));
    out->push_back(static_cast<char>((bits >> 16) & 0xFF));
    out->push_back(static_cast<char>((bits >> 8) & 0xFF));
    out->push_back(static_cast<char>(bits & 0xFF));
}

/// A sequential big-endian reader over an in-memory buffer.
class BigEndianBufferReader {
 public:
    BigEndianBufferReader(const char* data, size_t size) : data_(data), size_(size) {}

    Result<int32_t> ReadInt32() {
        PAIMON_RETURN_NOT_OK(Require(4));
        uint32_t bits = (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_])) << 24) |
                        (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 1])) << 16) |
                        (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 2])) << 8) |
                        static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 3]));
        pos_ += 4;
        return static_cast<int32_t>(bits);
    }

    Result<int8_t> ReadInt8() {
        PAIMON_RETURN_NOT_OK(Require(1));
        return static_cast<int8_t>(data_[pos_++]);
    }

    Result<uint16_t> ReadUInt16() {
        PAIMON_RETURN_NOT_OK(Require(2));
        auto bits = static_cast<uint16_t>((static_cast<uint8_t>(data_[pos_]) << 8) |
                                          static_cast<uint8_t>(data_[pos_ + 1]));
        pos_ += 2;
        return bits;
    }

    Result<std::string_view> ReadBytes(size_t length) {
        PAIMON_RETURN_NOT_OK(Require(length));
        std::string_view view(data_ + pos_, length);
        pos_ += length;
        return view;
    }

    size_t Position() const {
        return pos_;
    }

    size_t Remaining() const {
        return size_ - pos_;
    }

 private:
    Status Require(size_t length) const {
        if (pos_ + length > size_) {
            return Status::Invalid("Managed blob reference file is truncated.");
        }
        return Status::OK();
    }

    const char* data_;
    size_t size_;
    size_t pos_ = 0;
};

/// Encodes a UTF-8 string the way the sidecar format stores it: a uint16 big-endian byte
/// length followed by modified UTF-8 bytes. Modified UTF-8 equals standard UTF-8 except that
/// U+0000 becomes the two-byte form 0xC0 0x80 and supplementary characters are encoded as a
/// CESU-8 surrogate pair (two three-byte groups).
Status AppendJavaUtf(const std::string& value, std::string* out) {
    std::string encoded;
    encoded.reserve(value.size());
    size_t i = 0;
    const auto* bytes = reinterpret_cast<const uint8_t*>(value.data());
    while (i < value.size()) {
        uint8_t lead = bytes[i];
        uint32_t code_point = 0;
        size_t sequence_length = 0;
        if (lead < 0x80) {
            code_point = lead;
            sequence_length = 1;
        } else if ((lead >> 5) == 0x6) {
            code_point = lead & 0x1F;
            sequence_length = 2;
        } else if ((lead >> 4) == 0xE) {
            code_point = lead & 0x0F;
            sequence_length = 3;
        } else if ((lead >> 3) == 0x1E) {
            code_point = lead & 0x07;
            sequence_length = 4;
        } else {
            return Status::Invalid("Managed blob reference contains invalid UTF-8.");
        }
        if (i + sequence_length > value.size()) {
            return Status::Invalid("Managed blob reference contains truncated UTF-8.");
        }
        for (size_t j = 1; j < sequence_length; j++) {
            uint8_t continuation = bytes[i + j];
            if ((continuation >> 6) != 0x2) {
                return Status::Invalid("Managed blob reference contains invalid UTF-8.");
            }
            code_point = (code_point << 6) | (continuation & 0x3F);
        }
        i += sequence_length;

        if (code_point == 0) {
            encoded.push_back(static_cast<char>(0xC0));
            encoded.push_back(static_cast<char>(0x80));
        } else if (code_point < 0x10000) {
            // The single-, two- and three-byte forms match standard UTF-8.
            encoded.append(value, i - sequence_length, sequence_length);
        } else {
            // Supplementary character: encode the UTF-16 surrogate pair, three bytes each.
            uint32_t offset = code_point - 0x10000;
            uint32_t high = 0xD800 + (offset >> 10);
            uint32_t low = 0xDC00 + (offset & 0x3FF);
            for (uint32_t surrogate : {high, low}) {
                encoded.push_back(static_cast<char>(0xE0 | (surrogate >> 12)));
                encoded.push_back(static_cast<char>(0x80 | ((surrogate >> 6) & 0x3F)));
                encoded.push_back(static_cast<char>(0x80 | (surrogate & 0x3F)));
            }
        }
    }
    if (encoded.size() > 0xFFFF) {
        return Status::Invalid(
            fmt::format("Managed blob reference string is too long: {} bytes.", encoded.size()));
    }
    out->push_back(static_cast<char>((encoded.size() >> 8) & 0xFF));
    out->push_back(static_cast<char>(encoded.size() & 0xFF));
    out->append(encoded);
    return Status::OK();
}

/// Decodes such a value back to a standard UTF-8 string.
Result<std::string> ReadJavaUtf(BigEndianBufferReader* reader) {
    PAIMON_ASSIGN_OR_RAISE(uint16_t length, reader->ReadUInt16());
    PAIMON_ASSIGN_OR_RAISE(std::string_view encoded, reader->ReadBytes(length));
    std::string decoded;
    decoded.reserve(encoded.size());
    const auto* bytes = reinterpret_cast<const uint8_t*>(encoded.data());
    size_t i = 0;
    while (i < encoded.size()) {
        uint8_t lead = bytes[i];
        if (lead < 0x80) {
            decoded.push_back(static_cast<char>(lead));
            i += 1;
            continue;
        }
        size_t sequence_length = (lead >> 5) == 0x6 ? 2 : (lead >> 4) == 0xE ? 3 : 0;
        if (sequence_length == 0 || i + sequence_length > encoded.size()) {
            return Status::Invalid("Managed blob reference file holds invalid modified UTF-8.");
        }
        uint32_t code_point = lead & (sequence_length == 2 ? 0x1F : 0x0F);
        for (size_t j = 1; j < sequence_length; j++) {
            uint8_t continuation = bytes[i + j];
            if ((continuation >> 6) != 0x2) {
                return Status::Invalid("Managed blob reference file holds invalid modified UTF-8.");
            }
            code_point = (code_point << 6) | (continuation & 0x3F);
        }
        i += sequence_length;

        if (code_point >= 0xD800 && code_point <= 0xDBFF) {
            // High surrogate: the low surrogate must follow as another three-byte group with
            // valid continuation bytes. A well-formed group outside the low-surrogate range
            // and a high surrogate at the end of the input are rejected here as well, instead
            // of decoding into an unpaired surrogate that no longer round-trips.
            if (i + 3 > encoded.size() || (bytes[i] >> 4) != 0xE || (bytes[i + 1] >> 6) != 0x2 ||
                (bytes[i + 2] >> 6) != 0x2) {
                return Status::Invalid("Managed blob reference file holds an unpaired surrogate.");
            }
            uint32_t low =
                ((bytes[i] & 0x0F) << 12) | ((bytes[i + 1] & 0x3F) << 6) | (bytes[i + 2] & 0x3F);
            if (low < 0xDC00 || low > 0xDFFF) {
                return Status::Invalid("Managed blob reference file holds an unpaired surrogate.");
            }
            i += 3;
            uint32_t supplementary = 0x10000 + ((code_point - 0xD800) << 10) + (low - 0xDC00);
            decoded.push_back(static_cast<char>(0xF0 | (supplementary >> 18)));
            decoded.push_back(static_cast<char>(0x80 | ((supplementary >> 12) & 0x3F)));
            decoded.push_back(static_cast<char>(0x80 | ((supplementary >> 6) & 0x3F)));
            decoded.push_back(static_cast<char>(0x80 | (supplementary & 0x3F)));
            continue;
        }
        if (code_point == 0 && sequence_length == 2) {
            decoded.push_back('\0');
            continue;
        }
        // BMP character: re-encode as standard UTF-8, identical to the input bytes.
        decoded.append(encoded.substr(i - sequence_length, sequence_length));
    }
    return decoded;
}

}  // namespace

Result<ManagedBlobReferenceFile::Reference> ManagedBlobReferenceFile::Reference::Create(
    const std::string& storage_root_id, const std::string& relative_path) {
    if (storage_root_id.empty() || relative_path.empty()) {
        return Status::Invalid("Managed blob reference parts must not be empty.");
    }
    if (relative_path != PathUtil::GetName(relative_path) || relative_path == "." ||
        relative_path == "..") {
        return Status::Invalid(fmt::format(
            "Managed blob reference relative path must be a bare file name, but got {}.",
            relative_path));
    }
    Reference reference;
    reference.storage_root_id = storage_root_id;
    reference.relative_path = relative_path;
    return reference;
}

std::string ManagedBlobReferenceFile::Reference::ToString() const {
    return storage_root_id + "/" + relative_path;
}

Result<std::optional<ManagedBlobReferenceFile::Reference>>
ManagedBlobReferenceFile::FromDescriptorUri(const std::string& uri) {
    std::string name = PathUtil::GetName(uri);
    if (!StringUtils::EndsWith(name, kManagedBlobSuffix)) {
        return std::optional<Reference>();
    }
    PAIMON_ASSIGN_OR_RAISE(Reference reference,
                           Reference::Create(PathUtil::GetParentDirPath(uri), name));
    return std::optional<Reference>(std::move(reference));
}

std::string ManagedBlobReferenceFile::SidecarPath(const std::string& data_file_path) {
    return data_file_path + kReferenceFileSuffix;
}

Status ManagedBlobReferenceFile::Write(const std::shared_ptr<FileSystem>& fs,
                                       const std::string& path, std::vector<Reference> references) {
    std::sort(references.begin(), references.end());
    references.erase(std::unique(references.begin(), references.end()), references.end());

    std::string buffer;
    AppendBigEndianInt32(kMagic, &buffer);
    size_t covered_start = buffer.size();
    buffer.push_back(static_cast<char>(kVersion));
    AppendBigEndianInt32(static_cast<int32_t>(references.size()), &buffer);
    for (const auto& reference : references) {
        PAIMON_RETURN_NOT_OK(AppendJavaUtf(reference.storage_root_id, &buffer));
        PAIMON_RETURN_NOT_OK(AppendJavaUtf(reference.relative_path, &buffer));
    }
    uint32_t checksum =
        arrow::internal::crc32(0, buffer.data() + covered_start, buffer.size() - covered_start);
    AppendBigEndianInt32(static_cast<int32_t>(checksum), &buffer);

    Status status = [&]() -> Status {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> out,
                               fs->Create(path, /*overwrite=*/false));
        // The stream is closed on every path; a failed write keeps its own error.
        Status write_status = [&]() -> Status {
            int64_t total_written = 0;
            while (total_written < static_cast<int64_t>(buffer.size())) {
                PAIMON_ASSIGN_OR_RAISE(
                    int64_t written,
                    out->Write(buffer.data() + total_written,
                               static_cast<int64_t>(buffer.size()) - total_written));
                if (written <= 0 || written > static_cast<int64_t>(buffer.size()) - total_written) {
                    return Status::IOError(
                        fmt::format("Short write of managed blob reference file {}.", path));
                }
                total_written += written;
            }
            return out->Flush();
        }();
        Status close_status = out->Close();
        if (write_status.ok()) {
            write_status = close_status;
        }
        return write_status;
    }();
    if (!status.ok()) {
        [[maybe_unused]] Status delete_status = fs->Delete(path);
        return status;
    }
    return Status::OK();
}

Result<std::vector<ManagedBlobReferenceFile::Reference>> ManagedBlobReferenceFile::Read(
    const std::shared_ptr<FileSystem>& fs, const std::string& path) {
    PAIMON_ASSIGN_OR_RAISE(FileStatus file_status, fs->GetFileStatus(path));
    int64_t length = file_status.GetLen();
    if (length < 0) {
        return Status::Invalid(
            fmt::format("Managed blob reference file {} has unknown length.", path));
    }
    std::string buffer(static_cast<size_t>(length), '\0');
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> in, fs->Open(file_status));
    Status read_status = [&]() -> Status {
        int64_t total_read = 0;
        while (total_read < length) {
            PAIMON_ASSIGN_OR_RAISE(int64_t read,
                                   in->Read(buffer.data() + total_read, length - total_read));
            if (read <= 0 || read > length - total_read) {
                return Status::IOError(
                    fmt::format("Short read of managed blob reference file {}.", path));
            }
            total_read += read;
        }
        return Status::OK();
    }();
    // The stream is closed on every path; a failed read keeps its own error.
    Status close_status = in->Close();
    if (read_status.ok()) {
        read_status = close_status;
    }
    PAIMON_RETURN_NOT_OK(read_status);

    BigEndianBufferReader reader(buffer.data(), buffer.size());
    PAIMON_ASSIGN_OR_RAISE(int32_t magic, reader.ReadInt32());
    if (magic != kMagic) {
        return Status::Invalid(
            fmt::format("Managed blob reference file {} has a bad magic number.", path));
    }
    size_t covered_start = reader.Position();
    PAIMON_ASSIGN_OR_RAISE(int8_t version, reader.ReadInt8());
    if (version != kVersion) {
        return Status::Invalid(fmt::format(
            "Managed blob reference file {} has unsupported version {}.", path, version));
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t count, reader.ReadInt32());
    if (count < 0) {
        return Status::Invalid(
            fmt::format("Managed blob reference file {} has a negative count.", path));
    }
    // Each reference occupies at least two writeUTF values with non-empty content (2-byte
    // length prefix plus one byte each): bound the untrusted count by the remaining bytes
    // before allocating for it.
    if (static_cast<uint64_t>(count) > reader.Remaining() / 6) {
        return Status::Invalid(fmt::format(
            "Managed blob reference file {} declares more references than it can hold.", path));
    }
    std::vector<Reference> references;
    references.reserve(count);
    for (int32_t i = 0; i < count; i++) {
        PAIMON_ASSIGN_OR_RAISE(std::string storage_root_id, ReadJavaUtf(&reader));
        PAIMON_ASSIGN_OR_RAISE(std::string relative_path, ReadJavaUtf(&reader));
        PAIMON_ASSIGN_OR_RAISE(Reference reference,
                               Reference::Create(storage_root_id, relative_path));
        references.push_back(std::move(reference));
    }
    size_t covered_end = reader.Position();
    uint32_t expected_checksum =
        arrow::internal::crc32(0, buffer.data() + covered_start, covered_end - covered_start);
    PAIMON_ASSIGN_OR_RAISE(int32_t checksum, reader.ReadInt32());
    if (static_cast<uint32_t>(checksum) != expected_checksum) {
        return Status::Invalid(
            fmt::format("Managed blob reference file {} has a checksum mismatch.", path));
    }
    if (reader.Remaining() != 0) {
        return Status::Invalid(
            fmt::format("Managed blob reference file {} has trailing bytes.", path));
    }
    return references;
}

}  // namespace paimon
